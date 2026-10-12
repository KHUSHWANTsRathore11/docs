# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_spool_boundaries — Spool Boundaries (Notebook 4)
#
# Derives one row per spool from the LASTSpoolPV events in the telemetry table.
# Spec: `CLAUDE.md` section 3.4.
#
# | | |
# |---|---|
# | **Spool tags** | tags in `p_tags_table` whose description is `p_spool_tag_description` (one per machine/line/position) |
# | **Events** | rows of `p_taglog_table` for those tags; every row is a spool end |
# | **Target** | `p_target_table`, one row per spool, keyed by `(PlantName, DepartmentName, SpoolTagId, SpoolEndTime)` |
# | **Write** | Delta `MERGE` (idempotent); `p_full_rebuild = True` recomputes and overwrites the table |
# | **Optimize** | unpartitioned; `OPTIMIZE <table> ZORDER BY (<plant>, <department>, SpoolTagId)` |
#
# A spool runs from the previous spool end of the same tag (exclusive) to its own end
# (inclusive): `SpoolStartTime < Time <= SpoolEndTime`. The first event of a tag has no
# known start (`SpoolStartTime` is null).
#
# Incremental runs continue from the last `SpoolEndTime` per tag already in the target, so
# spools that span midnight are handled. Events older than that are not picked up; run with
# `p_full_rebuild = True` after a historical (re)load.
#
# Run after the day's telemetry and metadata loads.

# PARAMETERS CELL ********************

p_taglog_table          = "silver.TAGLOG"
p_tags_table            = "silver.TAGS"
p_target_table          = "silver.SPOOL"
p_spool_tag_description = "LASTSpoolPV"
p_plant_column          = "PlantName"
p_department_column     = "DepartmentName"
p_id_column             = "TagId"
p_time_column           = "Time"
p_value_column          = "Value"
p_description_column    = "Description"
p_full_rebuild          = False
p_dry_run               = False

# CELL ********************

# MAGIC %run nb_lib_delta_helpers

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

taglog_table = validate_table_name(p_taglog_table, "p_taglog_table")
tags_table   = validate_table_name(p_tags_table, "p_tags_table")
target_table = validate_table_name(p_target_table)
plant_column = validate_identifier(p_plant_column, "p_plant_column")
dept_column  = validate_identifier(p_department_column, "p_department_column")
id_column    = validate_identifier(p_id_column, "p_id_column")
time_column  = validate_identifier(p_time_column, "p_time_column")
value_column = validate_identifier(p_value_column, "p_value_column")
desc_column  = validate_identifier(p_description_column, "p_description_column")
spool_description = str(p_spool_tag_description).strip()
full_rebuild = to_bool(p_full_rebuild)
dry_run      = to_bool(p_dry_run)

if not spool_description:
    raise ValueError("p_spool_tag_description is required")

run_started = datetime.now(timezone.utc)
print(f"Telemetry : {taglog_table}")
print(f"Tags      : {tags_table} where {desc_column} = '{spool_description}' (case-insensitive)")
print(f"Target    : {target_table}")
print(f"Mode      : {'full rebuild' if full_rebuild else 'incremental'}; dry run: {dry_run}")

# CELL ********************

# Spool tags: one per machine/line/position, identified by description, never by TagId
tags = spark.table(tags_table)
t_plant, t_dept, t_id, t_desc = require_columns(
    tags, [plant_column, dept_column, id_column, desc_column], "tags table"
)
spool_tags = (
    tags.where(F.lower(F.trim(F.col(t_desc))) == spool_description.lower())
        .select(F.col(t_plant).alias(plant_column), F.col(t_dept).alias(dept_column),
                F.col(t_id).alias("SpoolTagId"))
        .distinct()
        .cache()
)
spool_tag_count = spool_tags.count()
print(f"Spool tags: {spool_tag_count}")

# CELL ********************

# Spool-end events since the last spool end per tag already in the target
key = [plant_column, dept_column, "SpoolTagId"]
taglog = spark.table(taglog_table)
l_plant, l_dept, l_id, l_time, l_value = require_columns(
    taglog, [plant_column, dept_column, id_column, time_column, value_column], "telemetry table"
)
events = (
    taglog.select(F.col(l_plant).alias(plant_column), F.col(l_dept).alias(dept_column),
                  F.col(l_id).alias("SpoolTagId"),
                  F.col(l_time).cast("timestamp").alias("SpoolEndTime"),
                  F.col(l_value).alias("LastSpoolPV"),
                  *([F.col("Date")] if "date" in [c.lower() for c in taglog.columns] else []))
          .join(F.broadcast(spool_tags), key, "inner")
          .where(F.col("SpoolEndTime").isNotNull())
)

incremental = not full_rebuild and table_exists(spark, target_table)
if incremental:
    watermarks = (spark.table(target_table).groupBy(*key)
                  .agg(F.max("SpoolEndTime").alias("_last_end")))
    # Prune the telemetry scan by date only when every spool tag already has a watermark;
    # a new spool tag needs its full history.
    new_tags = spool_tags.join(watermarks, key, "left_anti").count()
    if new_tags == 0 and "Date" in events.columns:
        oldest = watermarks.agg(F.min("_last_end")).first()[0]
        if oldest is not None:
            events = events.where(F.col("Date") >= F.to_date(F.lit(oldest)))
    events = (events.join(watermarks, key, "left")
                    .where(F.col("_last_end").isNull() | (F.col("SpoolEndTime") > F.col("_last_end")))
                    .drop("_last_end"))
    # The last known end per tag seeds the start of its next spool.
    seeds = watermarks.select(*key, F.col("_last_end").alias("SpoolEndTime"),
                              F.lit(None).cast(events.schema["LastSpoolPV"].dataType).alias("LastSpoolPV"),
                              F.lit(True).alias("_seed"))
else:
    new_tags = spool_tag_count
    seeds = None

events = events.drop("Date").dropDuplicates(key + ["SpoolEndTime"]).withColumn("_seed", F.lit(False))
boundaries = events.unionByName(seeds) if seeds is not None else events

w = Window.partitionBy(*key).orderBy("SpoolEndTime")
spools = (
    boundaries.withColumn("SpoolStartTime", F.lag("SpoolEndTime").over(w))
              .where(~F.col("_seed"))
              .select(
                  F.sha2(F.concat_ws("|", F.col(plant_column), F.col(dept_column),
                                     F.col("SpoolTagId").cast("string"),
                                     F.date_format("SpoolEndTime", "yyyy-MM-dd HH:mm:ss.SSSSSS")), 256)
                   .alias("SpoolKey"),
                  *key, "SpoolStartTime", "SpoolEndTime", "LastSpoolPV",
                  (F.col("SpoolEndTime").cast("double") - F.col("SpoolStartTime").cast("double"))
                   .alias("DurationSec"),
                  F.to_date("SpoolEndTime").alias("Date"),
              )
              .cache()
)
spool_count = spools.count()
print(f"Spool tags without earlier spools: {new_tags}")
print(f"Spools to write                  : {spool_count}")

# CELL ********************

# Write & optimize
merge_metrics = {}
if spool_count == 0 and not full_rebuild:
    status = "NO_DATA"
elif dry_run:
    spools.printSchema()
    spools.orderBy(*key, "SpoolEndTime").show(20, truncate=False)
    status = "DRY_RUN"
else:
    if full_rebuild:
        (spools.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
               .saveAsTable(target_table))
        merge_metrics = {"operation": "OVERWRITE", "numOutputRows": spool_count}
    else:
        merge_metrics = merge_scd1(spark, spools, target_table, key + ["SpoolEndTime"])
    print(f"Write metrics: {merge_metrics}")

    # Idempotent, so an OPTIMIZE failure can simply fail the run.
    zorder_optimize(spark, target_table, [plant_column, dept_column, "SpoolTagId"])
    status = "SUCCEEDED"

spools.unpersist()
spool_tags.unpersist()

# CELL ********************

notebook_exit({
    "notebook": "nb_spool_boundaries",
    "status": status,
    "table": target_table,
    "mode": "full_rebuild" if full_rebuild else "incremental",
    "spool_tags": spool_tag_count,
    "spool_tags_without_earlier_spools": new_tags,
    "spools_written": spool_count if status == "SUCCEEDED" else 0,
    "write_metrics": merge_metrics,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
