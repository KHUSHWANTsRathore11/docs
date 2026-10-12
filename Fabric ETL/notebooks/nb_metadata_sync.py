# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_metadata_sync — Daily Metadata Sync, SCD Type 1 (Notebook 3)
#
# Daily sync of a metadata / configuration table (e.g. `TAGS`) from a full snapshot.
# Spec: `CLAUDE.md` section 3.3.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path, e.g. `Files/rlayer_SCADA/PlantA/Boiler/TAGS.parquet` (daily full snapshot) |
# | **Target** | `p_target_table` |
# | **Write** | Delta `MERGE` on `(<p_plant_column>, <p_department_column>, <p_id_column>)`: update matched, insert new |
#
# Tags that disappear from the snapshot are left as they are (retirement handling is a future
# evolution, see `CLAUDE.md` section 5.2).
# Relative `Files/...` paths resolve against the default lakehouse; `abfss://...` paths work
# with any lakehouse attached.

# PARAMETERS CELL ********************

p_source_path    = "Files/rlayer_SCADA/PlantA/Boiler/TAGS.parquet"
p_target_table   = "silver.TAGS"
p_plant_column      = "PlantName"        # Plant column in the source (partition column)
p_department_column = "DepartmentName"   # Department column in the source
p_id_column      = "TagId"
p_dry_run        = False

# CELL ********************

# MAGIC %run nb_lib_delta_helpers

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

source_path  = validate_source_path(p_source_path)
target_table = validate_table_name(p_target_table)
plant_column = validate_identifier(p_plant_column, "p_plant_column")
dept_column  = validate_identifier(p_department_column, "p_department_column")
id_column    = validate_identifier(p_id_column, "p_id_column")
dry_run      = to_bool(p_dry_run)

key_columns = [plant_column, dept_column, id_column]

run_started = datetime.now(timezone.utc)
print(f"Source : {source_path}")
print(f"Target : {target_table}")
print(f"Key    : {key_columns}")
print(f"Dry run: {dry_run}")

# CELL ********************

# Read & stage the snapshot
raw_df = spark.read.parquet(source_path)
require_columns(raw_df, key_columns, "metadata snapshot")

staged = raw_df
rows_read = staged.count()

# Rows without a business key can never be matched and would be re-inserted every day.
null_key_rows = staged.filter(F.col(id_column).isNull()).count()
staged = staged.filter(F.col(id_column).isNotNull())

# MERGE fails if several source rows match one target row; keep one row per key.
staged = staged.dropDuplicates(key_columns).cache()
rows_staged = staged.count()
duplicate_rows = rows_read - null_key_rows - rows_staged

print(f"Rows read             : {rows_read}")
print(f"Rows dropped (null key): {null_key_rows}")
print(f"Rows dropped (dup key) : {duplicate_rows}")
print(f"Rows staged           : {rows_staged}")

# CELL ********************

# MERGE (SCD Type 1)
merge_metrics = {}
if rows_staged == 0:
    status = "NO_DATA"
elif dry_run:
    staged.printSchema()
    staged.show(10, truncate=False)
    status = "DRY_RUN"
else:
    merge_metrics = merge_scd1(spark, staged, target_table, key_columns)
    print(f"Merge metrics: {merge_metrics}")
    status = "SUCCEEDED"

staged.unpersist()

# CELL ********************

notebook_exit({
    "notebook": "nb_metadata_sync",
    "status": status,
    "table": target_table,
    "source_path": source_path,
    "rows_read": rows_read,
    "rows_staged": rows_staged,
    "rows_dropped_null_key": null_key_rows,
    "rows_dropped_duplicate_key": duplicate_rows,
    "merge_metrics": merge_metrics,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
