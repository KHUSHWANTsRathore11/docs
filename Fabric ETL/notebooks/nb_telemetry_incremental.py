# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_telemetry_incremental — Daily Incremental (Notebook 2)
#
# Scheduled at `X 00:00:00 UTC`; loads telemetry for the window `[X-1 00:00:00 UTC, X 00:00:00 UTC)`.
# Spec: `CLAUDE.md` section 3.2.
#
# | | |
# |---|---|
# | **Source** | `Files/ilayer_SCADA/{p_plant}/{p_department}/{Year}/{Month}/{Day}/{p_table_name}/*.parquet` |
# | **Target** | `silver.<p_table_name>` partitioned by `(plantname, Year, Month, Date)` |
# | **Write** | append (a re-run for the same day replaces that plant/department/day slice) |
# | **Optimize** | `OPTIMIZE ... WHERE plantname/Year/Month/Date ZORDER BY (departmentname, <p_id_column>)` |
#
# `p_execution_date` is the data day **X-1**. Pass either `YYYY-MM-DD` or the tumbling window
# start (`@trigger().outputs.windowStartTime`, must be 00:00:00 UTC); it decides both the source
# folder date and the `[start, start + 1 day)` filter, so re-running a failed window reloads
# exactly that day and replaces its slice. Empty defaults to yesterday (UTC) for manual runs.
# Attach the **Bronze** lakehouse as the default lakehouse so `Files/...` resolves.

# PARAMETERS CELL ********************

p_plant          = "PlantA"
p_department     = "Boiler"
p_table_name     = "TAGLOG"
p_id_column      = "TagId"
p_execution_date = ""      # "YYYY-MM-DD" or window start "YYYY-MM-DDT00:00:00Z" (Defaults to yesterday X-1 if empty)
p_dry_run        = False

# Infrastructure defaults (override from the pipeline only if the workspace layout differs)
p_bronze_root     = "Files"      # Bronze lakehouse Files root (relative to default lakehouse or abfss://...)
p_silver_database = "silver"     # Silver lakehouse / schema name
p_time_column     = "Time"       # Event timestamp column (UTC)
p_zero_pad_dates  = True         # Folder layout 2026/01/05 (True) vs 2026/1/5 (False)

# CELL ********************

# MAGIC %run delta_helpers

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

plant       = validate_path_segment(p_plant, "p_plant")
department  = validate_path_segment(p_department, "p_department")
table_name  = validate_identifier(p_table_name, "p_table_name")
id_column   = validate_identifier(p_id_column, "p_id_column")
time_column = validate_identifier(p_time_column, "p_time_column")
dry_run     = to_bool(p_dry_run)

target_date = resolve_execution_date(p_execution_date)
window_start, window_end = utc_day_window(target_date)

target_table = qualified_table(p_silver_database, table_name)
source_path = join_path(
    p_bronze_root, "ilayer_SCADA", plant, department,
    date_path(target_date, to_bool(p_zero_pad_dates)), table_name, "*.parquet",
)

run_started = datetime.now(timezone.utc)
print(f"Source : {source_path}")
print(f"Target : {target_table}")
print(f"Window : [{window_start.isoformat()}Z, {window_end.isoformat()}Z)")
print(f"Dry run: {dry_run}")

# CELL ********************

# Read & transform
raw_df = spark.read.parquet(source_path)
require_columns(raw_df, [time_column, id_column], "ilayer_SCADA source")

df = add_time_partitions(add_source_columns(raw_df, plant, department), time_column)

# Keep strictly the [X-1, X) window: intraday chunks can carry rows from neighbouring days,
# and every written row must belong to the replaced Date partition.
# Bounds are cast from strings so they are interpreted in the UTC session time zone,
# independent of the driver's local time zone.
start_ts = F.lit(window_start.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
end_ts = F.lit(window_end.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
in_window = (F.col(time_column) >= start_ts) & (F.col(time_column) < end_ts)
df = df.withColumn("_in_window", F.coalesce(in_window, F.lit(False))).cache()

counts = df.groupBy("_in_window").count().collect()
rows_in_window = next((r["count"] for r in counts if r["_in_window"]), 0)
rows_out_of_window = next((r["count"] for r in counts if not r["_in_window"]), 0)

df = df.filter("_in_window").drop("_in_window")

print(f"Rows in window      : {rows_in_window}")
print(f"Rows outside window : {rows_out_of_window} (dropped)")

# CELL ********************

# Write & optimize
if rows_in_window == 0:
    status = "NO_DATA"
elif dry_run:
    df.printSchema()
    df.show(10, truncate=False)
    status = "DRY_RUN"
else:
    # Replacing exactly this plant/department/day makes reruns and backfills idempotent;
    # for every other slice this is an append.
    replace_predicate = (
        f"{PLANT_COLUMN} = {sql_literal(plant)}"
        f" AND {DEPARTMENT_COLUMN} = {sql_literal(department)}"
        f" AND Date = {sql_literal(target_date)}"
    )
    write_partitioned_slice(df, target_table, TELEMETRY_PARTITION_COLUMNS, replace_predicate)

    optimize_predicate = (
        f"{PLANT_COLUMN} = {sql_literal(plant)}"
        f" AND Year = {target_date.year}"
        f" AND Month = {target_date.month}"
        f" AND Date = {sql_literal(target_date)}"
    )
    zorder_optimize(spark, target_table, optimize_predicate, [DEPARTMENT_COLUMN, id_column])
    status = "SUCCEEDED"

df.unpersist()

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_incremental",
    "status": status,
    "plant": plant,
    "department": department,
    "table": target_table,
    "source_path": source_path,
    "execution_date": target_date,
    "window_start_utc": window_start,
    "window_end_utc": window_end,
    "rows_read": rows_in_window + rows_out_of_window,
    "rows_written": rows_in_window if status == "SUCCEEDED" else 0,
    "rows_dropped_out_of_window": rows_out_of_window,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
