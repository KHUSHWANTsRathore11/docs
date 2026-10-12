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
# Loads one UTC day of telemetry `[X-1 00:00:00 UTC, X 00:00:00 UTC)` and replaces that day's
# plant/department slice in the target, so a failed run is recovered by simply re-running it.
# Spec: `CLAUDE.md` section 3.2.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path, globs allowed, e.g. `Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet` |
# | **Target** | `p_target_table` partitioned by `(plantname, Year, Month, Date)` |
# | **Write** | append (a re-run for the same day replaces that plant/department/day slice) |
# | **Optimize** | `OPTIMIZE ... WHERE plantname/Year/Month/Date ZORDER BY (departmentname, <p_id_column>)` |
#
# The caller builds `p_source_path` for the day (e.g. from the tumbling window start).
# Which day is loaded:
# * `p_execution_date` empty (default): the day is read from the data. All rows must fall on
#   one UTC day, otherwise the run fails and lists the days found.
# * `p_execution_date` set (`YYYY-MM-DD`, or a window start at 00:00:00 UTC): that day is
#   loaded and rows from other days are dropped. Use it when the source mixes days.
#
# Relative `Files/...` paths resolve against the default lakehouse; `abfss://...` paths work
# with any lakehouse attached.

# PARAMETERS CELL ********************

p_source_path    = "Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet"
p_target_table   = "silver.TAGLOG"
p_plant          = "PlantA"
p_department     = "Boiler"
p_id_column      = "TagId"
p_time_column    = "Time"   # Event timestamp column (UTC)
p_execution_date = ""       # Optional: "YYYY-MM-DD" or window start "YYYY-MM-DDT00:00:00Z"; empty = day found in the data
p_dry_run        = False

# CELL ********************

# MAGIC %run delta_helpers

# CELL ********************

from datetime import datetime, timezone

from pyspark.sql import functions as F

spark.conf.set("spark.sql.session.timeZone", "UTC")

source_path  = validate_source_path(p_source_path)
target_table = validate_table_name(p_target_table)
plant        = validate_label(p_plant, "p_plant")
department   = validate_label(p_department, "p_department")
id_column    = validate_identifier(p_id_column, "p_id_column")
time_column  = validate_identifier(p_time_column, "p_time_column")
dry_run      = to_bool(p_dry_run)

target_date = resolve_execution_date(p_execution_date)
date_source = "parameter" if target_date else "data"

run_started = datetime.now(timezone.utc)
print(f"Source : {source_path}")
print(f"Target : {target_table}")
print(f"Day    : {target_date or 'from data'}")
print(f"Dry run: {dry_run}")

# CELL ********************

# Read & transform
raw_df = spark.read.parquet(source_path)
require_columns(raw_df, [time_column, id_column], "source")

df = add_time_partitions(add_source_columns(raw_df, plant, department), time_column)

if target_date is None:
    target_date = single_date_in_data(df)

marked_df = None

if target_date is None:
    window_start = window_end = None
    rows_in_window, rows_out_of_window = 0, df.count()
else:
    window_start, window_end = utc_day_window(target_date)
    print(f"Window : [{window_start.isoformat()}Z, {window_end.isoformat()}Z) ({date_source})")

    # Keep strictly the [X-1, X) window: the source can carry rows from neighbouring days (or be a
    # wider glob), and every written row must belong to the replaced Date partition.
    # Bounds are cast from strings so they are interpreted in the UTC session time zone,
    # independent of the driver's local time zone.
    start_ts = F.lit(window_start.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    end_ts = F.lit(window_end.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp")
    in_window = (F.col(time_column) >= start_ts) & (F.col(time_column) < end_ts)
    marked_df = df.withColumn("_in_window", F.coalesce(in_window, F.lit(False))).cache()

    counts = marked_df.groupBy("_in_window").count().collect()
    rows_in_window = next((r["count"] for r in counts if r["_in_window"]), 0)
    rows_out_of_window = next((r["count"] for r in counts if not r["_in_window"]), 0)

    df = marked_df.filter("_in_window").drop("_in_window")

print(f"Rows in window      : {rows_in_window}")
print(f"Rows outside window : {rows_out_of_window} (dropped, incl. null {time_column})")

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

if marked_df is not None:
    marked_df.unpersist()

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_incremental",
    "status": status,
    "plant": plant,
    "department": department,
    "table": target_table,
    "source_path": source_path,
    "execution_date": target_date,
    "execution_date_source": date_source,
    "window_start_utc": window_start,
    "window_end_utc": window_end,
    "rows_read": rows_in_window + rows_out_of_window,
    "rows_written": rows_in_window if status == "SUCCEEDED" else 0,
    "rows_dropped_out_of_window": rows_out_of_window,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
