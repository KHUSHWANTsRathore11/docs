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
# Loads one UTC day of telemetry and replaces that day's plant/department slice in the target,
# so a failed run is recovered by simply re-running it. Spec: `CLAUDE.md` section 3.2.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path, globs allowed, e.g. `Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet` |
# | **Target** | `p_target_table` partitioned by `(plantname, Year, Month, Date)` |
# | **Write** | append (a re-run for the same day replaces that plant/department/day slice) |
# | **Optimize** | `OPTIMIZE ... WHERE plantname/Year/Month/Date ZORDER BY (departmentname, <p_id_column>)` |
#
# The caller builds `p_source_path` for the day (e.g. from the tumbling window start). The day
# loaded is read from the data: all rows must fall on one UTC day, otherwise the run fails and
# lists the days found.
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

run_started = datetime.now(timezone.utc)
print(f"Source : {source_path}")
print(f"Target : {target_table}")
print(f"Dry run: {dry_run}")

# CELL ********************

# Read & transform
raw_df = spark.read.parquet(source_path)
require_columns(raw_df, [time_column, id_column], "source")

df = add_time_partitions(add_source_columns(raw_df, plant, department), time_column)

# Rows without an event time cannot be partitioned; drop them and report the count.
null_time_rows = df.filter(F.col(time_column).isNull()).count()
df = df.filter(F.col(time_column).isNotNull())

# The day to replace comes from the data; fails if the rows span several days.
load_date = single_date_in_data(df)
row_count = df.count() if load_date else 0

print(f"Day                 : {load_date}")
print(f"Rows to load        : {row_count}")
print(f"Rows dropped (null {time_column}): {null_time_rows}")

# CELL ********************

# Write & optimize
if row_count == 0:
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
        f" AND Date = {sql_literal(load_date)}"
    )
    write_partitioned_slice(df, target_table, TELEMETRY_PARTITION_COLUMNS, replace_predicate)

    optimize_predicate = (
        f"{PLANT_COLUMN} = {sql_literal(plant)}"
        f" AND Year = {load_date.year}"
        f" AND Month = {load_date.month}"
        f" AND Date = {sql_literal(load_date)}"
    )
    zorder_optimize(spark, target_table, optimize_predicate, [DEPARTMENT_COLUMN, id_column])
    status = "SUCCEEDED"

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_incremental",
    "status": status,
    "plant": plant,
    "department": department,
    "table": target_table,
    "source_path": source_path,
    "load_date": load_date,
    "rows_read": row_count + null_time_rows,
    "rows_written": row_count if status == "SUCCEEDED" else 0,
    "rows_dropped_null_time": null_time_rows,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
