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
# Appends one day of telemetry to the target and Z-Orders that day's partition.
# Spec: `CLAUDE.md` section 3.2.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path of one day's files, e.g. `Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet` |
# | **Target** | `p_target_table` partitioned by `(<p_plant_column>, Year, Month, Date)` |
# | **Write** | append |
# | **Optimize** | `OPTIMIZE ... WHERE <plant>/Year/Month/Date ZORDER BY (<p_department_column>, <p_id_column>)` for the loaded day |
#
# The caller builds `p_source_path` for the day (e.g. from the tumbling window start); the
# source holds exactly that day, `00:00:00` to `23:59:59.999` UTC, and already carries the plant
# and department columns.
#
# Relative `Files/...` paths resolve against the default lakehouse; `abfss://...` paths work
# with any lakehouse attached.

# PARAMETERS CELL ********************

p_source_path    = "Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet"
p_target_table   = "silver.TAGLOG"
p_plant_column      = "PlantName"        # Plant column in the source (partition column)
p_department_column = "DepartmentName"   # Department column in the source
p_id_column      = "TagId"
p_time_column    = "Time"   # Event timestamp column (UTC)
p_dry_run        = False

# CELL ********************

# MAGIC %run delta_helpers

# CELL ********************

from datetime import datetime, timezone

spark.conf.set("spark.sql.session.timeZone", "UTC")

source_path  = validate_source_path(p_source_path)
target_table = validate_table_name(p_target_table)
plant_column = validate_identifier(p_plant_column, "p_plant_column")
dept_column  = validate_identifier(p_department_column, "p_department_column")
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
require_columns(raw_df, [plant_column, dept_column, time_column, id_column], "source")

df = add_time_partitions(raw_df, time_column)
partition_columns = telemetry_partition_columns(plant_column)

# Partitions written by this run, for the scoped Z-Order.
loaded_partitions = df.select(*partition_columns).distinct().collect()
row_count = df.count()

print(f"Rows to load : {row_count}")
partition_labels = sorted(f"{r[plant_column]}/{r['Date']}" for r in loaded_partitions)
print(f"Partitions   : {partition_labels}")

# CELL ********************

# Append & optimize
if row_count == 0:
    status = "NO_DATA"
elif dry_run:
    df.printSchema()
    df.show(10, truncate=False)
    status = "DRY_RUN"
else:
    write_partitioned_slice(df, target_table, partition_columns)

    for r in loaded_partitions:
        optimize_predicate = (
            f"{plant_column} <=> {sql_literal(r[plant_column])}"
            f" AND Year = {r['Year']}"
            f" AND Month = {r['Month']}"
            f" AND Date = {sql_literal(r['Date'])}"
        )
        zorder_optimize(spark, target_table, optimize_predicate, [dept_column, id_column])
    status = "SUCCEEDED"

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_incremental",
    "status": status,
    "table": target_table,
    "source_path": source_path,
    "partitions": partition_labels,
    "rows_written": row_count if status == "SUCCEEDED" else 0,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
