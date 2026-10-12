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
# | **Target** | `p_target_table` partitioned by `(plantname, Year, Month, Date)` |
# | **Write** | append |
# | **Optimize** | `OPTIMIZE ... WHERE plantname/Year/Month/Date ZORDER BY (departmentname, <p_id_column>)` for the loaded day |
#
# The caller builds `p_source_path` for the day (e.g. from the tumbling window start); the
# source holds exactly that day, `00:00:00` to `23:59:59.999` UTC.
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

# Partitions written by this run, for the scoped Z-Order.
loaded_partitions = df.select("Year", "Month", "Date").distinct().collect()
row_count = df.count()

print(f"Rows to load : {row_count}")
print(f"Days         : {sorted(str(r['Date']) for r in loaded_partitions)}")

# CELL ********************

# Append & optimize
if row_count == 0:
    status = "NO_DATA"
elif dry_run:
    df.printSchema()
    df.show(10, truncate=False)
    status = "DRY_RUN"
else:
    write_partitioned_slice(df, target_table, TELEMETRY_PARTITION_COLUMNS)

    for r in loaded_partitions:
        optimize_predicate = (
            f"{PLANT_COLUMN} = {sql_literal(plant)}"
            f" AND Year = {r['Year']}"
            f" AND Month = {r['Month']}"
            f" AND Date = {sql_literal(r['Date'])}"
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
    "days": sorted(str(r["Date"]) for r in loaded_partitions),
    "rows_written": row_count if status == "SUCCEEDED" else 0,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
