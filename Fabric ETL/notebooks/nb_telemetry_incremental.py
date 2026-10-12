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
# Appends one day of telemetry to the target and clusters it.
# Spec: `CLAUDE.md` section 3.2.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path of one day's files, e.g. `Files/ilayer_SCADA/PlantA/Boiler/2026/10/11/TAGLOG/*.parquet` |
# | **Target** | `p_target_table`, liquid-clustered by `(Date, <p_plant_column>, <p_department_column>, <p_id_column>)` |
# | **Write** | append |
# | **Optimize** | `OPTIMIZE <table>` (incremental clustering on Fabric Runtime 2.0); a failed OPTIMIZE does not fail the run |
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
p_plant_column      = "PlantName"        # Plant column in the source
p_department_column = "DepartmentName"   # Department column in the source
p_id_column      = "TagId"
p_time_column    = "Time"   # Event timestamp column (UTC)
p_dry_run        = False

# CELL ********************

# MAGIC %run nb_lib_delta_helpers

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

df = add_time_columns(raw_df, time_column)
cluster_columns = telemetry_cluster_columns(plant_column, dept_column, id_column)

loaded = df.select(plant_column, dept_column, "Date").distinct().collect()
row_count = df.count()
loaded_labels = sorted(f"{r[plant_column]}/{r[dept_column]}/{r['Date']}" for r in loaded)

print(f"Rows to load : {row_count}")
print(f"Loaded       : {loaded_labels}")

# CELL ********************

# Append & optimize
optimize_error = None
if row_count == 0:
    status = "NO_DATA"
elif dry_run:
    df.printSchema()
    df.show(10, truncate=False)
    status = "DRY_RUN"
else:
    ensure_clustered_table(spark, df, target_table, cluster_columns)
    write_slice(df, target_table)
    status = "SUCCEEDED"

    # The data is committed at this point. OPTIMIZE is maintenance: if it fails (e.g. a
    # concurrent OPTIMIZE from a parallel run), the run still succeeds and the next run's
    # OPTIMIZE clusters this data too. Failing here would invite a re-run that appends the
    # day a second time.
    try:
        optimize_table(spark, target_table)
    except Exception as e:  # noqa: BLE001 - reported in the exit payload
        optimize_error = f"{type(e).__name__}: {str(e)[:500]}"
        print(f"WARNING: OPTIMIZE failed, data is loaded: {optimize_error}")

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_incremental",
    "status": status,
    "table": target_table,
    "source_path": source_path,
    "loaded": loaded_labels,
    "rows_written": row_count if status == "SUCCEEDED" else 0,
    "optimize_error": optimize_error,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
