# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_telemetry_historical — Historical Onboarding (Notebook 1)
#
# One-time bulk load of time-series parquet into a liquid-clustered Delta table, e.g. onboarding a
# new `{Plant}/{Department}` from the consolidated history export. Spec: `CLAUDE.md` section 3.1.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path, globs allowed, e.g. `Files/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet` |
# | **Target** | `p_target_table` liquid-clustered by `(Date, <p_plant_column>, <p_department_column>, <p_id_column>)` |
# | **Write** | append (re-runs replace only the loaded plant/department/date range) |
# | **Optimize** | `OPTIMIZE <table>` (incremental clustering on Fabric Runtime 2.0) |
#
# The notebook builds no paths itself. Relative `Files/...` paths resolve against the default
# lakehouse; `abfss://...` paths work with any lakehouse attached.

# PARAMETERS CELL ********************

p_source_path    = "Files/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet"
p_target_table   = "silver.TAGLOG"
p_plant_column      = "PlantName"        # Plant column in the source
p_department_column = "DepartmentName"   # Department column in the source
p_id_column      = "TagId"
p_time_column    = "Time"       # Event timestamp column (UTC)
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
time_column  = validate_identifier(p_time_column, "p_time_column")
dry_run      = to_bool(p_dry_run)

run_started = datetime.now(timezone.utc)
print(f"Source : {source_path}")
print(f"Target : {target_table}")
print(f"Dry run: {dry_run}")

# CELL ********************

# Read & transform
raw_df = spark.read.parquet(source_path)
plant_column, dept_column, time_column, id_column = require_columns(
    raw_df, [plant_column, dept_column, time_column, id_column], "source"
)

df = add_time_columns(raw_df, time_column)
cluster_columns = telemetry_cluster_columns(plant_column, dept_column, id_column)

# Rows without an event time have no Date; drop them and report the count.
null_time_rows = df.filter(F.col(time_column).isNull()).count()
df = df.filter(F.col(time_column).isNotNull())

bounds = df.agg(F.min("Date").alias("min_date"), F.max("Date").alias("max_date"), F.count("*").alias("rows")).collect()[0]
min_date, max_date, row_count = bounds["min_date"], bounds["max_date"], bounds["rows"]

print(f"Rows to load        : {row_count}")
print(f"Rows dropped (null {time_column}): {null_time_rows}")
print(f"Date range          : {min_date} -> {max_date}")

# Plant/department combinations in this load; a re-run replaces exactly these over the date range.
lanes = [(r[0], r[1]) for r in df.select(plant_column, dept_column).distinct().collect()] if row_count else []
print(f"Plant/department    : {lanes}")

# CELL ********************

# Write & optimize
if row_count == 0:
    status = "NO_DATA"
elif dry_run:
    df.printSchema()
    df.show(10, truncate=False)
    status = "DRY_RUN"
else:
    # Replacing exactly these plant/department combinations over the loaded date range makes a
    # re-run of the onboarding idempotent instead of duplicating history.
    date_range = f"Date >= {sql_literal(min_date)} AND Date <= {sql_literal(max_date)}"
    lane_predicate = " OR ".join(
        f"({plant_column} <=> {sql_literal(p)} AND {dept_column} <=> {sql_literal(d)})" for p, d in lanes
    )
    replace_predicate = f"({lane_predicate}) AND {date_range}"
    ensure_clustered_table(spark, df, target_table, cluster_columns)
    write_slice(df, target_table, replace_predicate)

    # Re-running the onboarding is idempotent, so an OPTIMIZE failure can simply fail the run.
    optimize_table(spark, target_table)
    status = "SUCCEEDED"

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_historical",
    "status": status,
    "plant_departments": [f"{p}/{d}" for p, d in lanes],
    "table": target_table,
    "source_path": source_path,
    "rows_read": row_count + null_time_rows,
    "rows_written": row_count if status == "SUCCEEDED" else 0,
    "rows_dropped_null_time": null_time_rows,
    "min_date": min_date,
    "max_date": max_date,
    "started_utc": run_started,
    "finished_utc": datetime.now(timezone.utc),
})
