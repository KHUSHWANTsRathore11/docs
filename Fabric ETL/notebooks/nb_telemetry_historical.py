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
# One-time bulk load of time-series parquet into a partitioned Delta table, e.g. onboarding a
# new `{Plant}/{Department}` from the consolidated history export. Spec: `CLAUDE.md` section 3.1.
#
# | | |
# |---|---|
# | **Source** | `p_source_path`: full path, globs allowed, e.g. `Files/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet` |
# | **Target** | `p_target_table` partitioned by `(plantname, Year, Month, Date)` |
# | **Write** | append (re-runs replace only the loaded plant/department/date range) |
# | **Optimize** | `OPTIMIZE ... ZORDER BY (departmentname, <p_id_column>)` scoped to the loaded partitions |
#
# The notebook builds no paths itself. Relative `Files/...` paths resolve against the default
# lakehouse; `abfss://...` paths work with any lakehouse attached.

# PARAMETERS CELL ********************

p_source_path    = "Files/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet"
p_target_table   = "silver.TAGLOG"
p_plant          = "PlantA"
p_department     = "Boiler"
p_id_column      = "TagId"
p_time_column    = "Time"       # Event timestamp column (UTC)
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

bounds = df.agg(F.min("Date").alias("min_date"), F.max("Date").alias("max_date"), F.count("*").alias("rows")).collect()[0]
min_date, max_date, row_count = bounds["min_date"], bounds["max_date"], bounds["rows"]

print(f"Rows to load        : {row_count}")
print(f"Rows dropped (null {time_column}): {null_time_rows}")
print(f"Date range          : {min_date} -> {max_date}")

# CELL ********************

# Write & optimize
if row_count == 0:
    status = "NO_DATA"
elif dry_run:
    df.printSchema()
    df.show(10, truncate=False)
    status = "DRY_RUN"
else:
    # Replacing exactly this plant/department/date range makes a re-run of the onboarding
    # idempotent instead of duplicating history. Other plants/departments are untouched.
    replace_predicate = (
        f"{PLANT_COLUMN} = {sql_literal(plant)}"
        f" AND {DEPARTMENT_COLUMN} = {sql_literal(department)}"
        f" AND Date >= {sql_literal(min_date)} AND Date <= {sql_literal(max_date)}"
    )
    write_partitioned_slice(df, target_table, TELEMETRY_PARTITION_COLUMNS, replace_predicate)

    # OPTIMIZE WHERE only accepts partition columns, so the scope is the plant + loaded date
    # range; departmentname is clustered by the Z-Order instead.
    optimize_predicate = (
        f"{PLANT_COLUMN} = {sql_literal(plant)}"
        f" AND Date >= {sql_literal(min_date)} AND Date <= {sql_literal(max_date)}"
    )
    zorder_optimize(spark, target_table, optimize_predicate, [DEPARTMENT_COLUMN, id_column])
    status = "SUCCEEDED"

# CELL ********************

notebook_exit({
    "notebook": "nb_telemetry_historical",
    "status": status,
    "plant": plant,
    "department": department,
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
