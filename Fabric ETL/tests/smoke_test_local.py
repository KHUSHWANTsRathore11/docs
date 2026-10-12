"""Local smoke test: runs the three notebooks end to end against local PySpark + Delta.

    pip install pyspark==3.5.3 delta-spark==3.2.1   # needs Java 11/17
    python tests/smoke_test_local.py
"""
import json, os, shutil, sys, tempfile
from datetime import datetime
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
work = tempfile.mkdtemp()
spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", f"{work}/wh")
    .config("spark.sql.shuffle.partitions", "2")).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("CREATE DATABASE silver")
bronze = f"{work}/bronze"
helpers = open(f"{ROOT}/utils/delta_helpers.py").read()

def run(nb, **params):
    src = open(f"{ROOT}/notebooks/{nb}.py").read()
    src = src.replace("# MAGIC %run delta_helpers", helpers)
    # Overrides go right after the parameters cell, as Fabric's injected parameters cell does.
    head, tail = src.split("# CELL ********************", 1)
    override = "\n".join(f"{k} = {v!r}" for k, v in params.items())
    src = head + "\n" + override + "\n# CELL ********************" + tail
    g = {"spark": spark, "__name__": "nb"}
    import io, contextlib
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        exec(compile(src, nb, "exec"), g)
    last = [l for l in buf.getvalue().splitlines() if l.startswith("{")][-1]
    return json.loads(last)

def write(path, rows, cols):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    spark.createDataFrame(rows, cols).coalesce(1).write.mode("overwrite").parquet(path + ".dir")
    f = [x for x in os.listdir(path + ".dir") if x.endswith(".parquet")][0]
    shutil.move(f"{path}.dir/{f}", path); shutil.rmtree(path + ".dir")

T = lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
cols = ["TagId", "Time", "Value"]
# historical: 2 days for PlantA/Boiler
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/2026/10/01/TAGLOG.parquet", [(1, T("2026-10-01 01:00:00"), 1.0), (2, T("2026-10-01 05:00:00"), 2.0)], cols)
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/2026/10/02/TAGLOG.parquet", [(1, T("2026-10-02 01:00:00"), 3.0), (None, None, 9.0)], cols)
# metadata snapshot at same level must NOT be picked by historical glob
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/TAGS.parquet", [(1, "Temp", "C", 10.0), (2, "Press", "bar", 5.0), (2, "Press", "bar", 5.0), (None, "x", "y", 0.0)], ["TagId", "Description", "Unit", "LastValue"])

hist = dict(p_source_path=f"{bronze}/rlayer_SCADA/PlantA/Boiler/*/*/*/TAGLOG.parquet", p_target_table="silver.TAGLOG")
r = run("nb_telemetry_historical", **hist, p_dry_run="true"); print(r); assert r["status"] == "DRY_RUN"
assert not spark.catalog.tableExists("silver.TAGLOG")
r = run("nb_telemetry_historical", **hist); print(r)
assert r["rows_written"] == 3 and r["rows_dropped_null_time"] == 1
r = run("nb_telemetry_historical", **hist); assert spark.table("silver.TAGLOG").count() == 3, "historical rerun not idempotent"
print(spark.sql("DESCRIBE DETAIL silver.TAGLOG").select("partitionColumns").collect())

# incremental: one day per source path
write(f"{bronze}/ilayer_SCADA/PlantA/Boiler/2026/10/03/TAGLOG/part1.parquet", [(1, T("2026-10-03 00:00:00"), 4.0), (2, T("2026-10-03 23:59:59"), 5.0)], cols)
write(f"{bronze}/ilayer_SCADA/PlantA/Turbine/2026/10/03/TAGLOG/part1.parquet", [(7, T("2026-10-03 12:00:00"), 7.0)], cols)
inc = lambda dept: dict(p_source_path=f"{bronze}/ilayer_SCADA/PlantA/{dept}/2026/10/03/TAGLOG/*.parquet", p_target_table="silver.TAGLOG", p_department=dept)
r = run("nb_telemetry_incremental", **inc("Boiler"), p_dry_run=True); assert r["status"] == "DRY_RUN", r
assert spark.table("silver.TAGLOG").count() == 3
r = run("nb_telemetry_incremental", **inc("Boiler")); print(r)
assert r["days"] == ["2026-10-03"] and r["rows_written"] == 2, r
r = run("nb_telemetry_incremental", **inc("Turbine"))
assert spark.table("silver.TAGLOG").count() == 6, spark.table("silver.TAGLOG").count()
spark.table("silver.TAGLOG").orderBy("Time").show()

# reuse in another scenario: different folder layout, columns and target table
write(f"{bronze}/other_source/events/2026-10-03.parquet", [("A", T("2026-10-03 08:00:00")), ("B", T("2026-10-03 09:00:00"))], ["EventCode", "EventTime"])
r = run("nb_telemetry_incremental", p_source_path=f"{bronze}/other_source/events/*.parquet", p_target_table="silver.EVENTS",
        p_plant="PlantB", p_department="Packing", p_id_column="EventCode", p_time_column="EventTime")
assert r["rows_written"] == 2, r
assert spark.table("silver.EVENTS").count() == 2

# target table validation
from importlib.util import spec_from_file_location, module_from_spec
spec = spec_from_file_location("delta_helpers", f"{ROOT}/utils/delta_helpers.py"); h = module_from_spec(spec); spec.loader.exec_module(h)
for v in ["silver.TAGS", "TAGS", "silver.dbo.TAGS"]:
    h.validate_table_name(v)
for v in ["", "a.b.c.d", "silver.TAGS; DROP TABLE x", "silver..TAGS"]:
    try: h.validate_table_name(v); raise SystemExit(f"accepted {v}")
    except ValueError: pass

# metadata
meta = dict(p_source_path=f"{bronze}/rlayer_SCADA/PlantA/Boiler/TAGS.parquet", p_target_table="silver.TAGS")
r = run("nb_metadata_sync", **meta); print(r); assert r["rows_staged"] == 2
write(f"{bronze}/rlayer_SCADA/PlantA/Boiler/TAGS.parquet", [(1, "Temp", "K", 11.0), (3, "Flow", "m3", 1.0)], ["TagId", "Description", "Unit", "LastValue"])
r = run("nb_metadata_sync", **meta); print(r)
spark.table("silver.TAGS").orderBy("TagId").show()
rows = {x.TagId: x.Unit for x in spark.table("silver.TAGS").collect()}
assert rows == {1: "K", 2: "bar", 3: "m3"}, rows
for bad in [dict(p_target_table="silver.TAGS; DROP TABLE x"), dict(p_source_path="")]:
    try:
        run("nb_metadata_sync", **{**meta, **bad}); raise SystemExit(f"validation failed for {bad}")
    except ValueError as e: print("rejected:", e)
print("ALL OK")
