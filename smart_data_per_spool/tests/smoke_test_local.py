"""Local smoke test for the plant machine tag mapping notebooks.

Runs nb_scada_mes_plant_map and nb_plant_machine_tag_map end to end on small synthetic Silver
tables (temp views) with a local PySpark session and a stub `notebookutils`.

    pip install pyspark==4.0.1      # Fabric Runtime 2.0 is Spark 4.0; needs Java 17+
    python smart_data_per_spool/tests/smoke_test_local.py
"""

import json
import os
import re
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
CELL_MARKER = re.compile(r"^# (?:PARAMETERS CELL|CELL) \*+$", re.MULTILINE)


class NotebookExit(Exception):
    def __init__(self, value):
        super().__init__(value)
        self.value = value


class NotebookStub:
    @staticmethod
    def exit(value):
        raise NotebookExit(value)

    @staticmethod
    def run(name, timeout_seconds, arguments=None):
        return run_notebook(name, arguments or {})


class NotebookUtilsStub:
    notebook = NotebookStub()


def run_notebook(name, params):
    """Run a notebook source file: parameters cell, then overrides, then the remaining cells."""
    source = (NOTEBOOKS / f"{name}.py").read_text()
    cells = CELL_MARKER.split(source)
    scope = {"spark": spark, "notebookutils": NotebookUtilsStub(), "__name__": name}
    exec(cells[0] + cells[1], scope)          # header + PARAMETERS CELL
    scope.update(params)
    try:
        for cell in cells[2:]:
            exec(cell, scope)
    except NotebookExit as done:
        return done.value
    return None


spark = (SparkSession.builder.master("local[2]").appName("smoke_test")
         .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def view(name, schema, rows):
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)


# SCADA host CSV: Press host differs in case / spacing from MI_CONNECTOR, PlantB has no points,
# PlantC is not in MI_CONNECTOR, PlantD's connector feeds two MES plants
csv_dir = tempfile.mkdtemp()
csv_path = os.path.join(csv_dir, "scada_plant_hosts.csv")
Path(csv_path).write_text(
    "PlantName,DepartmentName,HostName\n"
    "PlantA,Boiler,scada-a-boiler\n"
    "PlantA,Press,SCADA-A-PRESS\n"
    "PlantB,Boiler,scada-b-boiler\n"
    "PlantC,Mill,scada-c-mill\n"
    "PlantD,Line,scada-d-line\n"
)

view("MI_CONNECTOR", "Id int, HostName string", [
    (1, "scada-a-boiler"), (2, "scada-a-press "), (3, "scada-b-boiler"), (4, "scada-d-line"), (5, "orphan-host"),
])
view("MI_DATA_SOURCE", "Id int, ConnectorId int", [
    (10, 1), (20, 2), (30, 99), (40, 4), (50, 5),
])
view("MI_POINT_GROUP", "Id int, DataSourceId int", [
    (100, 10), (200, 20), (300, 30), (400, 40), (401, 40), (500, 50), (999, 77),
])
view("MI_POINT", "Id int, PointGroupId int, Parameters string, PlantName string", [
    (1000, 100, "<Point><OPCpointName>Boiler1.Temp</OPCpointName></Point>", "MES_A"),
    (1001, 100, "<p:Point xmlns:p='x'><p:OpcPointName a='1'> Boiler1.Press </p:OpcPointName></p:Point>", "MES_A"),
    (1002, 200, "<Point><OPCpointName>Press1.Force&amp;X</OPCpointName></Point>", "MES_A"),
    (1003, 200, "<Point><OPCpointName><![CDATA[Press1.Speed]]></OPCpointName></Point>", "MES_A"),
    (1004, 999, "<Point><OPCpointName>Boiler1.Level</OPCpointName></Point>", "MES_A"),
    (1005, 100, "<Point><broken", "MES_A"),
    (1006, 400, "<Point><OPCpointName>Line.Speed</OPCpointName></Point>", "MES_D1"),
    (1007, 401, "<Point><OPCpointName>Line.Speed</OPCpointName></Point>", "MES_D2"),
    (1009, 100, "<Point><OPCpointName>Boiler1.Dup</OPCpointName></Point>", "MES_A"),
    (1010, 100, "<Point><OPCpointName>Boiler1.Dup</OPCpointName></Point>", "MES_A"),
    (1011, 300, "<Point><OPCpointName>Boiler1.Conn</OPCpointName></Point>", "MES_A"),
    (1012, 500, "<Point><OPCpointName>Boiler1.NoUnit</OPCpointName></Point>", "MES_A"),
    (1013, 999, "<Point><OPCpointName>Boiler1.Temp</OPCpointName></Point>", "MES_X"),
])
view("Unit_Characteristic", "UnitId int, Characteristic string, Value_ string", [
    (5001, "MiConnector", "SCADA-A-BOILER"), (5001, "Location", "Hall 1"), (5002, " miconnector ", "scada-a-press"),
])
view("Resource_", "UnitId int, Name string", [(5001, "Boiler Machine 1")])
view("scada_tags", "PlantName string, DepartmentName string, TagId int, DbTag string, Description string", [
    ("PlantA", "Boiler", 1, "Boiler1.Temp", ""),
    ("PlantA", "Boiler", 2, " Boiler1.Press", ""),
    ("PlantA", "Boiler", 3, "Boiler1.Level", ""),
    ("PlantA", "Boiler", 4, "Boiler1.Dup", ""),
    ("PlantA", "Boiler", 5, None, ""),
    ("PlantA", "Boiler", 6, "Unknown.Tag", ""),
    ("PlantA", "Boiler", 7, "Boiler1.Conn", ""),
    ("PlantA", "Boiler", 8, "Boiler1.NoUnit", ""),
    ("PlantA", "Press", 1, "Press1.Force&X", ""),
    ("PlantA", "Press", 2, "Press1.Speed", ""),
    ("PlantB", "Boiler", 1, "Boiler1.Temp", ""),
    ("PlantC", "Mill", 1, "Mill.X", ""),
    ("PlantD", "Line", 1, "Line.Speed", ""),
    ("PlantE", "Dept", 1, "Boiler1.Temp", ""),
])

manual = json.dumps([{"ScadaPlant": "PlantC", "Department": "Mill", "MesPlant": "MES_C"}])
plant_map_args = {"p_host_csv_path": csv_path, "p_manual_mapping_json": manual}

# --- Notebook 1 on its own ---------------------------------------------------------------------
result = json.loads(run_notebook("nb_scada_mes_plant_map", plant_map_args))
assert result["status"] == "SUCCEEDED_WITH_GAPS", result["status"]
assert result["mapping"] == [
    {"ScadaPlant": "PlantA", "Department": "Boiler", "MesPlant": "MES_A", "Source": "DERIVED"},
    {"ScadaPlant": "PlantA", "Department": "Press", "MesPlant": "MES_A", "Source": "DERIVED"},
    {"ScadaPlant": "PlantC", "Department": "Mill", "MesPlant": "MES_C", "Source": "MANUAL"},
], result["mapping"]
assert result["unmatched"] == [{"ScadaPlant": "PlantB", "Department": "Boiler", "HostName": "scada-b-boiler",
                                "Reason": "NO_POINTS_UNDER_CONNECTOR"}], result["unmatched"]
assert [(a["ScadaPlant"], a["MesPlants"]) for a in result["ambiguous"]] == [("PlantD", ["MES_D1", "MES_D2"])]

no_manual = json.loads(run_notebook("nb_scada_mes_plant_map", {"p_host_csv_path": csv_path}))
assert {u["Reason"] for u in no_manual["unmatched"]} == {"NO_POINTS_UNDER_CONNECTOR", "HOST_NOT_IN_MI_CONNECTOR"}

# --- Notebook 2 (runs notebook 1) ----------------------------------------------------------------
def run_and_capture(name, params):
    source = (NOTEBOOKS / f"{name}.py").read_text()
    cells = CELL_MARKER.split(source)
    scope = {"spark": spark, "notebookutils": NotebookUtilsStub(), "__name__": name}
    exec(cells[0] + cells[1], scope)
    scope.update(params)
    for cell in cells[2:-1]:                  # skip the final write / unpersist cell
        exec(cell, scope)
    return scope


scope = run_and_capture("nb_plant_machine_tag_map", {"p_plant_map_args_json": json.dumps(plant_map_args)})
rows = {(r.ScadaPlant, r.Department, r.TagId, r.PointId): r for r in scope["mapping_df"].collect()}
status = {}
for (plant, dept, tag, _), r in rows.items():
    status.setdefault((plant, dept, tag), set()).add(r.LineageStatus)

expected = {
    ("PlantA", "Boiler", 1): {"MATCHED"},
    ("PlantA", "Boiler", 2): {"MATCHED"},
    ("PlantA", "Boiler", 3): {"NO_DATA_SOURCE"},
    ("PlantA", "Boiler", 4): {"MATCHED"},
    ("PlantA", "Boiler", 5): {"NO_DBTAG"},
    ("PlantA", "Boiler", 6): {"NO_POINT"},
    ("PlantA", "Boiler", 7): {"NO_CONNECTOR"},
    ("PlantA", "Boiler", 8): {"NO_UNIT"},
    ("PlantA", "Press", 1): {"NO_RESOURCE"},
    ("PlantA", "Press", 2): {"NO_RESOURCE"},
    ("PlantB", "Boiler", 1): {"NO_PLANT_MAPPING"},
    ("PlantC", "Mill", 1): {"NO_POINT"},
    ("PlantD", "Line", 1): {"NO_PLANT_MAPPING"},
    ("PlantE", "Dept", 1): {"NO_PLANT_MAPPING"},
}
assert status == expected, {k: v for k, v in status.items() if expected.get(k) != v}

# Plant scoping: Boiler1.Temp also exists in MES_X, but PlantA/Boiler maps only to MES_A's point
assert [k[3] for k in rows if k[:3] == ("PlantA", "Boiler", 1)] == [1000]
# Fan-out: Boiler1.Dup has two points in MES_A
assert sorted(k[3] for k in rows if k[:3] == ("PlantA", "Boiler", 4)) == [1009, 1010]
matched = rows[("PlantA", "Boiler", 1, 1000)]
assert (matched.UnitId, matched.res_Name, matched.MesPlant) == (5001, "Boiler Machine 1", "MES_A")
# XML: namespace prefix + attribute + whitespace, entity, CDATA
assert rows[("PlantA", "Boiler", 2, 1001)].LineageStatus == "MATCHED"
assert rows[("PlantA", "Press", 1, 1002)].UnitId == 5002
assert rows[("PlantA", "Press", 2, 1003)].UnitId == 5002

# The final write stays commented out
nb2 = (NOTEBOOKS / "nb_plant_machine_tag_map.py").read_text()
assert "\n# (mapping_df" in nb2 and "\n(mapping_df" not in nb2

spark.stop()
print("\nSMOKE TEST PASSED")
sys.exit(0)
