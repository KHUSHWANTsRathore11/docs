# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # nb_scada_mes_plant_map — SCADA plant to MES plant mapping
#
# Derives which MES plant each SCADA plant / department belongs to.
# Spec: `smart_data_per_spool/plant_machine_tag_mapping.md`, section "Plant Name Mapping".
#
# | | |
# |---|---|
# | **Inputs** | CSV of SCADA plant, department and server host name; Silver `MI_CONNECTOR`, `MI_DATA_SOURCE`, `MI_POINT_GROUP`, `MI_POINT` |
# | **Logic** | CSV host name = `MI_CONNECTOR.HostName` (trimmed, case-insensitive) → data source → point group → point → `MI_POINT.PlantName` |
# | **Manual mapping** | `p_manual_mapping_json` entries replace the derived row for the same SCADA plant / department, or add a missing one |
# | **Output** | Exit value: JSON dict with `mapping` = list of `{ScadaPlant, Department, MesPlant, Source}`, plus `unmatched` and `ambiguous` for review |
#
# Writes nothing. Run it on its own to review the mapping, or from `nb_plant_machine_tag_map`.
# Relative `Files/...` paths and bare table names resolve against the default lakehouse (attach Silver).

# PARAMETERS CELL ********************

# CSV with one row per SCADA plant / department / server host
p_host_csv_path          = "Files/config/scada_plant_hosts.csv"
p_csv_plant_column       = "PlantName"
p_csv_department_column  = "DepartmentName"
p_csv_hostname_column    = "HostName"

# MES tables (Silver) and their columns
p_point_table            = "MI_POINT"
p_point_group_column     = "PointGroupId"
p_point_plant_column     = "PlantName"
p_point_group_table      = "MI_POINT_GROUP"
p_point_group_id_column  = "Id"
p_group_data_source_column = "DataSourceId"
p_data_source_table      = "MI_DATA_SOURCE"
p_data_source_id_column  = "Id"
p_data_source_connector_column = "ConnectorId"
p_connector_table        = "MI_CONNECTOR"
p_connector_id_column    = "Id"
p_connector_host_column  = "HostName"

# Manual entries, JSON list. Example:
# '[{"ScadaPlant": "PlantA", "Department": "Boiler", "MesPlant": "PLANT_A"}]'
p_manual_mapping_json    = "[]"

# CELL ********************

import json

from pyspark.sql import functions as F

MAPPING_KEYS = ("ScadaPlant", "Department", "MesPlant")


def host_key(column):
    """Host names are compared trimmed and case-insensitive."""
    return F.upper(F.trim(column))


def parse_manual_mapping(text):
    entries = json.loads(text or "[]")
    if not isinstance(entries, list):
        raise ValueError("p_manual_mapping_json must be a JSON list")
    parsed = []
    for i, entry in enumerate(entries):
        missing = [k for k in MAPPING_KEYS if not str(entry.get(k) or "").strip()]
        if missing:
            raise ValueError(f"p_manual_mapping_json entry {i} is missing {missing}: {entry}")
        parsed.append({k: str(entry[k]).strip() for k in MAPPING_KEYS})
    return parsed


manual_mapping = parse_manual_mapping(p_manual_mapping_json)
print(f"Host CSV       : {p_host_csv_path}")
print(f"Manual entries : {len(manual_mapping)}")

# CELL ********************

# SCADA side: plant / department / host from the CSV
hosts = (
    spark.read.option("header", True).csv(p_host_csv_path)
    .select(
        F.trim(F.col(p_csv_plant_column)).alias("ScadaPlant"),
        F.trim(F.col(p_csv_department_column)).alias("Department"),
        F.trim(F.col(p_csv_hostname_column)).alias("HostName"),
    )
    .filter("ScadaPlant <> '' AND Department <> '' AND HostName <> ''")
    .dropDuplicates()
    .withColumn("HostKey", host_key(F.col("HostName")))
)

# MES side: which MES plants sit under each connector host
connectors = spark.table(p_connector_table).select(
    F.col(p_connector_id_column).alias("ConnectorId"),
    host_key(F.col(p_connector_host_column)).alias("HostKey"),
)
data_sources = spark.table(p_data_source_table).select(
    F.col(p_data_source_id_column).alias("DataSourceId"),
    F.col(p_data_source_connector_column).alias("ConnectorId"),
)
point_groups = spark.table(p_point_group_table).select(
    F.col(p_point_group_id_column).alias("PointGroupId"),
    F.col(p_group_data_source_column).alias("DataSourceId"),
)
points = spark.table(p_point_table).select(
    F.col(p_point_group_column).alias("PointGroupId"),
    F.trim(F.col(p_point_plant_column)).alias("MesPlant"),
)

host_mes_plants = (
    connectors
    .join(data_sources, "ConnectorId")
    .join(point_groups, "DataSourceId")
    .join(points, "PointGroupId")
    .filter("MesPlant IS NOT NULL AND MesPlant <> ''")
    .groupBy("HostKey", "MesPlant")
    .agg(F.count(F.lit(1)).alias("Points"))
)
connector_hosts = connectors.select("HostKey").distinct().withColumn("InConnector", F.lit(True))

host_rows = (
    hosts
    .join(connector_hosts, "HostKey", "left")
    .join(host_mes_plants, "HostKey", "left")
    .select("ScadaPlant", "Department", "HostName", "InConnector", "MesPlant", "Points")
    .orderBy("ScadaPlant", "Department", "HostName", "MesPlant")
    .collect()
)

print("Host → MES plant (one row per host and MES plant):")
spark.createDataFrame(host_rows, "ScadaPlant string, Department string, HostName string, "
                      "InConnector boolean, MesPlant string, Points long").show(200, truncate=False)

# CELL ********************

# Resolve one MES plant per SCADA plant / department
by_key = {}
for row in host_rows:
    key = (row.ScadaPlant, row.Department)
    entry = by_key.setdefault(key, {"hosts": {}, "mes_plants": set()})
    host = entry["hosts"].setdefault(row.HostName, {"in_connector": bool(row.InConnector), "mes_plants": set()})
    if row.MesPlant:
        host["mes_plants"].add(row.MesPlant)
        entry["mes_plants"].add(row.MesPlant)

derived, unmatched, ambiguous = {}, [], []
for (scada_plant, department), entry in sorted(by_key.items()):
    if len(entry["mes_plants"]) == 1:
        derived[(scada_plant, department)] = {
            "ScadaPlant": scada_plant,
            "Department": department,
            "MesPlant": next(iter(entry["mes_plants"])),
            "Source": "DERIVED",
        }
    elif not entry["mes_plants"]:
        for host_name, host in sorted(entry["hosts"].items()):
            unmatched.append({
                "ScadaPlant": scada_plant,
                "Department": department,
                "HostName": host_name,
                "Reason": "NO_POINTS_UNDER_CONNECTOR" if host["in_connector"] else "HOST_NOT_IN_MI_CONNECTOR",
            })
    else:
        ambiguous.append({
            "ScadaPlant": scada_plant,
            "Department": department,
            "MesPlants": sorted(entry["mes_plants"]),
            "Hosts": {h: sorted(v["mes_plants"]) for h, v in sorted(entry["hosts"].items())},
        })

# Manual entries win over derived ones and resolve unmatched / ambiguous keys
mapping = dict(derived)
for entry in manual_mapping:
    key = (entry["ScadaPlant"], entry["Department"])
    if key in derived and derived[key]["MesPlant"] != entry["MesPlant"]:
        print(f"Manual entry overrides derived MES plant for {key}: "
              f"{derived[key]['MesPlant']} → {entry['MesPlant']}")
    mapping[key] = {**entry, "Source": "MANUAL"}

unmatched = [u for u in unmatched if (u["ScadaPlant"], u["Department"]) not in mapping]
ambiguous = [a for a in ambiguous if (a["ScadaPlant"], a["Department"]) not in mapping]
mapping_list = [mapping[k] for k in sorted(mapping)]

print(f"Mapped SCADA plant / departments : {len(mapping_list)} "
      f"(derived {sum(m['Source'] == 'DERIVED' for m in mapping_list)}, "
      f"manual {sum(m['Source'] == 'MANUAL' for m in mapping_list)})")
print(f"Unmatched hosts                  : {len(unmatched)}")
print(f"Ambiguous plant / departments    : {len(ambiguous)}")
for u in unmatched:
    print(f"  UNMATCHED {u}")
for a in ambiguous:
    print(f"  AMBIGUOUS {a}")

# CELL ********************

result = {
    "notebook": "nb_scada_mes_plant_map",
    "status": "SUCCEEDED" if not (unmatched or ambiguous) else "SUCCEEDED_WITH_GAPS",
    "mapping": mapping_list,
    "unmatched": unmatched,
    "ambiguous": ambiguous,
}
notebookutils.notebook.exit(json.dumps(result))
