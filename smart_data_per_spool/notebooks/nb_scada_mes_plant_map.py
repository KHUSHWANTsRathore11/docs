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
# Derives which MES plant each SCADA plant / department belongs to, from two inputs only.
# Spec: `smart_data_per_spool/plant_machine_tag_mapping.md`, section "Plant Name Mapping".
#
# | | |
# |---|---|
# | **Inputs** | CSV of SCADA plant, department and server host name; Silver `MI_CONNECTOR` |
# | **Logic** | CSV host name = `MI_CONNECTOR.HostName` (trimmed, case-insensitive) → MES plant on the connector |
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

# MI_CONNECTOR (Silver)
p_connector_table        = "MI_CONNECTOR"
p_connector_host_column  = "HostName"
p_connector_plant_column = "PlantName"   # MES plant column on MI_CONNECTOR, to be confirmed

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
print(f"Connector table: {p_connector_table}")
print(f"Manual entries : {len(manual_mapping)}")

# CELL ********************

# CSV host → MI_CONNECTOR host → MES plant
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
connectors = (
    spark.table(p_connector_table)
    .select(
        host_key(F.col(p_connector_host_column)).alias("HostKey"),
        F.trim(F.col(p_connector_plant_column)).alias("MesPlant"),
    )
    .withColumn("InConnector", F.lit(True))
    .dropDuplicates()
)

host_rows = (
    hosts.join(connectors, "HostKey", "left")
    .select("ScadaPlant", "Department", "HostName", "InConnector", "MesPlant")
    .orderBy("ScadaPlant", "Department", "HostName", "MesPlant")
    .collect()
)

print("CSV host → MI_CONNECTOR MES plant:")
spark.createDataFrame(host_rows, "ScadaPlant string, Department string, HostName string, "
                      "InConnector boolean, MesPlant string").show(500, truncate=False)

# CELL ********************

# One MES plant per SCADA plant / department
by_key = {}
for row in host_rows:
    entry = by_key.setdefault((row.ScadaPlant, row.Department), {"hosts": {}, "mes_plants": set()})
    entry["hosts"].setdefault(row.HostName, bool(row.InConnector))
    if row.MesPlant:
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
        for host_name, in_connector in sorted(entry["hosts"].items()):
            unmatched.append({
                "ScadaPlant": scada_plant,
                "Department": department,
                "HostName": host_name,
                "Reason": "NO_MES_PLANT_ON_CONNECTOR" if in_connector else "HOST_NOT_IN_MI_CONNECTOR",
            })
    else:
        ambiguous.append({
            "ScadaPlant": scada_plant,
            "Department": department,
            "MesPlants": sorted(entry["mes_plants"]),
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
