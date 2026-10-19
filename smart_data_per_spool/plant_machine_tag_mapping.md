# Plant Machine Tag Mapping

Maps each SCADA tag (TAGS) to its plant and machine in MES. Table and column details are in `table_details.md`.

## Tag to MES Point

- `MI_POINT.Parameters` is XML. The OPC point name is extracted from `<OPCpointName>`.
- The extracted `OPCpointName` matches `TAGS.DbTag` one to one.
- The join is built for each department.

## Join Chain (TAGS to Machine)

Joins are listed in order.

1. `TAGS.DbTag` = `MI_POINT.Parameters` → `<OPCpointName>` (one to one).
2. `MI_POINT.PointGroupId` = `MI_POINT_GROUP.Id`.
3. `MI_POINT_GROUP.DataSourceId` = `MI_DATA_SOURCE.Id`.
4. `MI_DATA_SOURCE.ConnectorId` = `MI_CONNECTOR.Id`.
5. `MI_CONNECTOR.HostName` is the host name used for the next step.
6. `Unit_Characteristic` is stored in rows, with one column per UnitId. Filter the rows where `Characteristic` = `"MiConnector"`, then match `Value_` to `MI_CONNECTOR.HostName`.
7. From the matched row, take `UnitId`.
8. `Unit_Characteristic.UnitId` = `Resource_.UnitId` gives the machine.

Machine comes from `Resource_` at step 8. The column that holds the machine name is to be provided.

## Plant Name Mapping

MES `PlantName` and SCADA `PlantName` are **not always the same**. This must be handled in the logic.

- For now, use a static mapping of MES plant name to SCADA plant name.
- Later, the mapping may move to a config table or an Excel file. This is not decided.
- The static mapping has not been provided yet.

## To Be Provided

- Sample `Parameters` XML from MI_POINT, to confirm the element path and any namespaces for `<OPCpointName>`.
- Static plant name mapping (MES PlantName → SCADA PlantName).
- List of departments, for the per-department join.
- Whether `OPCpointName` can be empty, missing, or repeated across rows.
- Whether `Unit_Characteristic` has one row per (UnitId, Characteristic), and whether a `"MiConnector"` value can appear more than once.
- Whether `MI_CONNECTOR.HostName` and `Unit_Characteristic.Value_` need trimming or case-insensitive matching.
- Whether `MI_POINT` can have more than one row for the same `OPCpointName`.
- Whether a tag can map to more than one machine or plant, and which result is wanted when it does.
- Platform for the join: SQL view or notebook step.
