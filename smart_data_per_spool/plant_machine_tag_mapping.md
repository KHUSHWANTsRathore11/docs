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

## Plant-Scoped Join

The Silver tables hold data for all plants, so every join must include the plant. The MES and SCADA plant names differ, so the derived plant mapping is the join key between them.

1. Build the derived plant mapping from the CSV and `MI_CONNECTOR` (see Plant Name Mapping). Result: `(SCADA PlantName, DepartmentName)` → MES `PlantName`.
2. Join `scada_tags` to the mapping on `(PlantName, DepartmentName)`, giving each SCADA tag its MES plant.
3. Join to `MI_POINT` on `DbTag` = `OPCpointName` **and** MES `PlantName` = `MI_POINT.PlantName`.
4. Continue the chain from `MI_POINT` to `Resource_` as above.

Why the plant is part of the join: `TagId` restarts per plant and department, and a `DbTag` value can appear in more than one plant. Matching on the tag name alone could link a tag to the wrong plant's point.

## Plant Name Mapping

MES `PlantName` and SCADA `PlantName` are **not always the same**. This must be handled in the logic.

The mapping is derived from a CSV file, not typed by hand:

- The CSV lists each SCADA `PlantName`, `DepartmentName` and server `hostname`. One SCADA plant has several hosts, one per department.
- MES has one server per plant. `MI_CONNECTOR` is the link: `CSV.hostname` = `MI_CONNECTOR.HostName`, and from there the chain goes up to `MI_POINT.PlantName`.
- The result is a mapping at grain `(SCADA PlantName, DepartmentName, hostname)` → MES `PlantName`. Several rows can map to the same MES plant.
- This derived mapping is then used when joining SCADA tags to MES points.
- A manual list is kept only as a fallback for hosts that do not match `MI_CONNECTOR`.

Checks on the derived mapping:
- CSV hostnames with no `MI_CONNECTOR` match.
- A host that resolves to more than one MES plant.
- A host with no points under its connector.

## To Be Provided

- Sample `Parameters` XML from MI_POINT, to confirm the element path and any namespaces for `<OPCpointName>`.
- The CSV file: its name, location, and exact columns (SCADA PlantName, DepartmentName, hostname).
- Whether the CSV hostnames match `MI_CONNECTOR.HostName` exactly, or need trimming or case-insensitive matching.
- Manual fallback mapping for hosts that do not match `MI_CONNECTOR`, if any.
- List of departments, for the per-department join.
- Whether `OPCpointName` can be empty, missing, or repeated across rows.
- Whether `Unit_Characteristic` has one row per (UnitId, Characteristic), and whether a `"MiConnector"` value can appear more than once.
- Whether `MI_CONNECTOR.HostName` and `Unit_Characteristic.Value_` need trimming or case-insensitive matching.
- Whether `MI_POINT` can have more than one row for the same `OPCpointName`.
- Whether a tag can map to more than one machine or plant, and which result is wanted when it does.
- Platform for the join: SQL view or notebook step.
