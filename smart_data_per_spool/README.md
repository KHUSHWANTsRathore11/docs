# Smart Data per Spool

Smart Data per Spool is a Microsoft Fabric project. A framework for it is being created. Details are added as they are provided.

## Files in This Folder

| File | Contents |
| :--- | :--- |
| `README.md` | Project overview and open items for the framework as a whole. |
| `table_details.md` | Column details for each source table: SCADA TAGS and TAGLOG, and MES tables. |
| `plant_machine_tag_mapping.md` | Logic that maps each SCADA tag to its plant and machine in MES. |
| `spool_window_creation.md` | Logic for creating spool windows: historical and future data. |
| `notebooks/` | Fabric notebooks (Runtime 2.0). |
| `tests/` | Local PySpark smoke tests for the notebooks. |

## To Be Provided

- Purpose and definition of "spool" in this project.
- Framework layers, pipelines and target tables.
- Keys, data types, and relationships between the tables.
- Refresh frequency and data volume.
- Owners and access.
