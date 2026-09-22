# DuckLake architecture

## Ownership

Crucible and the DuckLake extension write the same metabase. Crucible provisions the schema, creates catalogs and schemas, and forks catalogs. The extension attaches an existing catalog and handles SQL, transaction commits, table metadata, inline rows and Parquet storage.

The metabase is shared across catalogs. Catalog-owned relations use `catalog_id` in their keys and queries. Format metadata, snapshot IDs and object/file allocation counters are shared. Catalog isolation is implemented in metadata lookups and writes; it is not a PostgreSQL schema per workbook.

## Attachment and paths

`CATALOG_ID` selects an active `ducklake_catalog` row. `CATALOG`, when supplied, must match that row's name. The SQL attachment alias is independent of both. `METADATA_SCHEMA` selects the SQL schema containing the metabase relations; it does not select an analytical schema inside a catalog.

`DATA_PATH` is the shared storage root. `BaseDataPath()` resolves stored relative paths from that root. The effective catalog data path adds the stored catalog name. Forks retain paths to shared files, so a child's current data can remain under its parent's physical prefix. Ownership cannot be inferred from the path alone.

## Transactions and metadata

DuckDB binds SQL against DuckLake catalog entries and builds scan/write operators. Reads select metadata visible at the transaction snapshot; table data comes from Parquet and any inline tables. Writes collect transaction changes, allocate a snapshot and commit metadata through the backend manager. The extension and Crucible use the same transactional guard to protect shared allocation counters. See [snapshot allocation](SNAPSHOT_SEQUENCE.md).

Table, column and schema definitions have snapshot visibility. Table schema versions are recorded per table. Settings are catalog scoped; the format marker is global. Inline physical tables include catalog identity:

```text
ducklake_inlined_data_<catalog_id>_<table_id>_<schema_version>
ducklake_inlined_delete_<catalog_id>_<table_id>
```

## Forks and retention

Crucible copies current catalog metadata into a new catalog. Parquet files remain shared; inline data and inline deletions are copied to independent physical tables. Current data may use older column layouts, so a fork also retains the schema definitions required to read those layouts. This is not ancestor data-history inheritance. User-visible time travel is bounded by catalog creation.

Cleanup checks references from every catalog, including historical data-file and delete-file references. Failed reference checks abort cleanup. Orphan detection includes all catalogs sharing the root. Snapshot expiration is rejected until it can respect the shared history.

## Implementation map

| Area | Source |
|---|---|
| Extension registration and SQL attachment options | `src/ducklake_extension.cpp`, `src/storage/ducklake_storage.cpp` |
| Provisioning checks, catalog identity and data paths | `src/storage/ducklake_initializer.cpp`, `src/include/storage/ducklake_catalog.hpp` |
| Catalog entries and SQL planning | `src/storage/ducklake_catalog.cpp`, `src/storage/ducklake_schema_entry.cpp`, `src/storage/ducklake_table_entry.cpp` |
| Metadata queries, catalog predicates and cleanup checks | `src/storage/ducklake_metadata_manager.cpp` |
| PostgreSQL query execution and snapshot allocation | `src/metadata_manager/postgres_metadata_manager.cpp` |
| Transaction changes, conflict checks and commits | `src/storage/ducklake_transaction.cpp`, `src/storage/ducklake_transaction_state.cpp` |
| Catalog discovery | `src/functions/ducklake_catalogs.cpp` |
| Inline flushing and file cleanup entry points | `src/functions/ducklake_flush_inlined_data.cpp`, `src/functions/ducklake_cleanup_files.cpp` |
| Managed-catalog regression | `test/sql/multi_catalog/managed_catalogs.test` |

Upstream version-manager DDL remains in the source, but the managed-catalog initialization path rejects creation and migration. Crucible's baseline is authoritative; those upstream statements are not provisioning scripts for this fork.
