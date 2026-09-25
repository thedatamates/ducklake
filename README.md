# DuckLake for Crucible

This is [thedatamates/ducklake](https://github.com/thedatamates/ducklake), our fork of the DuckDB DuckLake extension. It adds catalog isolation over shared metadata and storage. Crucible owns metadata provisioning, catalog creation and catalog forks; DuckLake executes SQL and maintains table metadata and Parquet data.

The current integration uses upstream `7963da4265c0ed09681821a0f3a158b17573ded4` and carries forward our catalog behavior from `4ccee75fe091291ad722c41363c0cdc145ae9bad`. Both histories are retained in Git.

## Getting started

Build the pinned DuckDB runtime and extensions together using the [build instructions](docs/BUILD.md). Provision the metabase and create a catalog through Crucible, then attach its numeric ID:

```sql
ATTACH 'ducklake:postgres:dbname=metabase' AS workbook
    (CATALOG_ID 42, DATA_PATH '/shared/data/');
```

The metadata format is `1.1-dev1-catalog3`. The authoritative schema and migrations live in Monogram at `macro/services/crucible/src/migration/metabase/`. Crucible upgrades catalog1 metadata with native file records and a parent snapshot cutoff for forks, then catalog2 metadata with alias records. Drain existing connections before migration and reopen them with this matching extension. The extension does not create or migrate metadata.

## Documentation

- [Lifecycle and supported behavior](docs/README.md)
- [Architecture and implementation map](docs/ARCHITECTURE.md)
- [Building and runtime compatibility](docs/BUILD.md)
- [PostgreSQL setup](docs/POSTGRESQL.md)
- [Snapshot allocation and concurrency](docs/SNAPSHOT_SEQUENCE.md)
- [Test commands, feature coverage and exclusions](docs/TESTING.md)
- [Rust feasibility research](docs/RUST_RESEARCH.md)

The analytical SQL suite uses explicitly provisioned catalog fixtures. Dedicated native and PostgreSQL tests cover catalog isolation, concurrent allocation, lineage, forks and retention. [Testing](docs/TESTING.md) records coverage and the explicit exclusions for unsupported upstream lifecycle behavior. Production packaging has not been validated.

Snapshot expiration is disabled because upstream's implementation assumes exclusive ownership of the snapshot history. Cleanup protects table files and native file versions referenced by any catalog, plus the reserved `_files/` storage area. Crucible writes and reads native files; they are not SQL tables. Engine schema drops reject live native files, including CASCADE. Forks retain their selected source snapshot. Earlier table/view reads resolve through bounded ancestry, including successive forks; later parent changes do not enter child history. Change-range scans crossing a fork boundary are rejected rather than mixing catalog namespaces.
