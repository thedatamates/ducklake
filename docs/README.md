# DuckLake for Crucible

This fork adds catalog isolation and Crucible-managed lifecycle to the DuckDB DuckLake extension. The port targets upstream `7963da4265c0ed09681821a0f3a158b17573ded4`, with catalog behavior derived from fork commit `4ccee75fe091291ad722c41363c0cdc145ae9bad`.

DuckLake stores table metadata in SQL and data in Parquet. [Upstream documentation](https://ducklake.select) describes its analytical features; provisioning and catalog lifecycle in this fork are owned by Crucible.

## Provisioning and attachment

Crucible provisions and migrates metadata through `crucible mb migrate`. Its authoritative migrations live in `macro/services/crucible/src/migration/metabase/` in Monogram. The format marker is `1.1-dev1-catalog2`; the file migration upgrades catalog1 metadata. Drain existing connections before migration, then reopen with the matching extension.

Create catalogs through Crucible, then attach their numeric IDs:

```sql
ATTACH 'ducklake:postgres:dbname=metabase' AS workbook
    (CATALOG_ID 42, DATA_PATH '/shared/data/');
SELECT * FROM workbook.main.example;
```

`CATALOG_ID` is required. Optional `CATALOG` must match the stored name. `DATA_PATH` is the shared storage root. The extension does not initialize or automatically migrate metadata. Installing the official upstream extension does not install this fork.

Supported metadata backends are PostgreSQL and DuckDB. DuckDB cannot attach the same metadata file under multiple names simultaneously; the service uses PostgreSQL for shared catalogs.

## Allocation, forks and retention

Metadata queries and joins include catalog identity. Snapshot IDs come from a shared sequence; the extension and Crucible use a common transactional guard for shared object/file counters. Sequence gaps after rollback are valid.

Crucible performs head and selected-snapshot catalog forks. Parquet files remain shared references, while physical inline-data and inline-deletion tables are copied independently at the selected state. Older schema definitions are retained when inline data still uses those layouts. Pre-birth table/view reads resolve through the parent's recorded snapshot cutoff, recursively for successive forks. Snapshot listings include that bounded ancestor history. Change-range scans across catalog-fork boundaries are rejected; exact historical snapshots remain readable.

Native files have catalog/schema-scoped keys and snapshot-versioned metadata in `ducklake_file`, separate from table Parquet records in `ducklake_data_file`. Crucible owns their publication and byte access. File changes participate in snapshot change parsing; engine schema drops with live native files are rejected, including CASCADE. Native files do not appear in `information_schema.tables`.

File cleanup retains files referenced by any catalog, including historical native-file references. Both cleanup paths protect the reserved `_files/` storage area, including staged bytes. Orphan detection includes all catalogs sharing the storage root. Snapshot expiration is rejected because upstream's expiration implementation assumes exclusive ownership of the global snapshot history.

## Building

Keep the pinned submodules: DuckDB `ef853aebf803cc4f7738ffc34859227f5ebb6437` and extension-ci-tools `795096d04b009c0d087468439ebb526a5460dfac`.

```bash
git submodule update --init --recursive
ENABLE_POSTGRES_SCANNER=1 CMAKE_BUILD_PARALLEL_LEVEL=8 GEN=ninja make release
```

The pinned PostgreSQL scanner requires libpq 18. The local macOS build uses Homebrew Roaring and libpq 18.6; if CMake selects an older PostgreSQL installation, set `PostgreSQL_LIBRARY` and `PostgreSQL_INCLUDE_DIR` explicitly during configuration.

Crucible links `build/release/src/libduckdb` and loads the extensions under `build/release/extension`. Build and distribute these together. Its `agent-data-duck` dependency disables the bundled engine; other Rust consumers can still enable that feature.

## Verification

```bash
./build/release/test/unittest --test-dir . --test-config test/configs/managed.json '~[.]test/sql/*'
make format-fix
```

The analytical suite provisions metadata explicitly. [Testing](TESTING.md) documents the direct PostgreSQL fork and isolation tests, feature coverage, restored historical regressions and unsupported upstream lifecycle exclusions. Crucible's integration suite remains separate.
