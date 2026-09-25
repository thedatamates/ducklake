# PostgreSQL metadata

PostgreSQL is the shared metabase used by Crucible. Multiple DuckLake catalogs occupy the same metabase relations, distinguished by `catalog_id`. `METADATA_SCHEMA` selects the PostgreSQL schema holding those relations; it does not provide catalog isolation.

## Provisioning

Build the [matched runtime and PostgreSQL scanner](BUILD.md). Provision the fresh metabase with `crucible mb migrate`, using Crucible's configured metabase connection, then create catalogs through Crucible. Its authoritative schema is `macro/services/crucible/src/migration/metabase/schema.sql` in Monogram.

The format marker is `1.1-dev1-catalog2`. In addition to the shared snapshot sequence, snapshot lineage and catalog-aware keys, it includes versioned native files and the source snapshot for catalog forks. Crucible's file migration upgrades catalog1 metadata; drain existing attachments before migration and reopen with the matching extension. The extension requires an existing schema and active catalog; ATTACH does not provision either.

## Attachment

Use the matched shell and locally built extensions:

```bash
./build/release/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:postgres:host=127.0.0.1 dbname=metabase user=crucible'
    AS workbook (CATALOG_ID 42, DATA_PATH '/shared/data/', METADATA_SCHEMA 'public');
```

Replace the connection, root and numeric ID with those provisioned for the application. Optional `CATALOG` must match the stored catalog name. SQL aliases can differ from catalog names.

The PostgreSQL manager executes metadata SQL through the scanner's `postgres_query` and `postgres_execute` wrappers. The remote SQL qualifies relations and sequences by PostgreSQL schema; the attachment name is passed separately to the wrapper. The sequence query keeps preparation enabled so it returns the allocated value, not a row count.

## Concurrent writers

The extension and Crucible acquire the same transactional guard before consuming shared allocation counters. Retrying conflicts and sequence gaps are expected. See [snapshot allocation](SNAPSHOT_SEQUENCE.md).

Integration validation used a disposable PostgreSQL 17 cluster on `127.0.0.1:55439`, with explicit test connection overrides. Follow Crucible's test README when repeating these tests: its fixtures reset the dedicated test databases. No production database or existing application data was used for validation.
