<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="../logo/DuckLake_Logo-horizontal.svg">
    <source media="(prefers-color-scheme: dark)" srcset="../logo/DuckLake_Logo-horizontal-dark.svg">
    <img alt="DuckLake logo" src="../logo/DuckLake_Logo-horizontal.svg" height="100">
  </picture>
</div>
<br>

# DuckDB DuckLake Extension

> While we tested the DuckLake extension extensively, it is currently experimental as demonstrated by its version number 0.x.
> If you encounter any problems, please file a [new issue](https://github.com/duckdb/ducklake/issues).

DuckLake is an open Lakehouse format that is built on SQL and Parquet. DuckLake stores metadata in a [catalog database](https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database), and stores data in Parquet files. The DuckLake extension allows DuckDB to directly read and write data from DuckLake.

See the [DuckLake website](https://ducklake.select) for more information.

## Additional Fork Docs

- Build instructions: [BUILD.md](./BUILD.md)
- PostgreSQL setup and behavior: [POSTGRESQL.md](./POSTGRESQL.md)
- Snapshot allocation and conflict-visibility notes: [SNAPSHOT_SEQUENCE.md](./SNAPSHOT_SEQUENCE.md)
- Architecture deep dive: [ARCHITECTURE.md](./ARCHITECTURE.md)

## Installation

DuckLake can be installed using the `INSTALL` command:

```sql
INSTALL ducklake;
```

The latest development version can be installed from `core_nightly`:

```sql
FORCE INSTALL ducklake FROM core_nightly;
```

## Usage

DuckLake databases can be attached using the  [`ATTACH`](https://duckdb.org/docs/stable/sql/statements/attach.html) syntax, after which tables can be created, modified and queried using standard SQL.

**Note:** This fork requires `CATALOG` for multi-tenant isolation. See [DUCKLAKE.md](../DUCKLAKE.md) for details.

Below is a short usage example that stores the metadata in a DuckDB database file called `metadata.ducklake`, and the data in Parquet files in the `file_path/my-catalog/` directory:

```sql
ATTACH 'ducklake:metadata.ducklake' AS my_ducklake (
  DATA_PATH 'file_path/',
  CATALOG 'my-catalog',
  CREATE_IF_NOT_EXISTS true
);
USE my_ducklake;
CREATE TABLE my_ducklake.my_table(id INTEGER, val VARCHAR);
INSERT INTO my_ducklake.my_table VALUES (1, 'Hello'), (2, 'World');
FROM my_ducklake.my_table;
┌───────┬─────────┐
│  id   │   val   │
│ int32 │ varchar │
├───────┼─────────┤
│     1 │ Hello   │
│     2 │ World   │
└───────┴─────────┘
```
##### Updates
```sql
UPDATE my_ducklake.my_table SET val='DuckLake' WHERE id=2;
FROM my_ducklake.my_table;
┌───────┬──────────┐
│  id   │   val    │
│ int32 │ varchar  │
├───────┼──────────┤
│     1 │ Hello    │
│     2 │ DuckLake │
└───────┴──────────┘
```
##### Time Travel
```sql
FROM my_ducklake.my_table AT (VERSION => 2);
┌───────┬─────────┐
│  id   │   val   │
│ int32 │ varchar │
├───────┼─────────┤
│     1 │ Hello   │
│     2 │ World   │
└───────┴─────────┘
```
##### Schema Evolution
```sql
ALTER TABLE my_ducklake.my_table ADD COLUMN new_column VARCHAR;
FROM my_ducklake.my_table;
┌───────┬──────────┬────────────┐
│  id   │   val    │ new_column │
│ int32 │ varchar  │  varchar   │
├───────┼──────────┼────────────┤
│     1 │ Hello    │ NULL       │
│     2 │ DuckLake │ NULL       │
└───────┴──────────┴────────────┘
```
##### Change Data Feed
```sql
FROM my_ducklake.table_changes('my_table', 2, 2);
┌─────────────┬───────┬─────────────┬───────┬─────────┐
│ snapshot_id │ rowid │ change_type │  id   │   val   │
│    int64    │ int64 │   varchar   │ int32 │ varchar │
├─────────────┼───────┼─────────────┼───────┼─────────┤
│           2 │     0 │ insert      │     1 │ Hello   │
│           2 │     1 │ insert      │     2 │ World   │
└─────────────┴───────┴─────────────┴───────┴─────────┘
```

See the [Usage](https://ducklake.select/docs/stable/duckdb/introduction) guide for more information.

## Building & Loading the Extension

To build, type
```
git submodule init
git submodule update
# to build with multiple cores, use `make GEN=ninja release`
make pull
make
```

To run, run the bundled `duckdb` shell:
```
 ./build/release/duckdb
```
