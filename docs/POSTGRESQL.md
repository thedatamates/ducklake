# Using PostgreSQL as DuckLake Metadata Store

DuckLake can use PostgreSQL instead of DuckDB for storing catalog metadata. This enables shared metadata access across multiple clients and integrates with existing PostgreSQL infrastructure.

## Building with PostgreSQL Support

The `postgres_scanner` extension is required but not built by default.

```bash
ENABLE_POSTGRES_SCANNER=1 make release
```

This builds both the DuckLake extension and the PostgreSQL scanner extension.

## Setting Up PostgreSQL

### 1. Create Database and Schema

```bash
psql -U <username> -d postgres -c "CREATE DATABASE ducklake;"
psql -U <username> -d ducklake -c "CREATE SCHEMA ducklake;"
```

### 2. Load the Schema

```bash
psql -U <username> -d ducklake -c "SET search_path TO ducklake;" \
  -f schema/postgresql.sql
```

### 3. Initialize Metadata

`schema/postgresql.sql` creates the bootstrap snapshot and initializes `ducklake_snapshot_id_seq`.
You only need to set the `encrypted` metadata value:

```bash
psql -U <username> -d ducklake -c "
SET search_path TO ducklake;
INSERT INTO ducklake_metadata (key, value) VALUES ('encrypted', 'false');
"
```

## Using DuckLake with PostgreSQL

### Use the Built DuckDB Binary

Extensions are version-locked. Use the DuckDB built with the project, not a system-installed version:

```bash
./build/release/duckdb -unsigned
```

The `-unsigned` flag allows loading locally-built extensions.

### Load Extensions

```sql
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';
```

### Attach with PostgreSQL Metadata

```sql
ATTACH 'ducklake:postgres:dbname=ducklake user=<username>' AS lake (
  DATA_PATH '/path/to/data',
  METADATA_SCHEMA 'ducklake',
  CATALOG 'mycatalog'
);
```

Connection string format follows libpq:
- `host=localhost` - PostgreSQL host
- `port=5432` - PostgreSQL port
- `dbname=ducklake` - Database name
- `user=username` - Username
- `password=secret` - Password (or use .pgpass)

### Example Session

```sql
LOAD 'ducklake';
LOAD 'postgres_scanner';

ATTACH 'ducklake:postgres:dbname=ducklake user=joshferguson' AS lake (
  DATA_PATH '/tmp/ducklake_data',
  METADATA_SCHEMA 'ducklake',
  CATALOG 'production'
);

USE lake;

CREATE TABLE events (id INT, ts TIMESTAMP, data VARCHAR);
INSERT INTO events VALUES (1, NOW(), 'test');
SELECT * FROM events;
```

## Attach Options

| Option | Description |
|--------|-------------|
| `DATA_PATH` | Directory for Parquet data files |
| `METADATA_SCHEMA` | PostgreSQL schema containing DuckLake tables |
| `CATALOG` | Catalog name within DuckLake |
| `ENCRYPTED` | Enable encryption for data files |
| `DATA_INLINING_ROW_LIMIT` | Max rows to inline in metadata |

## Verifying Setup

Check metadata is being stored in PostgreSQL:

```bash
psql -U <username> -d ducklake -c "
SET search_path TO ducklake;
SELECT table_name, table_id FROM ducklake_table;
"
```

Check data files:

```bash
psql -U <username> -d ducklake -c "
SET search_path TO ducklake;
SELECT path, record_count, file_size_bytes FROM ducklake_data_file;
"
```

## Schema Indexes

The `schema/postgresql.sql` file includes indexes optimized for DuckLake query patterns:

| Index | Purpose |
|-------|---------|
| `idx_catalog_name` | Catalog name lookups |
| `idx_table_schema` | Tables by schema |
| `idx_data_file_table` | Data files by table |
| `idx_data_file_id` | Data file lookups |
| `idx_delete_file_table` | Delete files by table |
| `idx_delete_file_data` | Delete files by data file |

## Troubleshooting

### "No snapshots found"

The schema wasn't initialized. Run the initialization SQL above.

### "Extension postgres_scanner not found"

Rebuild with `ENABLE_POSTGRES_SCANNER=1 make release`.

### "The file was built specifically for DuckDB version X"

This project builds against DuckDB's development branch. The built extension only works with the DuckDB binary built alongside it:

```bash
./build/release/duckdb -unsigned
```

If using a released DuckDB version, install extensions from the repository instead:

```sql
INSTALL ducklake;
INSTALL postgres_scanner;
LOAD ducklake;
LOAD postgres_scanner;
```

### Connection errors

Verify PostgreSQL is running and credentials are correct:

```bash
psql -U <username> -d ducklake -c "SELECT 1;"
```
