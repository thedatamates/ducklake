# DuckLake Fork - Multi-Tenant Catalog Isolation

This is a fork of [DuckLake](https://github.com/duckdb/ducklake) that adds **catalog-based multi-tenant isolation**. Instead of using PostgreSQL schema-level isolation (one schema per tenant), this fork uses a `catalog_id` column to isolate tenants within a shared schema.

## Why This Fork?

DuckLake's standard approach uses `METADATA_SCHEMA` to isolate tenants:
- Each tenant gets a separate PostgreSQL schema
- Works for dozens/hundreds of tenants
- Does NOT scale to millions of tenants

This fork uses `catalog_id` column-based isolation:
- Single shared PostgreSQL schema for all catalogs
- All queries filtered by `WHERE catalog_id = ?`
- Scales to millions of catalogs (workbooks, workspaces, etc.)

## Quick Start

```sql
INSTALL ducklake; LOAD ducklake;

-- Attach with required CATALOG name
ATTACH 'ducklake:my_catalog.ducklake' AS my_lake (
    DATA_PATH '/data/files/',
    CATALOG 'my-catalog'
);

USE my_lake;
CREATE TABLE test(id INTEGER, name VARCHAR);
INSERT INTO test VALUES (1, 'hello');
```

**CATALOG is mandatory.** Every DuckLake attachment must specify a catalog name.

---

# Architecture

## Metadata Storage

```
┌─────────────────────────────────────────────────────────────┐
│                    Metadata Database                         │
│  (DuckDB file, PostgreSQL, or MySQL)                        │
├─────────────────────────────────────────────────────────────┤
│  ducklake_catalog      │ catalog_id, catalog_name           │
│  ducklake_snapshot     │ catalog_id, snapshot_id, ...       │
│  ducklake_schema       │ catalog_id, schema_id, ...         │
│  ducklake_table        │ catalog_id, table_id, ...          │
│  ducklake_column       │ table_id, column_id, ...           │
│  ducklake_data_file    │ table_id, data_file_id, ...        │
│  ...                   │                                     │
└─────────────────────────────────────────────────────────────┘
```

## Data Storage

```
DATA_PATH/
├── catalog_name_a/
│   ├── schema_name/
│   │   ├── table_name/
│   │   │   ├── 00000000-0000-0000-0000-000000000001.parquet
│   │   │   └── 00000000-0000-0000-0000-000000000002.parquet
│   │   └── another_table/
│   │       └── ...
│   └── another_schema/
│       └── ...
├── catalog_name_b/
│   └── ...
└── ...
```

## Design Assumptions

1. **Catalog Name Uniqueness** - Catalog names are unique within a metadata database (enforced by UNIQUE constraint)
2. **Catalog ID Assignment** - First catalog gets `catalog_id = 0`, subsequent catalogs get incrementing IDs (never reused)
3. **Data Path Isolation** - Each catalog's data is stored in `DATA_PATH/catalog_name/`
4. **Metadata Isolation** - All metadata queries filter by `catalog_id`
5. **Snapshot Independence** - Each catalog has its own snapshot sequence (`snapshot_id` unique per catalog, not globally)
6. **File-Based DuckDB Limitation** - Cannot attach same DuckDB file as metadata for multiple catalogs (use PostgreSQL for shared metadata)

## Version Compatibility

| DuckLake Version | Catalog Support | Migration |
|------------------|-----------------|-----------|
| 0.1 - 0.3 | None | MigrateV01-V03 + V04 |
| 0.4-dev1 | VARCHAR catalog_id | MigrateV04 |
| 0.5-dev1 | BIGINT catalog_id | Current |

Migration is automatic when `MIGRATE_IF_REQUIRED = true` (default).

---

# Usage Guide

## ATTACH

```sql
ATTACH 'ducklake:<metadata_path>' AS <alias> (
    CATALOG '<catalog_name>',
    DATA_PATH '<base_path>',
    [other options...]
);
```

| Option | Required | Description |
|--------|----------|-------------|
| `CATALOG` | **Yes** | Catalog name for tenant isolation |
| `DATA_PATH` | No | Base path for parquet files |
| `METADATA_SCHEMA` | No | PostgreSQL schema for metadata tables |
| `SNAPSHOT_VERSION` | No | Attach at specific version (read-only) |
| `SNAPSHOT_TIME` | No | Attach at specific timestamp (read-only) |
| `ENCRYPTED` | No | Enable/disable encryption |
| `DATA_INLINING_ROW_LIMIT` | No | Max rows to inline in catalog |
| `CREATE_IF_NOT_EXISTS` | No | Create new DuckLake if not found |

## CREATE TABLE

```sql
CREATE TABLE [schema_name.]table_name (
    column_name type [constraints],
    ...
);
```

## INSERT

```sql
INSERT INTO table_name [(columns)] VALUES (...) | SELECT ...;
```

**Data Inlining:** If row count ≤ `DATA_INLINING_ROW_LIMIT`, data stored in metadata (no Parquet file).

## SELECT

```sql
SELECT ... FROM [catalog.]schema.table WHERE ...;
```

**Time Travel:**
```sql
SELECT * FROM table AT SNAPSHOT 5;
SELECT * FROM table AT TIMESTAMP '2024-01-01';
```

## UPDATE

```sql
UPDATE table SET column = value WHERE condition;
```

DuckLake uses copy-on-write - updated files are rewritten completely.

## DELETE

```sql
DELETE FROM table WHERE condition;
TRUNCATE TABLE table_name;  -- Faster for full table deletion
```

## ALTER TABLE

```sql
ALTER TABLE table ADD COLUMN name type [DEFAULT value];
ALTER TABLE table DROP COLUMN name;
ALTER TABLE table RENAME COLUMN old TO new;
ALTER TABLE table ALTER COLUMN name SET DATA TYPE new_type;  -- Safe promotions only
```

## DROP TABLE

```sql
DROP TABLE [IF EXISTS] table_name;
```

Files remain until `ducklake_expire_snapshots()` is called.

## Time Travel

**By Version:**
```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', SNAPSHOT_VERSION 5);
SELECT * FROM table AT SNAPSHOT 5;
```

**By Timestamp:**
```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', SNAPSHOT_TIME '2024-01-01 12:00:00');
SELECT * FROM table AT TIMESTAMP '2024-01-01';
```

## Snapshots

```sql
-- View snapshots
SELECT * FROM ducklake_snapshots('catalog_alias');

-- Expire old snapshots
CALL ducklake_expire_snapshots('catalog_alias', older_than := INTERVAL '7 days');
```

## Compaction

```sql
-- Merge small files
CALL ducklake_merge_adjacent_files('catalog_alias', 'schema.table');

-- Rewrite to remove deleted rows
CALL ducklake_rewrite_data_files('catalog_alias', 'schema.table');
```

## Partitioning

```sql
CREATE TABLE events (
    event_date DATE,
    event_type VARCHAR,
    data VARCHAR
) PARTITION BY (year(event_date), month(event_date));
```

**Partition Transforms:** `identity()`, `year()`, `month()`, `day()`, `hour()`

## Encryption

```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', ENCRYPTED true);
```

---

# Error Handling

| Error | Cause | Solution |
|-------|-------|----------|
| "CATALOG is required" | Missing CATALOG option | Add `CATALOG 'name'` to ATTACH |
| "Catalog 'x' does not exist" | Catalog not in database | Use `CREATE_IF_NOT_EXISTS true` |
| "Unique file handle conflict" | Same DuckDB file attached twice | Use PostgreSQL for shared metadata |

---

# Performance Considerations

## PostgreSQL Indexing

```sql
CREATE INDEX idx_schema_catalog ON ducklake_schema(catalog_id);
CREATE INDEX idx_table_catalog ON ducklake_table(catalog_id);
CREATE INDEX idx_snapshot_catalog ON ducklake_snapshot(catalog_id);
```

## File Size Tuning

```sql
SET ducklake_target_file_size = '512MB';
SET ducklake_parquet_row_group_size_bytes = '128MB';
```

---

# References

- Upstream: https://github.com/duckdb/ducklake
- DuckDB Docs: https://duckdb.org/docs/stable/core_extensions/ducklake
- License: MIT
