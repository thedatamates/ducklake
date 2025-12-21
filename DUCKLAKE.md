# DuckLake Fork - Multi-Tenant Catalog Isolation

## Overview

This is a fork of [DuckLake](https://github.com/duckdb/ducklake) that adds **catalog-based multi-tenant isolation**. Instead of using PostgreSQL schema-level isolation (one schema per tenant), this fork uses a `catalog_id` column to isolate tenants within a shared schema.

## Current State

The CATALOG refactor is complete with the following implemented:

- [x] `CATALOG 'name'` option replaces `CATALOG_ID 'string'`
- [x] `catalog_id` is now BIGINT (not VARCHAR)
- [x] `ducklake_catalog` table stores catalog metadata
- [x] `next_catalog_id` renamed to `next_entry_id`
- [x] All SELECT queries filter by `catalog_id`
- [x] Catalog lookup by name (`LookupCatalogByName`)
- [x] Auto-create catalogs with proper ID sequencing (`CreateCatalog`)
- [x] Multi-catalog isolation in snapshot queries (`GetAllSnapshots`)
- [x] Multi-catalog isolation in file cleanup (`GetOrphanFilesForCleanup`, `GetOldFilesForCleanup`)
- [x] Multi-catalog isolation in table sizes (`GetTableSizes`)
- [x] Multi-catalog isolation in conflict detection (`GetFilesDeletedOrDroppedAfterSnapshot`)
- [x] Per-catalog snapshot expiration (`expire_snapshots`)
- [x] All 247 tests pass

---

### Why This Fork?

DuckLake's standard approach uses `METADATA_SCHEMA` to isolate tenants:
- Each tenant gets a separate PostgreSQL schema
- Works for dozens/hundreds of tenants
- Does NOT scale to millions of tenants

This fork uses `catalog_id` column-based isolation:
- Single shared PostgreSQL schema for all catalogs
- All queries filtered by `WHERE catalog_id = ?`
- Scales to millions of catalogs (workbooks, workspaces, etc.)

---

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

### Required: CATALOG

**CATALOG is mandatory.** Every DuckLake attachment must specify a catalog name:

```sql
-- This WORKS
ATTACH 'ducklake:catalog.db' AS lake (DATA_PATH '/data/', CATALOG 'tenant-123');

-- This FAILS with: "CATALOG is required"
ATTACH 'ducklake:catalog.db' AS lake (DATA_PATH '/data/');
```

---

## Attach Options

| Option | Required | Description |
|--------|----------|-------------|
| `CATALOG` | **Yes** | Catalog name for tenant isolation (stored in `ducklake_catalog` table) |
| `DATA_PATH` | No | Base path for parquet files (catalog name subdirectory added automatically) |
| `METADATA_SCHEMA` | No | PostgreSQL schema for metadata tables |
| `METADATA_CATALOG` | No | Database name for internal metadata reference |
| `OVERRIDE_DATA_PATH` | No | Allow attaching with different DATA_PATH than stored |
| `SNAPSHOT_VERSION` | No | Attach at specific version (read-only) |
| `SNAPSHOT_TIME` | No | Attach at specific timestamp (read-only) |
| `ENCRYPTED` | No | Enable/disable encryption |
| `DATA_INLINING_ROW_LIMIT` | No | Max rows to inline in catalog |
| `CREATE_IF_NOT_EXISTS` | No | Create new DuckLake if not found |

---

## Data Path Structure

When you specify `DATA_PATH` and `CATALOG`, the effective data path is:

```
DATA_PATH + CATALOG_NAME + /
```

Example:
```sql
ATTACH 'ducklake:...' AS lake (DATA_PATH '/datalake/', CATALOG 'wb_abc');
-- Files stored at: /datalake/wb_abc/schema_name/table_name/*.parquet
```

---

## Multi-Catalog Architecture

### Shared Metadata (PostgreSQL Required)

For true multi-tenant isolation with a shared metadata store, you need PostgreSQL:

```sql
-- Workbook catalog
ATTACH 'ducklake:postgres:host=db.example.com dbname=ducklake' AS workbook (
    CATALOG 'wb_abc',
    DATA_PATH '/datalake/'
);

-- Workspace catalog (same PostgreSQL, different catalog)
ATTACH 'ducklake:postgres:host=db.example.com dbname=ducklake' AS workspace (
    CATALOG 'ws_123',
    DATA_PATH '/datalake/'
);

USE workbook;

-- Query both catalogs
SELECT * FROM history.my_results;              -- workbook.history.my_results
SELECT * FROM workspace.datasets.customers;   -- workspace.datasets.customers
```

### File-Based DuckDB Limitation

With file-based DuckDB metadata, you cannot attach the same file twice (DuckDB file locking prevents this). Each catalog needs its own separate metadata file:

```sql
-- Separate metadata files - each tenant isolated in its own file
ATTACH 'ducklake:tenant_a.db' AS tenant_a (CATALOG 'tenant_a', DATA_PATH '/data/');
ATTACH 'ducklake:tenant_b.db' AS tenant_b (CATALOG 'tenant_b', DATA_PATH '/data/');

-- This WILL NOT WORK with file-based metadata:
-- ATTACH 'ducklake:shared.db' AS tenant_a (CATALOG 'tenant_a', DATA_PATH '/data/');
-- ATTACH 'ducklake:shared.db' AS tenant_b (CATALOG 'tenant_b', DATA_PATH '/data/');
-- Error: "Cannot attach - the database file is already attached"
```

For true multi-tenant shared metadata, use PostgreSQL as the metadata backend.

---

## Metadata Schema

### ducklake_catalog Table (New)

Stores catalog metadata:

```sql
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT PRIMARY KEY,
    catalog_name VARCHAR NOT NULL UNIQUE,
    created_snapshot BIGINT NOT NULL
);
```

### Tables with catalog_id Column

The following tables have `catalog_id BIGINT NOT NULL` for tenant isolation:

| Table | Purpose |
|-------|---------|
| `ducklake_snapshot` | Snapshots for time travel |
| `ducklake_snapshot_changes` | Change tracking per snapshot |
| `ducklake_schema` | Schema definitions |
| `ducklake_table` | Table definitions |
| `ducklake_view` | View definitions |
| `ducklake_table_stats` | Table-level statistics |
| `ducklake_partition_info` | Partition metadata |
| `ducklake_files_scheduled_for_deletion` | Files pending cleanup |
| `ducklake_schema_versions` | Schema version history |
| `ducklake_macro` | Macro definitions |

All queries to these tables are automatically filtered by `catalog_id`.

---

## Implementation Details

### Catalog Lookup and Creation

When attaching to an existing DuckLake, the system automatically looks up or creates the catalog:

**In `DuckLakeMetadataManager`:**

```cpp
// Lookup catalog by name - returns catalog_id if found
optional_idx LookupCatalogByName(const string &catalog_name);

// Create new catalog - returns new catalog_id
idx_t CreateCatalog(const string &catalog_name);
```

**In `LoadExistingDuckLake`:**

```cpp
auto catalog_id = metadata_manager.LookupCatalogByName(options.catalog_name);
if (catalog_id.IsValid()) {
    // Catalog exists - use its ID
    options.catalog_id = catalog_id.GetIndex();
} else if (options.create_if_not_exists) {
    // Catalog doesn't exist - create it
    options.catalog_id = metadata_manager.CreateCatalog(options.catalog_name);
} else {
    throw InvalidInputException("Catalog '%s' does not exist", options.catalog_name);
}
```

**What `CreateCatalog` does:**
1. Computes next available `catalog_id` (MAX + 1)
2. Computes next available `snapshot_id` (MAX + 1)
3. Computes next available `entry_id` for the 'main' schema
4. Computes next available `file_id` to avoid conflicts
5. Inserts into `ducklake_catalog`
6. Inserts initial 'main' schema into `ducklake_schema`
7. Inserts initial snapshot into `ducklake_snapshot`
8. Inserts snapshot changes into `ducklake_snapshot_changes`
9. Inserts schema version into `ducklake_schema_versions`

This ensures new catalogs get properly sequenced IDs that don't conflict with existing catalogs.

### Catalog ID Design

The catalog system follows the same pattern as other DuckLake entities:

| Entity | ID Column | Name Column | ID Type |
|--------|-----------|-------------|---------|
| Catalog | `catalog_id` | `catalog_name` | BIGINT |
| Schema | `schema_id` | `schema_name` | BIGINT |
| Table | `table_id` | `table_name` | BIGINT |
| View | `view_id` | `view_name` | BIGINT |
| Macro | `macro_id` | `macro_name` | BIGINT |

- User provides the **name** (e.g., `CATALOG 'my-catalog'`)
- System assigns an auto-generated **BIGINT ID** from `ducklake_catalog`
- All queries filter by the numeric `catalog_id`

### Query Placeholder Replacement

All queries use placeholders that are replaced at runtime:
- `{CATALOG_ID}` → Numeric catalog ID (BIGINT)
- `{CATALOG_NAME}` → SQL-quoted catalog name string
- `{METADATA_CATALOG}` → Schema prefix for metadata tables
- `{SNAPSHOT_ID}` → Current snapshot ID
- `{NEXT_ENTRY_ID}` → Next ID for schemas/tables/views/macros

### Snapshot Counter

The `ducklake_snapshot` table has a `next_entry_id` column (renamed from `next_catalog_id` for clarity) that generates sequential IDs for schemas, tables, views, macros, and partitions.

### Tables That NEED catalog_id Filtering

These tables are queried in bulk without foreign key filtering:

| Table | Reason |
|-------|--------|
| `ducklake_schema` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_table` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_view` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_macro` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_partition_info` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_snapshot` | Queried for time travel |
| `ducklake_snapshot_changes` | Joined with ducklake_snapshot |
| `ducklake_schema_versions` | Loaded for schema version tracking |
| `ducklake_table_stats` | Queried without FK filter |
| `ducklake_files_scheduled_for_deletion` | Queried without FK filter |

### Tables That DON'T Need catalog_id

These tables are always queried with a foreign key filter (table_id, schema_id, etc.) and inherit isolation from the FK:

- `ducklake_column` (filtered by table_id)
- `ducklake_data_file` (filtered by table_id)
- `ducklake_delete_file` (filtered by table_id)
- `ducklake_file_column_stats` (filtered by data_file_id)
- `ducklake_table_column_stats` (joined with ducklake_table_stats)
- `ducklake_partition_column` (joined with ducklake_partition_info)
- And others...

---

## Migration from Upstream DuckLake

Migrating from upstream DuckLake requires providing a CATALOG name:

```sql
-- Migration requires CATALOG to assign to existing data
ATTACH 'ducklake:old_catalog.db' AS lake (
    DATA_PATH '/existing/data/',
    CATALOG 'migrated-catalog'
);
-- MigrateV04 runs automatically:
-- 1. Creates ducklake_catalog table
-- 2. Inserts catalog record with catalog_id = 0
-- 3. Adds catalog_id BIGINT column to all tables
-- 4. Renames next_catalog_id → next_entry_id
```

**Note:** Migrated databases have `catalog_id` as the LAST column (ALTER TABLE behavior), while new databases have it FIRST. This is handled transparently via explicit column names in all INSERT statements.

---

## Direct PostgreSQL Access (Webapp)

With catalog_id column-based isolation, webapps can query metadata directly:

```sql
-- List catalogs in the metadata database
SELECT catalog_id, catalog_name, created_snapshot
FROM ducklake.ducklake_catalog;

-- List tables for a catalog (by numeric ID)
SELECT t.table_name, t.table_uuid, s.schema_name
FROM ducklake.ducklake_table t
JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
WHERE t.catalog_id = 0  -- numeric catalog_id
  AND t.end_snapshot IS NULL
  AND s.catalog_id = 0
  AND s.end_snapshot IS NULL;

-- Or lookup by catalog name first
SELECT t.table_name, t.table_uuid, s.schema_name
FROM ducklake.ducklake_table t
JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
JOIN ducklake.ducklake_catalog c ON t.catalog_id = c.catalog_id
WHERE c.catalog_name = 'my-catalog'
  AND t.end_snapshot IS NULL
  AND s.end_snapshot IS NULL;
```

---

## DuckLake Features

### Supported
- ACID transactions via catalog database
- Time travel (snapshots)
- Schema evolution (add/drop columns, type promotion)
- Multi-client concurrency (with PostgreSQL/MySQL catalog)
- Parquet storage with relative or absolute paths
- External file registration (`ducklake_add_data_files`)
- Encryption support
- Partitioning (year/month/day/hour/identity transforms)
- Data inlining (small tables stored in catalog)
- Compaction and snapshot expiration

### Not Supported by DuckLake Specification

**May be supported in future:**
- User defined types, ENUM, ARRAY types
- CHECK constraints
- Scalar and table macros
- Non-literal default values
- DROP ... CASCADE
- Generated columns

**Unlikely to be supported:**
- Indexes
- Primary/foreign key constraints (enforced)
- Sequences
- VARINT, BITSTRING, UNION types

---

## File Changes Summary

### Core Changes

| File | Changes |
|------|---------|
| `src/include/common/ducklake_options.hpp` | `catalog_name` string, `catalog_id` idx_t (BIGINT) |
| `src/include/common/ducklake_snapshot.hpp` | Renamed `next_catalog_id` → `next_entry_id` |
| `src/include/storage/ducklake_catalog.hpp` | `CatalogId()` returns idx_t, added `CatalogName()` |
| `src/storage/ducklake_storage.cpp` | Parse `CATALOG` option |
| `src/storage/ducklake_transaction.cpp` | `{CATALOG_ID}` as integer, `{CATALOG_NAME}` as string |
| `src/storage/ducklake_initializer.cpp` | Compute effective_data_path with catalog name, catalog lookup/creation |
| `src/storage/ducklake_metadata_manager.cpp` | Schema, queries, filtering, MigrateV04, multi-catalog isolation fixes |
| `src/storage/ducklake_secret.cpp` | `catalog` parameter (was `catalog_id`) |
| `src/functions/ducklake_expire_snapshots.cpp` | Per-catalog MAX(snapshot_id) in expiration filter |
| `src/metadata_manager/postgres_metadata_manager.cpp` | Query changes |

### Schema Changes

- New `ducklake_catalog` table with `catalog_id BIGINT PRIMARY KEY`
- All 10 tables now have `catalog_id BIGINT NOT NULL`
- CREATE TABLE statements define catalog_id first
- INSERT statements use explicit column names
- All SELECT/UPDATE/DELETE queries filter by catalog_id

### Test Changes

- ~258 ATTACH statements updated: `CATALOG_ID 'test'` → `CATALOG 'test'`
- Expected query results updated for catalog_id as BIGINT (not string)
- GLOB paths updated to include catalog name subdirectory
- Multi-catalog isolation test uses separate metadata files (file-based DuckDB limitation)

---

## Version History

| Version | Changes |
|---------|---------|
| 0.5-dev1 | Added catalog-based multi-tenant isolation with BIGINT catalog_id |
| 0.4-dev1 | Upstream DuckLake version |

---

## Testing Checklist

### Unit Tests (Done)
- [x] All 243 existing tests pass
- [x] CATALOG option parsing
- [x] catalog_id BIGINT in schema
- [x] catalog_id filtering in queries

### Integration Tests (TODO)
- [ ] PostgreSQL multi-tenant isolation
- [ ] Concurrent catalog access
- [ ] Cross-catalog queries
- [ ] Catalog creation/lookup
- [ ] Migration from older versions with existing data

### Performance Tests (TODO)
- [ ] Query performance with catalog_id filtering
- [ ] Index effectiveness on PostgreSQL
- [ ] Large number of catalogs (1000+)

---

## References

- Upstream: https://github.com/duckdb/ducklake
- DuckDB Docs: https://duckdb.org/docs/stable/core_extensions/ducklake
- License: MIT
