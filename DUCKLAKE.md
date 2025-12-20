# DuckLake Exploration

## What is DuckLake?

DuckLake is an open lakehouse format built on SQL + Parquet, released May 2025. It uses a SQL database for catalog metadata and Parquet files for data storage.

- GitHub: https://github.com/duckdb/ducklake
- Docs: https://duckdb.org/docs/stable/core_extensions/ducklake
- License: MIT
- Version: 0.4-dev1 (current)

## Key Features

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

## Catalog Database Options

| Option | Use Case |
|--------|----------|
| DuckDB | Single client, local data warehousing |
| SQLite | Multiple local clients |
| PostgreSQL/MySQL | Multi-user lakehouse, remote clients |

## Attach Options

| Option | Description |
|--------|-------------|
| `DATA_PATH` | Where parquet files are stored |
| `METADATA_SCHEMA` | Schema within catalog DB for metadata tables |
| `METADATA_CATALOG` | Database name for internal metadata reference |
| `OVERRIDE_DATA_PATH` | Allow attaching with different DATA_PATH than stored |
| `SNAPSHOT_VERSION` | Attach at specific version (read-only) |
| `SNAPSHOT_TIME` | Attach at specific timestamp (read-only) |
| `ENCRYPTED` | Enable/disable encryption |
| `DATA_INLINING_ROW_LIMIT` | Max rows to inline in catalog |
| `CREATE_IF_NOT_EXISTS` | Create new DuckLake if not found |

## Basic Usage

```sql
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:catalog.ducklake' AS my_lake (DATA_PATH 'data_files/');
USE my_lake;

CREATE SCHEMA history;
CREATE TABLE history.my_table AS SELECT 1 as id, 'hello' as data;
```

## Catalog Schema

DuckLake stores metadata in these tables:

```sql
ducklake_metadata          -- key/value config
ducklake_snapshot          -- snapshots for time travel
ducklake_snapshot_changes  -- change tracking
ducklake_schema            -- schemas (with begin/end snapshots)
ducklake_table             -- tables (with begin/end snapshots)
ducklake_view              -- views
ducklake_column            -- columns
ducklake_data_file         -- parquet file locations
ducklake_delete_file       -- deletion tracking
ducklake_*_stats           -- statistics at table/column/file level
ducklake_partition_*       -- partitioning info
```

Key fields in `ducklake_data_file`:
- `path` - file path
- `path_is_relative` - whether path is relative to DATA_PATH

## Experiment: Multi-Tenant Isolation

### Question

Can we use a shared catalog with different DATA_PATHs per workbook to achieve tenant isolation?

### Test Setup

```bash
mkdir -p /tmp/ducklake_test/workbook_a/history /tmp/ducklake_test/workbook_b/history
```

### Test 1: Separate Catalogs (Works)

```sql
-- Workbook A
ATTACH 'ducklake:catalog_a.ducklake' AS workbook_a (DATA_PATH 'workbook_a/');
USE workbook_a;
CREATE SCHEMA history;
CREATE TABLE history.my_secret_data AS SELECT 1 as id, 'secret from A' as data;

-- Workbook B (separate session)
ATTACH 'ducklake:catalog_b.ducklake' AS workbook_b (DATA_PATH 'workbook_b/');
USE workbook_b;
CREATE SCHEMA history;
CREATE TABLE history.my_other_data AS SELECT 2 as id, 'data from B' as data;
```

Result: Complete isolation. Each workbook has its own catalog and data files.

### Test 2: Shared Catalog, Different DATA_PATHs (Fails with DuckDB)

```sql
ATTACH 'ducklake:shared_catalog.ducklake' AS wb_a (DATA_PATH 'workbook_a/');
ATTACH 'ducklake:shared_catalog.ducklake' AS wb_b (DATA_PATH 'workbook_b/');
```

Result: **Error** - "Unique file handle conflict: Database wb_a is already attached"

DuckDB won't let you attach the same catalog file twice.

### Conclusion

With DuckDB as catalog: **Must use separate catalogs per workbook** (isolation by design).

With PostgreSQL as catalog: Might allow multiple attachments with different DATA_PATHs (untested).

## Proposed Architecture for Our System

### Directory Structure

```
/datalake/
└── workspaces/
    └── ws_abc123/
        ├── workbooks/
        │   ├── wb_def456/
        │   │   ├── catalog.ducklake     ← workbook's DuckLake catalog
        │   │   ├── history/
        │   │   │   └── *.parquet
        │   │   └── main/
        │   │       └── *.parquet
        │   └── wb_ghi789/
        │       ├── catalog.ducklake
        │       ├── history/
        │       └── main/
        └── shared/                       ← future: workspace-level shared data
            ├── catalog.ducklake
            └── tables/
                └── *.parquet
```

### Session Setup

When agent starts a session for workbook `wb_def456`:

```sql
INSTALL ducklake;
LOAD ducklake;

-- Attach workbook's catalog
ATTACH 'ducklake:/datalake/workspaces/ws_abc123/workbooks/wb_def456/catalog.ducklake'
    AS workbook (DATA_PATH '/datalake/workspaces/ws_abc123/workbooks/wb_def456/');
USE workbook;

-- Future: attach workspace shared (read-only)
-- ATTACH 'ducklake:/datalake/workspaces/ws_abc123/shared/catalog.ducklake'
--     AS shared (DATA_PATH '/datalake/workspaces/ws_abc123/shared/');
```

### Agent SQL

Agent writes simple schema.table references:

```sql
SELECT * FROM history.my_table
SELECT * FROM main.customers
-- Future: SELECT * FROM shared.materialized_data
```

### Isolation Model

- Each workbook = separate DuckLake catalog
- Agent can only see its own workbook's schemas/tables
- No cross-workbook access possible (different catalogs)
- Workspace shared = separate catalog, attached read-only

## Open Questions

1. **PostgreSQL catalog sharing**: Can multiple DuckDB instances attach to the same PostgreSQL catalog with different DATA_PATHs? Would need to test.

2. **Catalog file per workbook**: Is one .ducklake file per workbook too much overhead? These are small files, but could be millions of them.

3. **Webapp reads**: Can webapp query catalog.ducklake directly (without DuckDB) to list tables? Or use DuckDB in read-only mode?

4. **Snapshot management**: DuckLake creates snapshots on every change. Do we need to manage/clean these up?

5. **Schema structure**: Should we use `history` and `main` schemas, or something else?

## Benefits of DuckLake

- No custom catalog implementation
- ACID transactions
- Time travel for free
- Statistics tracking
- Schema evolution
- Well-defined format

## Risks

- v0.4-dev1, still young
- Single-catalog-per-workbook might not scale to PostgreSQL well
- Unknown behavior with very large number of catalogs

## Unsupported Features

### Not Supported by DuckLake Specification

**Likely to be supported in the future:**
- User defined types
- Fixed-size arrays (`ARRAY` type)
- `ENUM` type
- Variant types
- `CHECK` constraints (not Primary/Foreign Key constraints)
- Scalar and table macros (workaround: create in catalog DB directly)
- Non-literal default values (e.g., `DEFAULT now()` not allowed, `DEFAULT '2025-08-08'` is fine)
- Dropping dependencies with `DROP ... CASCADE`
- Generated columns

**Unlikely to ever be supported:**
- Indexes
- Primary key / enforced unique constraints / foreign key constraints (too expensive to enforce in data lake setups)
- Upserting (only via `MERGE INTO` syntax since no primary keys)
- Sequences
- `VARINT` type
- `BITSTRING` type
- `UNION` type

### Not Supported by ducklake DuckDB Extension (yet)

- Data inlining limited to DuckDB catalogs only
- MySQL catalogs not fully supported
- Updates targeting the same row multiple times

### Our Current Usage

Our datasets are **immutable/append-only**. We don't use UPDATE or DELETE operations - only:
- `CREATE TABLE ... AS SELECT ...`
- `INSERT INTO ... SELECT ...`
- `SELECT ...`
- `DROP TABLE`

This means the update/delete limitations don't affect us currently. However, the fork implements complete DuckLake functionality including all update/delete code paths for future flexibility.

---

## Codebase Exploration (from cloned repo)

### Repository Structure

```
ducklake/
├── src/
│   ├── ducklake_extension.cpp      # Extension entry point
│   ├── common/                     # Shared utilities
│   ├── functions/                  # Table functions (add_files, snapshots, etc.)
│   ├── metadata_manager/           # PostgreSQL-specific metadata handling
│   └── storage/                    # Core catalog, transaction, insert logic
├── test/sql/                       # Extensive SQL test cases
└── docs/                           # Documentation
```

### Key Components

**ducklake_extension.cpp** - Registers:
- `ducklake_snapshots()` - List all snapshots
- `ducklake_table_info()` - Table metadata
- `ducklake_table_insertions()` / `ducklake_table_deletions()` - Change tracking
- `ducklake_merge_adjacent_files()` / `ducklake_rewrite_data_files()` - Compaction
- `ducklake_cleanup_old_files()` / `ducklake_cleanup_orphaned_files()` - Cleanup
- `ducklake_expire_snapshots()` - Remove old snapshots
- `ducklake_flush_inlined_data()` - Move inline data to parquet
- `ducklake_set_option()` / `ducklake_options()` - Configuration
- `ducklake_table_changes()` - CDC-style change feed
- `ducklake_list_files()` - List data files for a table
- `ducklake_add_data_files()` - Register external parquet files

### PostgreSQL Multi-Tenant Architecture

From `postgres_metadata_manager.cpp`, DuckLake uses these placeholders in queries:
- `{METADATA_CATALOG}` - Schema identifier for DuckLake tables
- `{DATA_PATH}` - Base path for parquet files

**Key insight**: The `METADATA_SCHEMA` option controls which PostgreSQL schema contains the DuckLake metadata tables (`ducklake_snapshot`, `ducklake_table`, etc.).

This enables a multi-tenant pattern:
```sql
-- Workbook A connects to PostgreSQL with METADATA_SCHEMA='wb_a'
ATTACH 'ducklake:postgres:dbname=catalog' AS wb_a (
    DATA_PATH 's3://bucket/workbooks/wb_a/',
    METADATA_SCHEMA 'wb_a'
);

-- Workbook B connects with METADATA_SCHEMA='wb_b'
ATTACH 'ducklake:postgres:dbname=catalog' AS wb_b (
    DATA_PATH 's3://bucket/workbooks/wb_b/',
    METADATA_SCHEMA 'wb_b'
);
```

Each workbook gets:
- Its own PostgreSQL schema with complete DuckLake metadata tables
- Its own DATA_PATH for parquet storage
- Complete isolation at the catalog level

### External File Registration

From `test/sql/add_files/add_files.test`:

```sql
-- Write parquet externally
COPY (SELECT 200 col1, 'world' col2) TO 'path/to/my_file.parquet';

-- Register with DuckLake
CALL ducklake_add_data_files('catalog', 'table_name', 'path/to/my_file.parquet');
```

Options:
- `allow_missing` - Don't error if files don't exist
- `ignore_extra_columns` - Ignore columns not in table schema
- `hive_partitioning` - Auto-detect hive partition structure

This is useful for importing existing parquet files without copying data.

### Data Path Handling

From `ducklake_storage.cpp` and `ducklake_initializer.cpp`:

1. DATA_PATH is stored in catalog metadata (`ducklake_metadata` table)
2. On attach, if DATA_PATH differs from stored, error is thrown
3. `OVERRIDE_DATA_PATH TRUE` bypasses this check (files won't be found)
4. Relative paths in `ducklake_data_file` are resolved against DATA_PATH

### Secrets Support

From `ducklake_secret.cpp`:

```sql
CREATE SECRET my_ducklake (
    TYPE ducklake,
    metadata_path 'postgres:dbname=catalog',
    data_path 's3://bucket/data/',
    metadata_schema 'my_schema'
);

-- Then attach by name
ATTACH 'ducklake:my_ducklake' AS lake;
```

Useful for:
- Pre-configuring connection parameters
- Hiding credentials from SQL
- Simplifying workbook setup

### Compaction Functions

From `ducklake_compaction_functions.cpp`:

- `ducklake_merge_adjacent_files(catalog, table)` - Combine small files
- `ducklake_rewrite_data_files(catalog, table)` - Full rewrite
- `ducklake_expire_snapshots(catalog, older_than)` - Remove old snapshots
- `ducklake_cleanup_old_files(catalog)` - Delete orphaned parquet files

These would need to be scheduled as maintenance tasks.

---

## Architecture Options for Our System

### Option A: DuckDB Catalog per Workbook (Current Plan)

```
/datalake/workspaces/{ws}/workbooks/{wb}/
├── catalog.ducklake    ← DuckDB file
├── history/
│   └── *.parquet
└── main/
    └── *.parquet
```

**Pros:**
- Complete isolation by design
- No shared infrastructure
- Simple to reason about
- Works offline

**Cons:**
- Millions of small .ducklake files on EFS
- No shared catalog for webapp queries
- Each agent session needs exclusive file access

### Option B: PostgreSQL Catalog + Schema per Workbook

```
PostgreSQL: catalog_db
├── wb_abc123 (schema)
│   ├── ducklake_metadata
│   ├── ducklake_snapshot
│   ├── ducklake_table
│   └── ...
├── wb_def456 (schema)
│   └── ...
└── wb_ghi789 (schema)
    └── ...

EFS/S3:
/datalake/workspaces/{ws}/workbooks/{wb}/
├── history/
│   └── *.parquet
└── main/
    └── *.parquet
```

**Pros:**
- Single PostgreSQL database for all workbooks
- Webapp can query metadata directly via SQL
- Multi-client concurrency built-in
- No file locking issues

**Cons:**
- Requires PostgreSQL infrastructure
- Schema creation/deletion overhead
- Potential for PostgreSQL to become bottleneck

### Option C: PostgreSQL Catalog + Schema per Workspace

```
PostgreSQL: catalog_db
├── ws_abc123 (schema)
│   ├── ducklake_metadata
│   ├── ducklake_snapshot (shared for workspace)
│   ├── ducklake_table (shared for workspace)
│   └── ...
└── ws_def456 (schema)
    └── ...
```

All workbooks in a workspace share the same DuckLake catalog. Workbook isolation via:
- Table naming conventions (`wb_xxx_tablename`)
- Or additional column in metadata tables (requires custom)

**Pros:**
- Fewer PostgreSQL schemas
- Potential for workspace-level sharing

**Cons:**
- Requires custom isolation logic
- May need DuckLake modifications

---

## The Problem: Scale

**Constraint 1**: Webapp cannot access DuckDB files directly. This rules out DuckDB catalog files.

**Constraint 2**: We'll have millions of workbooks. PostgreSQL cannot handle millions of schemas.

**Solution**: Fork DuckLake to add `workbook_id` column-level isolation instead of schema-level isolation.

---

## Forked DuckLake: workbook_id Multi-Tenancy

### Current DuckLake Architecture

DuckLake uses `METADATA_SCHEMA` to isolate tenants:
- Each tenant gets a separate PostgreSQL schema
- All queries use `{METADATA_CATALOG}` placeholder replaced with `schema_name.`
- Works for dozens/hundreds of tenants, not millions

### Proposed Architecture

Add `catalog_id` column to all metadata tables:
- Single shared PostgreSQL schema for all catalogs
- All queries filtered by `WHERE catalog_id = ?`
- Scales to millions of catalogs (workbooks, workspaces, etc.)

### Usage After Fork

```sql
-- Before (schema-based isolation)
ATTACH 'ducklake:postgres:...' AS workbook (
    DATA_PATH '/data/wb_abc123/',
    METADATA_SCHEMA 'wb_abc123'
);

-- After (catalog_id-based isolation)
ATTACH 'ducklake:postgres:...' AS workbook (
    DATA_PATH '/data/wb_abc/',
    CATALOG_ID 'wb_abc'
);

-- Can attach multiple catalogs in same session
ATTACH 'ducklake:postgres:...' AS workspace (
    DATA_PATH '/data/ws_123/',
    CATALOG_ID 'ws_123'
);

-- Agent queries both
SELECT * FROM workbook.history.my_results;    -- workbook data
SELECT * FROM workspace.datasets.customers;   -- workspace data
```

---

## Complete File-by-File Modification Plan

### Test Suite Overview

**253 test files** using DuckDB's sqllogictest format. All existing tests should continue to pass (catalog_id defaults to empty/null for backward compatibility).

New tests needed:
- Multi-catalog isolation tests (workbook + workspace)
- Cross-catalog query prevention
- Catalog creation/deletion

---

## Storage Path Integration

The `catalog_id` must be incorporated into the storage path so:
- User specifies: `DATA_PATH '/datalake/', CATALOG_ID 'wb_abc'`
- Effective path becomes: `/datalake/wb_abc/`
- All files for that catalog live under `/datalake/wb_abc/schema/table/*.parquet`

### File: `src/include/common/ducklake_options.hpp`

**Purpose**: Defines the `DuckLakeOptions` struct that holds all attach options.

**Changes**:
```cpp
struct DuckLakeOptions {
    string metadata_database;
    string metadata_path;
    string metadata_schema;
    string data_path;              // Base data path (e.g., "/datalake/")
    string catalog_id;             // NEW: Catalog identifier (e.g., "wb_abc")
    string effective_data_path;    // NEW: Computed = data_path + catalog_id + "/"
    bool override_data_path = false;
    // ... rest unchanged
};
```

---

### File: `src/storage/ducklake_storage.cpp`

**Purpose**: Parses attach options and creates the catalog.

**Function**: `HandleDuckLakeOption` (line 10-59)

**Changes**: Add handling for `catalog_id` option:
```cpp
} else if (lcase == "catalog_id") {
    options.catalog_id = value.ToString();
}
```

**Validation**: Should require `catalog_id` when using PostgreSQL backend.

---

### File: `src/storage/ducklake_secret.cpp`

**Purpose**: Handles DuckLake secrets for pre-configured connections.

**Function**: `GetFunction` (line 40-49)

**Changes**: Add catalog_id to named parameters:
```cpp
function.named_parameters["catalog_id"] = LogicalType::VARCHAR;
```

---

### File: `src/include/storage/ducklake_catalog.hpp`

**Purpose**: Declares the DuckLakeCatalog class.

**Changes**:
```cpp
const string &CatalogId() const { return options.catalog_id; }

// Modify DataPath() to return effective path (includes catalog_id)
const string &DataPath() const {
    return options.effective_data_path;  // Changed from options.data_path
}

const string &BaseDataPath() const {
    return options.data_path;  // Original base path without catalog_id
}
```

---

### File: `src/storage/ducklake_transaction.cpp`

**Purpose**: Handles query execution with placeholder replacement.

**Function**: `Query(string query)` (line 1772-1790)

**Changes**: Add catalog_id placeholder replacement:
```cpp
auto catalog_id = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.CatalogId());
query = StringUtil::Replace(query, "{CATALOG_ID}", catalog_id);
```

---

### File: `src/metadata_manager/postgres_metadata_manager.cpp`

**Purpose**: PostgreSQL-specific query execution.

**Function**: `ExecuteQuery` (line 25-56)

**Changes**: Add catalog_id placeholder replacement:
```cpp
auto catalog_id = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.CatalogId());
query = StringUtil::Replace(query, "{CATALOG_ID}", catalog_id);
```

---

### File: `src/storage/ducklake_metadata_manager.cpp` (3,413 lines)

**Purpose**: THE CORE FILE. All metadata queries live here.

This is the largest change. Every table creation, every SELECT, every INSERT needs modification.

#### Metadata Storage (line 92)

**Current**:
```cpp
INSERT INTO {METADATA_CATALOG}.ducklake_metadata (key, value)
VALUES ('version', '0.4-dev1'), ('created_by', 'DuckDB %s'), ('data_path', %s), ('encrypted', '%s');
```

**New**: Store base path and catalog_id separately:
```cpp
INSERT INTO {METADATA_CATALOG}.ducklake_metadata (key, value, catalog_id)
VALUES ('version', '0.5', {CATALOG_ID}),
       ('created_by', 'DuckDB %s', {CATALOG_ID}),
       ('base_data_path', %s, {CATALOG_ID}),
       ('catalog_id', {CATALOG_ID}, {CATALOG_ID}),
       ('encrypted', '%s', {CATALOG_ID});
```

#### Schema Creation (lines 64-93)

**22 tables need `catalog_id` column added**:

| Table | Current Primary Key | New Column |
|-------|---------------------|------------|
| `ducklake_metadata` | (key, value) | `catalog_id VARCHAR` |
| `ducklake_snapshot` | snapshot_id | `catalog_id VARCHAR` |
| `ducklake_snapshot_changes` | snapshot_id | `catalog_id VARCHAR` |
| `ducklake_schema` | schema_id | `catalog_id VARCHAR` |
| `ducklake_table` | table_id | `catalog_id VARCHAR` |
| `ducklake_view` | view_id | `catalog_id VARCHAR` |
| `ducklake_tag` | (object_id, key) | `catalog_id VARCHAR` |
| `ducklake_column_tag` | (table_id, column_id, key) | `catalog_id VARCHAR` |
| `ducklake_data_file` | data_file_id | `catalog_id VARCHAR` |
| `ducklake_file_column_stats` | (data_file_id, column_id) | `catalog_id VARCHAR` |
| `ducklake_delete_file` | delete_file_id | `catalog_id VARCHAR` |
| `ducklake_column` | column_id | `catalog_id VARCHAR` |
| `ducklake_table_stats` | table_id | `catalog_id VARCHAR` |
| `ducklake_table_column_stats` | (table_id, column_id) | `catalog_id VARCHAR` |
| `ducklake_partition_info` | partition_id | `catalog_id VARCHAR` |
| `ducklake_partition_column` | (partition_id, column_id) | `catalog_id VARCHAR` |
| `ducklake_file_partition_value` | (data_file_id, partition_key_index) | `catalog_id VARCHAR` |
| `ducklake_files_scheduled_for_deletion` | data_file_id | `catalog_id VARCHAR` |
| `ducklake_inlined_data_tables` | (table_id, table_name) | `catalog_id VARCHAR` |
| `ducklake_column_mapping` | mapping_id | `catalog_id VARCHAR` |
| `ducklake_name_mapping` | (mapping_id, column_id) | `catalog_id VARCHAR` |
| `ducklake_schema_versions` | (begin_snapshot, schema_version) | `catalog_id VARCHAR` |
| `ducklake_macro` | macro_id | `catalog_id VARCHAR` |
| `ducklake_macro_impl` | (macro_id, impl_id) | `catalog_id VARCHAR` |
| `ducklake_macro_parameters` | (macro_id, impl_id, column_id) | `catalog_id VARCHAR` |

**Example change** (line 65):
```sql
-- Before
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    next_catalog_id BIGINT,
    next_file_id BIGINT
);

-- After
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(
    catalog_id VARCHAR NOT NULL,
    snapshot_id BIGINT,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    next_catalog_id BIGINT,
    next_file_id BIGINT,
    PRIMARY KEY (catalog_id, snapshot_id)
);
```

#### Functions Requiring WHERE Clause Changes

Every function that queries metadata tables needs `WHERE catalog_id = {CATALOG_ID}` added:

| Function | Line | Purpose | Tables Queried |
|----------|------|---------|----------------|
| `LoadDuckLake` | 175-180 | Load metadata | `ducklake_metadata` |
| `GetCatalogForSnapshot` | 286-491 | Get catalog info | `ducklake_inlined_data_tables`, `ducklake_table`, `ducklake_schema`, `ducklake_tag`, `ducklake_column`, `ducklake_column_tag`, `ducklake_view`, `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters`, `ducklake_partition_info`, `ducklake_partition_column` |
| `GetGlobalTableStats` | 614-620 | Get table stats | `ducklake_table_stats`, `ducklake_table_column_stats` |
| `GetFilesForTable` | 1097-1140 | Get data files | `ducklake_data_file`, `ducklake_delete_file`, `ducklake_file_column_stats` |
| `GetTableInsertions` | 1166-1175 | Get inserted files | `ducklake_data_file` |
| `GetTableDeletions` | 1220-1260 | Get deleted files | `ducklake_delete_file`, `ducklake_data_file` |
| `GetExtendedFilesForTable` | 1295-1340 | Get extended file info | `ducklake_data_file`, `ducklake_delete_file` |
| `GetFilesForCompaction` | 1365-1390 | Get compaction files | `ducklake_data_file`, `ducklake_delete_file`, `ducklake_schema_versions`, `ducklake_file_partition_value` |
| `GetOldFilesForCleanup` | 2480-2495 | Get old files | `ducklake_delete_file`, `ducklake_data_file` |
| `GetOrphanFilesForCleanup` | 2880-2895 | Get orphan files | `ducklake_data_file`, `ducklake_delete_file`, `ducklake_table`, `ducklake_schema` |
| `GetFilesForCleanup` | 2900-2910 | Get files for cleanup | `ducklake_files_scheduled_for_deletion` |
| `GetSnapshot` | 2520-2560 | Get snapshot info | `ducklake_snapshot` |
| `GetAllSnapshots` | 2810-2820 | List all snapshots | `ducklake_snapshot`, `ducklake_snapshot_changes` |
| `GetTableSizes` | 3325-3340 | Get table sizes | `ducklake_table`, `ducklake_data_file`, `ducklake_delete_file` |
| `GetColumnMappings` | 2320-2330 | Get column mappings | `ducklake_column_mapping`, `ducklake_name_mapping` |
| `GetNextColumnId` | 2950-2955 | Get next column ID | `ducklake_column` |
| `ReadInlinedData` | 1975-1995 | Read inlined data | Inlined data tables |
| `ReadInlinedDataInsertions` | 1990-2010 | Read inlined inserts | Inlined data tables |
| `ReadInlinedDataDeletions` | 2005-2025 | Read inlined deletes | Inlined data tables |
| `GetPathForSchema` | 2020-2030 | Get schema path | `ducklake_schema` |
| `GetPathForTable` | 2040-2080 | Get table path | `ducklake_schema`, `ducklake_table` |
| `GetConfigOption` | 3380-3390 | Get config option | `ducklake_metadata` |

#### Functions Requiring INSERT Changes

Every function that inserts data needs `catalog_id` value added:

| Function | Line | Purpose | Tables Written |
|----------|------|---------|----------------|
| `InitializeDuckLake` | 56-93 | Create new lake | All metadata tables |
| `WriteNewSchemas` | 1528-1535 | Insert schemas | `ducklake_schema` |
| `WriteNewTables` | 1660-1720 | Insert tables | `ducklake_table`, `ducklake_column`, `ducklake_inlined_data_tables` |
| `WriteNewViews` | 1800-1810 | Insert views | `ducklake_view` |
| `WriteNewMacros` | 1725-1750 | Insert macros | `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters` |
| `WriteNewPartitionKeys` | 2645-2665 | Insert partitions | `ducklake_partition_info`, `ducklake_partition_column` |
| `WriteDroppedColumns` | 1770-1795 | Update columns | `ducklake_column` |
| `WriteNewColumns` | 1785-1795 | Insert columns | `ducklake_column` |
| `WriteNewTags` | 2680-2710 | Insert tags | `ducklake_tag` |
| `WriteNewColumnTags` | 2725-2750 | Insert column tags | `ducklake_column_tag` |
| `WriteNewDataFiles` | 2260-2280 | Insert data files | `ducklake_data_file`, `ducklake_file_column_stats`, `ducklake_file_partition_value` |
| `WriteNewInlinedData` | 1835-1910 | Insert inlined data | `ducklake_inlined_data_tables`, inlined tables |
| `WriteNewInlinedDeletes` | 1930-1945 | Insert deletes | Inlined tables |
| `WriteNewInlinedTables` | 1715-1720 | Insert inlined tables | `ducklake_inlined_data_tables` |
| `WriteNewDeleteFiles` | 2310-2320 | Insert delete files | `ducklake_delete_file` |
| `WriteNewColumnMappings` | 2375-2385 | Insert mappings | `ducklake_column_mapping`, `ducklake_name_mapping` |
| `InsertSnapshot` | 2383-2390 | Insert snapshot | `ducklake_snapshot` |
| `WriteSnapshotChanges` | 2395-2405 | Insert changes | `ducklake_snapshot_changes` |
| `UpdateGlobalTableStats` | 2775-2800 | Update stats | `ducklake_table_stats`, `ducklake_table_column_stats` |
| `InsertNewSchema` | 3320-3325 | Insert schema version | `ducklake_schema_versions` |
| `SetConfigOption` | 3390-3405 | Set config | `ducklake_metadata` |

#### Functions Requiring UPDATE/DELETE Changes

| Function | Line | Purpose | Tables Modified |
|----------|------|---------|-----------------|
| `DropSchemas` | 1475-1495 | Drop schemas | `ducklake_schema`, related tables |
| `DropTables` | 1480-1500 | Drop tables | `ducklake_table`, `ducklake_column`, `ducklake_data_file`, `ducklake_delete_file`, etc. |
| `DropViews` | Same | Drop views | `ducklake_view` |
| `DropMacros` | Same | Drop macros | `ducklake_macro`, `ducklake_macro_impl`, `ducklake_macro_parameters` |
| `DropDataFiles` | 2280-2285 | Drop data files | `ducklake_data_file` |
| `DropDeleteFiles` | 2285-2290 | Drop delete files | `ducklake_delete_file` |
| `RemoveFilesScheduledForCleanup` | 2935-2945 | Remove scheduled | `ducklake_files_scheduled_for_deletion` |
| `DeleteSnapshots` | 3100-3200 | Delete snapshots | Multiple tables |
| `WriteMergeAdjacent` | 2985-3010 | Merge files | `ducklake_data_file`, `ducklake_delete_file` |
| `WriteDeleteRewrites` | 3040-3080 | Rewrite deletes | `ducklake_data_file`, `ducklake_delete_file` |
| `WriteCompactions` | 3080-3100 | Write compactions | `ducklake_data_file`, `ducklake_files_scheduled_for_deletion` |

---

### File: `src/storage/ducklake_initializer.cpp`

**Purpose**: Initializes new DuckLake instances.

**Function**: `InitializeDataPath` (line 97-115)

**Changes**: Compute effective data path by appending catalog_id:
```cpp
void DuckLakeInitializer::InitializeDataPath() {
    auto &data_path = options.data_path;
    if (data_path.empty()) {
        return;
    }

    CheckAndAutoloadedRequiredExtension(data_path);

    auto &fs = FileSystem::GetFileSystem(context);
    auto separator = fs.PathSeparator(data_path);

    if (!StringUtil::EndsWith(data_path, separator)) {
        data_path += separator;
    }
    catalog.Separator() = separator;

    // NEW: Compute effective data path with catalog_id
    if (!options.catalog_id.empty()) {
        options.effective_data_path = data_path + options.catalog_id + separator;
    } else {
        options.effective_data_path = data_path;
    }
}
```

**Function**: `InitializeNewDuckLake` (line 117-136)

**Changes**: Store catalog_id in metadata, create catalog directory.

**Function**: `LoadExistingDuckLake` (line 138-189)

**Changes**: Load and validate catalog_id from metadata.

---

### Migration Strategy

Add new migration `MigrateV04()`:

```cpp
void DuckLakeMetadataManager::MigrateV04(bool allow_failures) {
    string migrate_query = R"(
ALTER TABLE {METADATA_CATALOG}.ducklake_metadata ADD COLUMN IF NOT EXISTS catalog_id VARCHAR DEFAULT '';
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot ADD COLUMN IF NOT EXISTS catalog_id VARCHAR DEFAULT '';
-- ... all 22 tables
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.5' WHERE key = 'version';
)";
    ExecuteMigration(migrate_query, allow_failures);
}
```

---

## Implementation Order

### Phase 1: Core Infrastructure
1. Add `catalog_id` to `DuckLakeOptions`
2. Add option parsing in `ducklake_storage.cpp`
3. Add accessor in `DuckLakeCatalog`
4. Add placeholder replacement in `ducklake_transaction.cpp`
5. Add placeholder replacement in `postgres_metadata_manager.cpp`

### Phase 2: Schema Changes
1. Update all CREATE TABLE statements (22 tables)
2. Add migration function
3. Update initialization code

### Phase 3: Query Changes (largest phase)
1. Update all SELECT queries with WHERE clause
2. Update all INSERT queries with catalog_id value
3. Update all UPDATE/DELETE queries with WHERE clause

### Phase 4: Testing
1. Verify all 253 existing tests pass
2. Add multi-catalog isolation tests (workbook + workspace)
3. Test webapp direct PostgreSQL access

---

## Estimated Scope

| Category | Count | Complexity |
|----------|-------|------------|
| Header files to modify | 3 | Low |
| Option parsing | 2 functions | Low |
| Storage path computation | 3 functions | Medium |
| Schema creation | 22 tables | Medium |
| SELECT queries | ~50 queries | High |
| INSERT queries | ~30 queries | High |
| UPDATE/DELETE queries | ~20 queries | Medium |
| Migration | 1 function | Medium |
| New tests | ~10 test files | Medium |

**Total estimate**: ~100 query modifications across ~3,500 lines of code.

---

## Summary of All Changes

### Storage Path Changes (6 locations)

| File | Function/Field | Change |
|------|----------------|--------|
| `ducklake_options.hpp` | `catalog_id` | Add field |
| `ducklake_options.hpp` | `effective_data_path` | Add field |
| `ducklake_storage.cpp` | `HandleDuckLakeOption` | Parse `catalog_id` |
| `ducklake_catalog.hpp` | `DataPath()` | Return `effective_data_path` |
| `ducklake_catalog.hpp` | `CatalogId()` | Add accessor |
| `ducklake_initializer.cpp` | `InitializeDataPath()` | Compute effective path |
| `ducklake_secret.cpp` | `GetFunction()` | Add `catalog_id` param |

### Query Placeholder Changes (2 locations)

| File | Function | Change |
|------|----------|--------|
| `ducklake_transaction.cpp` | `Query()` | Add `{CATALOG_ID}` replacement |
| `postgres_metadata_manager.cpp` | `ExecuteQuery()` | Add `{CATALOG_ID}` replacement |

### Schema Changes (22 tables)

All tables in `ducklake_metadata_manager.cpp` lines 64-93 need `catalog_id VARCHAR NOT NULL` column added to schema and primary key.

### Query Changes (~100 queries)

All queries in `ducklake_metadata_manager.cpp` need:
- SELECT: Add `WHERE catalog_id = {CATALOG_ID}`
- INSERT: Add `catalog_id` value
- UPDATE/DELETE: Add `WHERE catalog_id = {CATALOG_ID}`

---

## Webapp Access After Fork

With catalog_id column-based isolation:

```ruby
def list_tables_for_workbook(workbook)
  catalog_id = "wb_#{workbook.token}"

  ActiveRecord::Base.connection.execute(<<~SQL)
    SELECT t.table_name, t.table_uuid, s.schema_name
    FROM ducklake.ducklake_table t
    JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
    WHERE t.catalog_id = '#{catalog_id}'
      AND t.end_snapshot IS NULL
      AND s.catalog_id = '#{catalog_id}'
      AND s.end_snapshot IS NULL
  SQL
end

def list_tables_for_workspace(workspace)
  catalog_id = "ws_#{workspace.token}"

  ActiveRecord::Base.connection.execute(<<~SQL)
    SELECT t.table_name, t.table_uuid, s.schema_name
    FROM ducklake.ducklake_table t
    JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
    WHERE t.catalog_id = '#{catalog_id}'
      AND t.end_snapshot IS NULL
      AND s.catalog_id = '#{catalog_id}'
      AND s.end_snapshot IS NULL
  SQL
end

def list_columns(catalog_id, table_name)
  ActiveRecord::Base.connection.execute(<<~SQL)
    SELECT c.column_name, c.column_type, c.nulls_allowed
    FROM ducklake.ducklake_column c
    JOIN ducklake.ducklake_table t ON c.table_id = t.table_id
    WHERE t.catalog_id = '#{catalog_id}'
      AND t.table_name = '#{table_name}'
      AND c.catalog_id = '#{catalog_id}'
      AND c.end_snapshot IS NULL
  SQL
end

def get_data_files(catalog_id, schema_name, table_name)
  ActiveRecord::Base.connection.execute(<<~SQL)
    SELECT df.path, df.path_is_relative, df.record_count
    FROM ducklake.ducklake_data_file df
    JOIN ducklake.ducklake_table t ON df.table_id = t.table_id
    JOIN ducklake.ducklake_schema s ON t.schema_id = s.schema_id
    WHERE df.catalog_id = '#{catalog_id}'
      AND t.table_name = '#{table_name}'
      AND s.schema_name = '#{schema_name}'
      AND df.end_snapshot IS NULL
  SQL
end
```

---

## Agent Session Setup (Final)

```sql
INSTALL ducklake; LOAD ducklake;
INSTALL postgres; LOAD postgres;

-- Workbook catalog (read-write)
ATTACH 'ducklake:postgres:host=db.example.com dbname=ducklake' AS workbook (
    CATALOG_ID 'wb_abc',
    DATA_PATH '/datalake/workbooks/wb_abc/'
);

-- Workspace catalog (read-only for shared datasets)
ATTACH 'ducklake:postgres:host=db.example.com dbname=ducklake' AS workspace (
    CATALOG_ID 'ws_123',
    DATA_PATH '/datalake/workspaces/ws_123/',
    READ_ONLY
);

USE workbook;

-- Agent can now query both
SELECT * FROM history.my_results;              -- workbook.history.my_results
SELECT * FROM workspace.datasets.customers;   -- workspace.datasets.customers
```

---

## Next Steps

1. **Fork DuckLake** to `thedatamates/ducklake`
2. **Implement Phase 1** - Core infrastructure changes
3. **Implement Phase 2** - Schema changes with migration
4. **Implement Phase 3** - Query changes (bulk of work)
5. **Implement Phase 4** - Testing
6. **Integrate with Data Plane Service**

---

## catalog_id Column Analysis

### How DuckLake Query Resolution Works

When a user queries `SELECT * FROM history.my_table`:

1. **Schema Resolution**: Look up `history` in `ducklake_schema` → get `schema_id`
2. **Table Resolution**: Look up `my_table` with that `schema_id` in `ducklake_table` → get `table_id`
3. **File Resolution**: Query `ducklake_data_file WHERE table_id = X` → get parquet file paths
4. **Read Files**: Read the parquet files

The key insight: once you have a valid `table_id`, downstream queries are implicitly tenant-scoped because `table_id` is globally unique and was resolved from a tenant-filtered schema lookup.

### Current Isolation Model (METADATA_SCHEMA)

DuckLake uses PostgreSQL schema-level isolation:
- Tenant A: `tenant_a.ducklake_schema`, `tenant_a.ducklake_table`, etc.
- Tenant B: `tenant_b.ducklake_schema`, `tenant_b.ducklake_table`, etc.

Each tenant has completely separate tables. No row-level filtering needed.

### Our Isolation Model (catalog_id column)

All tenants share the same PostgreSQL schema:
- Single `ducklake.ducklake_schema` table with rows for ALL tenants
- Single `ducklake.ducklake_table` table with rows for ALL tenants
- Filter by `WHERE catalog_id = 'tenant_a'`

### Query Patterns in ducklake_metadata_manager.cpp

**Pattern 1: Bulk Loading (NO foreign key filter)**

`GetCatalogForSnapshot()` loads the entire catalog structure:

```sql
-- Line 338: Load ALL schemas (no FK filter)
SELECT schema_id, schema_uuid, schema_name, path, path_is_relative
FROM ducklake_schema
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ...

-- Line 387: Load ALL tables (no FK filter)
SELECT schema_id, table_id, table_uuid, table_name, ...
FROM ducklake_table
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ...

-- Line 487: Load ALL views (no FK filter)
SELECT view_id, view_uuid, schema_id, view_name, ...
FROM ducklake_view
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ...
```

These queries load EVERYTHING visible at a snapshot. In our model, they NEED `catalog_id` filtering.

**Pattern 2: FK-Filtered Queries (filtered by table_id/schema_id)**

```sql
-- Line 1149: Get files for specific table
SELECT ...
FROM ducklake_data_file
WHERE table_id = %d AND {SNAPSHOT_ID} >= begin_snapshot AND ...

-- Line 1152: Get delete files for specific table
SELECT ...
FROM ducklake_delete_file
WHERE table_id = %d AND ...
```

These queries filter by `table_id` which was already resolved from tenant-filtered schema lookup. They inherit tenant isolation from the FK.

### Tables That NEED catalog_id

These tables are queried in bulk without foreign key filtering:

| Table | Reason |
|-------|--------|
| `ducklake_schema` | Loaded in bulk by GetCatalogForSnapshot (line 338) |
| `ducklake_table` | Loaded in bulk by GetCatalogForSnapshot (line 387) |
| `ducklake_view` | Loaded in bulk by GetCatalogForSnapshot (line 487) |
| `ducklake_macro` | Loaded in bulk by GetCatalogForSnapshot (line 522) |
| `ducklake_partition_info` | Loaded in bulk by GetCatalogForSnapshot (line 542) |
| `ducklake_snapshot` | Queried for time travel (line 2551, 2575, etc.) |
| `ducklake_snapshot_changes` | Joined with ducklake_snapshot |
| `ducklake_schema_versions` | Loaded for schema version tracking |
| `ducklake_table_stats` | Queried without FK filter (line 649) |
| `ducklake_files_scheduled_for_deletion` | Queried without FK filter (line 2875) |

### Tables That DON'T Need catalog_id

These tables are always queried with a foreign key filter (table_id, schema_id, etc.):

| Table | Filter Column | Reason |
|-------|---------------|--------|
| `ducklake_column` | table_id | Always filtered by table_id |
| `ducklake_data_file` | table_id | Always filtered by table_id |
| `ducklake_delete_file` | table_id | Always filtered by table_id |
| `ducklake_file_column_stats` | table_id, data_file_id | Always filtered by FK |
| `ducklake_table_column_stats` | table_id | Joined with ducklake_table_stats |
| `ducklake_partition_column` | partition_id | Joined with ducklake_partition_info |
| `ducklake_file_partition_value` | data_file_id | Always filtered by data_file_id |
| `ducklake_tag` | object_id | Filtered by object_id (table_id/view_id) |
| `ducklake_column_tag` | table_id | Always filtered by table_id |
| `ducklake_inlined_data_tables` | table_id | Always filtered by table_id |
| `ducklake_column_mapping` | table_id | Always filtered by table_id |
| `ducklake_name_mapping` | mapping_id | Always filtered by mapping_id |
| `ducklake_macro_impl` | macro_id | Always filtered by macro_id |
| `ducklake_macro_parameters` | macro_id | Always filtered by macro_id |

### Global Tables (NO catalog_id)

| Table | Reason |
|-------|--------|
| `ducklake_metadata` | Global config (version, data_path, encrypted). Instance-level, not tenant-level. |

### Implementation Strategy

1. Add `catalog_id` column only to tables that need it (10 tables)
2. Add `WHERE catalog_id = {CATALOG_ID}` to bulk queries
3. Tables filtered by FK inherit isolation - no changes needed
4. Use explicit column names in INSERT statements to avoid position dependency
