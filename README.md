# DuckLake Fork - Multi-Tenant Catalog Isolation

## The Vision: Hyper-Tenancy for AI Agents

Traditional database scaling focuses on capacity (more data) and throughput (more queries). But [AI agents are creating a new dimension for database scalability](https://thenewstack.io/ai-agents-create-a-new-dimension-for-database-scalability/): **How many databases can you create and maintain?**

Agents are spun up by the millions. Each agent needs private data storage with strict isolation guarantees. Traditional multitenancy breaks down at this scale. What's needed is **hyper-tenancy**: finer-grained isolation with elastic scalability.

This fork enables that vision by separating **compute** from **catalog**:

- **DuckDB as compute nodes** - Spin up ephemeral DuckDB instances per agent or workflow. They're lightweight, in-process, and disposable.
- **PostgreSQL as shared catalog** - One central metadata store with `catalog_id`-based isolation. Millions of catalogs, one database.
- **Instant provisioning** - Creating a new catalog is a single INSERT, not a new database instance.
- **True isolation** - Each catalog's data is completely separated via `WHERE catalog_id = ?`

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Agent A    │  │  Agent B    │  │  Agent C    │
│  (DuckDB)   │  │  (DuckDB)   │  │  (DuckDB)   │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       │ ATTACH         │ ATTACH         │ ATTACH
       │ CATALOG 'a'    │ CATALOG 'b'    │ CATALOG 'c'
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │   PostgreSQL    │
              │  (shared catalog)│
              │                 │
              │ catalog_id = 0  │
              │ catalog_id = 1  │
              │ catalog_id = 2  │
              │      ...        │
              └─────────────────┘
```

Each agent gets its own tables, schemas, snapshots, and parquet files - all isolated by a simple integer ID. When the agent is done, the compute node disappears; the catalog entry can be cleaned up trivially.

---

## Overview

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
    CATALOG 'my-catalog',
    CREATE_IF_NOT_EXISTS true
);

USE my_lake;
CREATE TABLE test(id INTEGER, name VARCHAR);
INSERT INTO test VALUES (1, 'hello');
```

**CATALOG is mandatory.** Every DuckLake attachment must specify a catalog name.
`CREATE_IF_NOT_EXISTS` defaults to `false`, so set it to `true` when bootstrapping a new metadata catalog.

---

# Architecture

## Metadata Storage

Metadata is stored in a catalog database (DuckDB file, PostgreSQL, or MySQL).

### Core Tables (Global)

| Table | Columns |
|-------|---------|
| `ducklake_metadata` | key, value, scope, scope_id |
| `ducklake_snapshot` | snapshot_id (PK), snapshot_time, schema_version, next_catalog_id, next_file_id |
| `ducklake_snapshot_changes` | snapshot_id (PK), changes_made, author, commit_message, commit_extra_info |

### Catalog & Schema (Per-Catalog)

| Table | Columns |
|-------|---------|
| `ducklake_catalog` | **catalog_id**, catalog_uuid, catalog_name, begin_snapshot, end_snapshot |
| `ducklake_schema` | **catalog_id**, schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative |
| `ducklake_schema_versions` | **catalog_id**, begin_snapshot, schema_version |

### Table & View Definitions

| Table | Columns |
|-------|---------|
| `ducklake_table` | **catalog_id**, table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative |
| `ducklake_view` | **catalog_id**, view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases |
| `ducklake_column` | **catalog_id**, column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, default_value_type, default_value_dialect |
| `ducklake_tag` | **catalog_id**, object_id, begin_snapshot, end_snapshot, key, value |
| `ducklake_column_tag` | **catalog_id**, table_id, column_id, begin_snapshot, end_snapshot, key, value |

### Data Files

| Table | Columns |
|-------|---------|
| `ducklake_data_file` | **catalog_id**, data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id |
| `ducklake_delete_file` | **catalog_id**, delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key |
| `ducklake_files_scheduled_for_deletion` | **catalog_id**, data_file_id, path, path_is_relative, schedule_start |
| `ducklake_inlined_data_tables` | **catalog_id**, table_id, table_name, schema_version |

### Statistics

| Table | Columns |
|-------|---------|
| `ducklake_table_stats` | **catalog_id**, table_id, record_count, next_row_id, file_size_bytes |
| `ducklake_table_column_stats` | **catalog_id**, table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats |
| `ducklake_file_column_stats` | **catalog_id**, data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats |

### Partitioning

| Table | Columns |
|-------|---------|
| `ducklake_partition_info` | **catalog_id**, partition_id, table_id, begin_snapshot, end_snapshot |
| `ducklake_partition_column` | **catalog_id**, partition_id, table_id, partition_key_index, column_id, transform |
| `ducklake_file_partition_value` | **catalog_id**, data_file_id, table_id, partition_key_index, partition_value |

### Column Mapping

| Table | Columns |
|-------|---------|
| `ducklake_column_mapping` | **catalog_id**, mapping_id, table_id, type |
| `ducklake_name_mapping` | **catalog_id**, mapping_id, column_id, source_name, target_field_id, parent_column, is_partition |

### Macros

| Table | Columns |
|-------|---------|
| `ducklake_macro` | **catalog_id**, schema_id, macro_id, macro_name, begin_snapshot, end_snapshot |
| `ducklake_macro_impl` | **catalog_id**, macro_id, impl_id, dialect, sql, type |
| `ducklake_macro_parameters` | **catalog_id**, macro_id, impl_id, column_id, parameter_name, parameter_type, default_value, default_value_type |

**Bold** columns indicate `catalog_id` for multi-tenant isolation. With global snapshots, the snapshot tables (`ducklake_snapshot`, `ducklake_snapshot_changes`) are shared across all catalogs, while entity tables use composite primary keys for isolation.

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
2. **Global Snapshots** - One snapshot sequence shared by all catalogs (enables forking without ID remapping)
3. **Unified ID Counter** - All entity IDs (catalogs, schemas, tables, views, macros, partitions) come from a single `next_catalog_id` counter on snapshots
4. **Composite Primary Keys** - Entity tables use `(catalog_id, entity_id)` composite keys for isolation
5. **Initial ID Allocation** - First catalog gets `catalog_id = 0`, main schema gets `schema_id = 1`, `next_catalog_id` starts at 2
6. **Data Path Isolation** - Each catalog's data is stored in `DATA_PATH/catalog_name/`
7. **Metadata Isolation** - All metadata queries filter by `catalog_id`
8. **File-Based DuckDB Limitation** - Cannot attach same DuckDB file as metadata for multiple catalogs (use PostgreSQL for shared metadata)

## Version

This fork is based on DuckLake 0.3 with significant schema changes for multi-tenant isolation and catalog forking. Migration from upstream DuckLake is automatic when `MIGRATE_IF_REQUIRED = true` (default).

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

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `CATALOG` | **Yes** | - | Catalog name for tenant isolation |
| `DATA_PATH` | No | (none) | Base path for parquet files |
| `METADATA_SCHEMA` | No | `public` | PostgreSQL schema for metadata tables |
| `SNAPSHOT_VERSION` | No | (latest) | Attach at specific version (read-only) |
| `SNAPSHOT_TIME` | No | (latest) | Attach at specific timestamp (read-only) |
| `ENCRYPTED` | No | `AUTOMATIC` | Enable/disable encryption (inherits from existing catalog) |
| `DATA_INLINING_ROW_LIMIT` | No | `0` | Max rows to inline in catalog (0 = disabled) |
| `CREATE_IF_NOT_EXISTS` | No | `false` | Create new DuckLake if not found |
| `MIGRATE_IF_REQUIRED` | No | `true` | Auto-migrate older DuckLake versions |
| `OVERRIDE_DATA_PATH` | No | `false` | Allow different DATA_PATH than stored in catalog |

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

---

# Catalog Forking

Fork a catalog to create an instant copy that **shares the underlying parquet files**. The fork gets its own metadata but reads from the same physical files as the parent.

## Use Case

- **AI Agents**: Each agent gets an isolated workspace forked from shared data
- **Experimentation**: Branch a catalog to try changes without affecting the original
- **Testing**: Create throwaway copies of production data

## How It Works

```sql
-- Parent catalog with 1TB of data
ATTACH 'ducklake:postgres:...' AS parent (CATALOG 'shared_data', DATA_PATH '/data/');

-- Instant fork - no data copying!
SELECT ducklake_fork_catalog('shared_data', 'agent_workspace');

-- Attach the fork with its own DATA_PATH for new writes
ATTACH 'ducklake:postgres:...' AS workspace (CATALOG 'agent_workspace', DATA_PATH '/data/');

-- Fork can read all parent's data
SELECT * FROM workspace.main.big_table;  -- Reads parent's parquet files

-- Fork's writes go to its own location
INSERT INTO workspace.main.results VALUES (...);  -- Writes to /data/agent_workspace/
```

## Key Points

- **Instant**: Fork copies metadata rows, not terabytes of parquet files
- **Isolated**: Fork's writes don't affect parent; parent's writes don't affect fork
- **Safe cleanup**: Deleting a fork doesn't delete shared files still used by parent
- **Same IDs**: Fork's `data_file_id` values match parent's (same physical files)

For technical details on how forking works (global snapshots, composite keys, etc.), see [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md).

---

# Parquet File Lifecycle

DuckLake uses a **copy-on-write** model. Files are never modified in place - new files are created and metadata tracks which files are valid for each snapshot.

## File Storage

Files are stored at: `DATA_PATH/catalog_name/schema_name/table_name/*.parquet`

Each file is tracked in metadata with:
- `data_file_id` - Unique identifier
- `begin_snapshot` - Snapshot when file became valid
- `end_snapshot` - Snapshot when file was invalidated (NULL if still valid)
- `record_count`, `file_size_bytes` - Statistics

## Operations and File Behavior

| Operation | Files Created | Files Modified | Files Deleted |
|-----------|--------------|----------------|---------------|
| **INSERT** | New parquet file(s) | None | None |
| **SELECT** | None | None | None |
| **UPDATE** | New parquet + delete file | None | None |
| **DELETE** | Delete file (or none if full file) | None | None |
| **DROP TABLE** | None | Sets `end_snapshot` on all files | None |
| **expire_snapshots** | None | Schedules old files for deletion | None |
| **cleanup_old_files** | None | None | Deletes scheduled files |
| **merge_adjacent_files** | New merged file | Sets `end_snapshot` on old | None |
| **rewrite_data_files** | New file without deleted rows | Sets `end_snapshot` on old | None |

## INSERT

```
Before: table has files [A, B]
INSERT INTO table VALUES (...)
After:  table has files [A, B, C]  ← new file C created
```

- New parquet file written to `DATA_PATH/catalog/schema/table/`
- File registered in `ducklake_data_file` with `begin_snapshot = current`
- **Data inlining**: If rows ≤ `data_inlining_row_limit`, data stored in metadata (no file created)

## UPDATE (Copy-on-Write)

```
Before: file A contains rows [1, 2, 3]
UPDATE table SET x = 'new' WHERE id = 2
After:  file A unchanged, delete file D marks row 2, file E has updated row 2
```

- Original file **not modified**
- Delete file created to mark updated rows as deleted
- New file created with updated row values
- Both files remain accessible for time travel

## DELETE

**Partial delete** (some rows in file):
```
Before: file A contains rows [1, 2, 3]
DELETE FROM table WHERE id = 2
After:  file A unchanged, delete file D created marking row 2
```

**Full file delete** (all rows in file):
```
Before: file A contains rows [1, 2, 3]
DELETE FROM table WHERE id IN (1, 2, 3)
After:  file A marked with end_snapshot (no delete file needed)
```

- Delete files are small parquet files containing row IDs to skip
- Original data files remain for time travel
- Full file deletes just set `end_snapshot` (more efficient)

## SELECT (Read Path)

```sql
SELECT * FROM table;  -- Current snapshot
SELECT * FROM table AT SNAPSHOT 5;  -- Historical snapshot
```

1. Find files where `begin_snapshot <= target AND (end_snapshot IS NULL OR end_snapshot > target)`
2. For each file, check for associated delete files
3. Read parquet files, filtering out deleted rows
4. Apply query predicates (pushed down to parquet reader when possible)

## File Cleanup Lifecycle

Files go through this lifecycle:

```
ACTIVE                    SCHEDULED               DELETED
─────────────────────────────────────────────────────────────
[File created] ──────────► [end_snapshot set] ──► [File removed]
    INSERT                  expire_snapshots        cleanup_old_files
    UPDATE (new file)       rewrite_data_files      delete_orphaned_files
    merge (new file)        merge (old files)
```

**Step 1: File becomes inactive**
- `end_snapshot` is set when file is superseded (compaction, rewrite) or table is dropped
- File still exists on disk for time travel to old snapshots

**Step 2: Snapshot expires**
- `expire_snapshots()` marks old snapshots as expired
- Files only referenced by expired snapshots are scheduled for deletion
- Added to `ducklake_files_scheduled_for_deletion` table

**Step 3: File deleted**
- `cleanup_old_files()` deletes files that have been scheduled for longer than `delete_older_than`
- `delete_orphaned_files()` finds and removes files not referenced by any snapshot

## Compaction

**merge_adjacent_files** - Combines small files:
```
Before: files [A(1MB), B(2MB), C(1MB)]
CALL ducklake.merge_adjacent_files();
After:  files [A, B, C] marked with end_snapshot, new file D(4MB) created
```

**rewrite_data_files** - Removes deleted rows:
```
Before: file A has 1000 rows, 300 marked deleted
CALL ducklake.rewrite_data_files();
After:  file A marked with end_snapshot, new file B has 700 rows
```

Both operations:
- Create new optimized files
- Mark old files with `end_snapshot`
- Old files remain until `cleanup_old_files()` is called
- Controlled by `auto_compact` and `rewrite_delete_threshold` options

---

# Functions & Operations

## Query Functions

These return data and are called via `SELECT`:

| Function | Description |
|----------|-------------|
| `ducklake_snapshots('catalog')` | List all snapshots with metadata |
| `ducklake_current_snapshot('catalog')` | Get current snapshot ID |
| `ducklake_last_committed_snapshot('catalog')` | Get last committed snapshot ID |
| `ducklake_options('catalog')` | List all configuration options and their values |
| `ducklake_list_files('catalog', 'table')` | List parquet files for a table |
| `ducklake_table_info('catalog', 'table')` | Get table metadata |
| `ducklake_table_changes('catalog', 'schema', 'table', start, end)` | Get all changes between snapshots |
| `ducklake_table_insertions('catalog', 'schema', 'table', start, end)` | Get insertions between snapshots |
| `ducklake_table_deletions('catalog', 'schema', 'table', start, end)` | Get deletions between snapshots |

**Shorthand syntax:** `SELECT * FROM catalog.snapshots()` instead of `ducklake_snapshots('catalog')`.

## Procedure Functions

These perform operations and are called via `CALL`:

| Function | Description |
|----------|-------------|
| `ducklake_fork_catalog('parent', 'new_name')` | Create instant copy of a catalog (shares parquet files) |
| `ducklake_expire_snapshots('catalog', ...)` | Remove old snapshots (keeps files until cleanup) |
| `ducklake_merge_adjacent_files('catalog', 'table')` | Merge small parquet files into larger ones |
| `ducklake_rewrite_data_files('catalog', 'table')` | Rewrite files to remove deleted rows |
| `ducklake_flush_inlined_data('catalog')` | Flush inlined data to parquet files |
| `ducklake_add_data_files('catalog', 'table', 'path')` | Register external parquet files |
| `ducklake_cleanup_old_files('catalog')` | Delete files from expired snapshots |
| `ducklake_delete_orphaned_files('catalog')` | Delete orphaned files not in any snapshot |
| `ducklake_set_option('catalog', 'option', value, ...)` | Set configuration option |
| `ducklake_set_commit_message('catalog', 'author', 'message')` | Set commit metadata for next snapshot |

**Shorthand syntax:** `CALL catalog.merge_adjacent_files()` instead of `ducklake_merge_adjacent_files('catalog')`.

## Configuration Options

Set via `CALL ducklake_set_option('catalog', 'option', value)` or `CALL catalog.set_option('option', value)`.

Options can be scoped globally, per-schema, or per-table using `schema =>` and `table_name =>` parameters.

| Option | Default | Description |
|--------|---------|-------------|
| `data_inlining_row_limit` | `0` | Max rows to store inline in metadata (0 = disabled) |
| `target_file_size` | `512MB` | Target size for parquet files |
| `parquet_compression` | `snappy` | Compression: uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw |
| `parquet_compression_level` | (default) | Compression level for algorithms that support it |
| `parquet_version` | `2` | Parquet format version (1 or 2) |
| `parquet_row_group_size` | (auto) | Rows per row group |
| `parquet_row_group_size_bytes` | (auto) | Bytes per row group |
| `per_thread_output` | `false` | Create separate files per thread during parallel insert |
| `hive_file_pattern` | `true` | Use hive-style folder structure for partitions |
| `auto_compact` | `true` | Enable automatic compaction in maintenance functions |
| `rewrite_delete_threshold` | `0.1` | Min deleted fraction (0-1) before rewrite is triggered |
| `delete_older_than` | `2 days` | Age threshold for `cleanup_old_files` and `delete_orphaned_files` |
| `expire_older_than` | (none) | Default age threshold for `expire_snapshots` |
| `require_commit_message` | `false` | Require explicit commit message for snapshots |
| `encrypted` | `false` | Encrypt parquet files |

## Example Workflows

**View snapshot history:**
```sql
SELECT snapshot_id, snapshot_time, changes FROM ducklake.snapshots();
```

**Track changes between versions:**
```sql
SELECT * FROM ducklake.table_changes('main', 'orders', 5, 10) ORDER BY snapshot_id;
```

**Expire old snapshots and clean up files:**
```sql
CALL ducklake_expire_snapshots('ducklake', older_than := INTERVAL '30 days');
CALL ducklake_cleanup_old_files('ducklake');
```

**Compact small files:**
```sql
CALL ducklake.merge_adjacent_files();  -- All tables
CALL ducklake_merge_adjacent_files('ducklake', 'main.orders');  -- Specific table
```

**Set compression for a schema:**
```sql
CALL ducklake.set_option('parquet_compression', 'zstd', schema => 'analytics');
```

---

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
| "CATALOG is required. Please provide CATALOG when attaching the database." | Missing CATALOG option | Add `CATALOG 'name'` to ATTACH |
| "Catalog 'x' does not exist" | Catalog not in database | Use `CREATE_IF_NOT_EXISTS true` |
| "Unique file handle conflict" | Same DuckDB file attached twice | Use PostgreSQL for shared metadata |

---

# Performance Considerations

## PostgreSQL Indexing

```sql
CREATE INDEX idx_schema_catalog ON ducklake_schema(catalog_id);
CREATE INDEX idx_table_catalog ON ducklake_table(catalog_id);
CREATE INDEX idx_data_file_catalog ON ducklake_data_file(catalog_id);
CREATE INDEX idx_snapshot_id ON ducklake_snapshot(snapshot_id DESC);
```

## File Size Tuning

```sql
CALL ducklake.set_option('target_file_size', '512MB');
CALL ducklake.set_option('parquet_row_group_size_bytes', '128MB');
```

---

# References

- Upstream: https://github.com/duckdb/ducklake
- DuckDB Docs: https://duckdb.org/docs/stable/core_extensions/ducklake
- Project docs index: ./docs/README.md
- Snapshot allocation and conflict-visibility notes: ./docs/SNAPSHOT_SEQUENCE.md
- License: MIT
