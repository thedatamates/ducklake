# DuckLake Catalog Forking

## Overview

Catalog forking enables one DuckLake catalog to create a copy of another catalog's metadata while sharing the underlying parquet files. This is essential for hyper-tenancy scenarios where millions of catalogs need instant provisioning without copying terabytes of data.

### Use Case: Hyper-Tenancy

In a multi-agent environment:
- A **parent catalog** contains shared, immutable datasets
- **Forked catalogs** are created instantly for each agent
- Each agent can read shared data and write new data to their own `DATA_PATH`
- Files written by one agent are isolated from others
- The parent's files are never modified or deleted by forked catalogs

```
┌─────────────────────────────────────────────────────────────┐
│                  Shared Metadata Database                   │
├─────────────────────────────────────────────────────────────┤
│  Global Snapshots: 0, 1, 2, ... 50, 51, 52 ...             │
│                                                             │
│  parent_catalog (catalog_id=0, begin_snapshot=0)           │
│    └── schemas, tables, data_file references               │
│                                                             │
│  agent_001 (catalog_id=2, begin_snapshot=50)               │
│    └── forked from parent + new writes                     │
│                                                             │
│  agent_002 (catalog_id=5, begin_snapshot=51)               │
│    └── forked from parent + new writes                     │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ Parent DATA_PATH│  │Agent 001        │  │Agent 002        │
│ /shared/data/   │  │DATA_PATH        │  │DATA_PATH        │
│                 │  │/agents/001/     │  │/agents/002/     │
│ *.parquet       │  │                 │  │                 │
│ (read by all)   │  │*.parquet (new)  │  │*.parquet (new)  │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

## Core Design: Global Snapshots + Composite Keys

### Key Principles

1. **Global snapshots** - One snapshot sequence for the entire metadata database
2. **Global counters** - Stored on snapshots, naturally global
3. **Composite primary keys** - Entities scoped to catalog_id for isolation
4. **Catalogs are versioned** - `ducklake_catalog` has begin_snapshot/end_snapshot like other entities

### The Architecture

```sql
-- Global snapshots (no catalog_id - this IS the global version)
CREATE TABLE ducklake_snapshot(
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    next_catalog_id BIGINT,  -- Global counter for entity IDs
    next_file_id BIGINT      -- Global counter for file IDs
);

-- Catalogs are versioned entities
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT,
    catalog_uuid UUID NOT NULL,
    catalog_name VARCHAR NOT NULL,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, begin_snapshot)
);

-- Other entities have composite keys with catalog_id
CREATE TABLE ducklake_data_file(
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    ...,
    PRIMARY KEY (catalog_id, data_file_id)
);
```

### How It Works

Every commit from ANY catalog creates the next global snapshot:

```
Catalog A commits  → snapshot 51 (next_file_id=100)
Catalog B commits  → snapshot 52 (next_file_id=103)
Catalog A commits  → snapshot 53 (next_file_id=105)
Fork C created     → snapshot 54 (next_file_id=105)
```

| Aspect | Behavior |
|--------|----------|
| Snapshot sequence | Global - shared across all catalogs |
| Counters | On snapshots - already global |
| ID generation | Read latest snapshot, increment counter |
| Catalog isolation | Composite keys `(catalog_id, entity_id)` |
| Same ID across catalogs | Means same physical file (forked reference) |
| Cross-catalog file check | `WHERE data_file_id = ?` finds all references |
| Fork | Copy rows, change `catalog_id`, set `begin_snapshot` |
| Time travel | Global: see entire database at snapshot N |

### Why This Design

1. **Counters are already on snapshots** - no new table needed
2. **Global snapshots make counters naturally global** - no per-catalog collision
3. **Catalogs as versioned entities** - can time travel the whole database
4. **Immutable data is cheap** - just INSERT with begin_snapshot, no end_snapshot updates
5. **Fork is trivial** - copy rows, change catalog_id, same IDs preserved

## Schema Changes Required

### Changed: ducklake_snapshot (Remove catalog_id)

```sql
-- Before
CREATE TABLE ducklake_snapshot(
    catalog_id BIGINT NOT NULL,
    snapshot_id BIGINT PRIMARY KEY,
    ...
);

-- After
CREATE TABLE ducklake_snapshot(
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    next_catalog_id BIGINT,
    next_file_id BIGINT
);
```

### Changed: ducklake_snapshot_changes (Remove catalog_id)

```sql
-- Before
CREATE TABLE ducklake_snapshot_changes(
    catalog_id BIGINT NOT NULL,
    snapshot_id BIGINT PRIMARY KEY,
    ...
);

-- After
CREATE TABLE ducklake_snapshot_changes(
    snapshot_id BIGINT PRIMARY KEY,
    catalog_id BIGINT NOT NULL,  -- Which catalog made this commit (metadata, not key)
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);
```

### Changed: ducklake_catalog (Add versioning)

```sql
-- Before
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT PRIMARY KEY,
    catalog_name VARCHAR NOT NULL UNIQUE,
    created_snapshot BIGINT NOT NULL
);

-- After
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT,
    catalog_uuid UUID NOT NULL,
    catalog_name VARCHAR NOT NULL,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, begin_snapshot)
);
```

### Changed: Composite Primary Keys on Entity Tables

**Tables already having catalog_id (change PK only):**

| Table | Current PK | New PK |
|-------|------------|--------|
| `ducklake_schema` | `schema_id` | `(catalog_id, schema_id)` |
| `ducklake_table` | none | `(catalog_id, table_id, begin_snapshot)` |
| `ducklake_view` | none | `(catalog_id, view_id, begin_snapshot)` |
| `ducklake_table_stats` | none | `(catalog_id, table_id)` |
| `ducklake_partition_info` | none | `(catalog_id, partition_id)` |
| `ducklake_schema_versions` | none | `(catalog_id, begin_snapshot)` |
| `ducklake_macro` | none | `(catalog_id, macro_id, begin_snapshot)` |

**Tables needing catalog_id added:**

| Table | Current PK | New PK |
|-------|------------|--------|
| `ducklake_data_file` | `data_file_id` | `(catalog_id, data_file_id)` |
| `ducklake_delete_file` | `delete_file_id` | `(catalog_id, delete_file_id)` |
| `ducklake_column` | none | `(catalog_id, table_id, column_id, begin_snapshot)` |
| `ducklake_file_column_stats` | none | `(catalog_id, data_file_id, column_id)` |
| `ducklake_table_column_stats` | none | `(catalog_id, table_id, column_id)` |
| `ducklake_partition_column` | none | `(catalog_id, partition_id, partition_key_index)` |
| `ducklake_file_partition_value` | none | `(catalog_id, data_file_id, partition_key_index)` |
| `ducklake_column_mapping` | none | `(catalog_id, mapping_id)` |
| `ducklake_name_mapping` | none | `(catalog_id, mapping_id, column_id)` |
| `ducklake_tag` | none | `(catalog_id, object_id, key, begin_snapshot)` |
| `ducklake_column_tag` | none | `(catalog_id, table_id, column_id, key, begin_snapshot)` |
| `ducklake_inlined_data_tables` | none | `(catalog_id, table_id)` |
| `ducklake_macro_impl` | none | `(catalog_id, macro_id, impl_id)` |
| `ducklake_macro_parameters` | none | `(catalog_id, macro_id, impl_id, column_id)` |

### Full Schema (After Changes)

```sql
CREATE TABLE ducklake_metadata(
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    scope VARCHAR,
    scope_id BIGINT
);

CREATE TABLE ducklake_snapshot(
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ,
    schema_version BIGINT,
    next_catalog_id BIGINT,
    next_file_id BIGINT
);

CREATE TABLE ducklake_snapshot_changes(
    snapshot_id BIGINT PRIMARY KEY,
    catalog_id BIGINT NOT NULL,  -- Who made this commit
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);

CREATE TABLE ducklake_catalog(
    catalog_id BIGINT,
    catalog_uuid UUID NOT NULL,
    catalog_name VARCHAR NOT NULL,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, begin_snapshot)
);

CREATE TABLE ducklake_schema(
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    schema_uuid UUID,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    schema_name VARCHAR,
    path VARCHAR,
    path_is_relative BOOLEAN,
    PRIMARY KEY (catalog_id, schema_id)
);

CREATE TABLE ducklake_table(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    table_uuid UUID,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    schema_id BIGINT,
    table_name VARCHAR,
    path VARCHAR,
    path_is_relative BOOLEAN,
    PRIMARY KEY (catalog_id, table_id, begin_snapshot)
);

CREATE TABLE ducklake_data_file(
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    table_id BIGINT,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    file_order BIGINT,
    path VARCHAR,
    path_is_relative BOOLEAN,
    file_format VARCHAR,
    record_count BIGINT,
    file_size_bytes BIGINT,
    footer_size BIGINT,
    row_id_start BIGINT,
    partition_id BIGINT,
    encryption_key VARCHAR,
    partial_file_info VARCHAR,
    mapping_id BIGINT,
    PRIMARY KEY (catalog_id, data_file_id)
);

-- ... (remaining tables follow same pattern with catalog_id + composite keys)
```

## Snapshot and Counter Behavior

### How Snapshots Are Created (Current DuckLake Behavior)

**Snapshots are automatic, not manual.** Every transaction commit that makes changes creates a new snapshot.

The flow:
```
DuckLakeTransaction::Commit()
  └── if ChangesMade():
        └── FlushChanges()
              └── snapshot_id++
              └── INSERT INTO ducklake_snapshot (...)
```

**What counts as "changes made"** (from `ducklake_transaction.cpp:101-103`):
- Schema changes: CREATE/DROP TABLE, CREATE/DROP SCHEMA, CREATE/DROP VIEW, CREATE/DROP MACRO
- Data changes: INSERT, UPDATE, DELETE (tracked in `table_data_changes`)
- Dropped files
- New name maps

**Read-only transactions don't create snapshots.** A SELECT query commits without incrementing `snapshot_id`.

**Transaction batching matters.** DuckDB uses auto-commit by default:
```sql
-- Auto-commit (default): 3 statements = 3 transactions = 3 snapshots
INSERT INTO t VALUES (1);  -- snapshot N+1
INSERT INTO t VALUES (2);  -- snapshot N+2
INSERT INTO t VALUES (3);  -- snapshot N+3

-- Explicit transaction: 3 statements = 1 transaction = 1 snapshot
BEGIN;
INSERT INTO t VALUES (1);
INSERT INTO t VALUES (2);
INSERT INTO t VALUES (3);
COMMIT;  -- snapshot N+1 (just one)
```

**Code references:**
- `src/storage/ducklake_transaction.cpp:56-65` - `Commit()` calls `FlushChanges()` if `ChangesMade()`
- `src/storage/ducklake_transaction.cpp:101-103` - `ChangesMade()` definition
- `src/storage/ducklake_transaction.cpp:1645-1708` - `FlushChanges()` increments `snapshot_id` and inserts snapshot
- `src/storage/ducklake_transaction_manager.cpp:26-36` - `CommitTransaction()` calls `Commit()`

### Scalability Implications for Global Snapshots

With global snapshots, every commit from ANY catalog increments the global counter:

```
Catalog A commits → snapshot 51
Catalog B commits → snapshot 52
Catalog C commits → snapshot 53
```

**Worst case (auto-commit, no batching):**
```
1 million agents × 10 writes/second × auto-commit = 10 million snapshots/second
```

**Best case (batched transactions):**
```
1 million agents × 1 transaction/minute = ~17,000 snapshots/second
```

**For hyper-tenancy, recommend:**
1. Agents use explicit transactions to batch work
2. Read-heavy workloads (most agent work) don't create snapshots
3. If scaling limits hit, shard into multiple metadata databases (stamps)

### Global Snapshot Sequence

Every commit creates the next global snapshot:

```
Initial state:           snapshot 0 (next_catalog_id=2, next_file_id=0)
Parent creates table:    snapshot 1 (next_catalog_id=3, next_file_id=0)
Parent inserts data:     snapshot 2 (next_catalog_id=3, next_file_id=5)
Fork A created:          snapshot 3 (next_catalog_id=4, next_file_id=5)
Fork A inserts data:     snapshot 4 (next_catalog_id=4, next_file_id=8)
Parent inserts data:     snapshot 5 (next_catalog_id=4, next_file_id=11)
Fork B created:          snapshot 6 (next_catalog_id=5, next_file_id=11)
```

### Counter Allocation

To allocate an ID:

```cpp
// Get latest snapshot
auto result = transaction.Query(
    "SELECT next_file_id FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1");
idx_t next_id = result->GetValue<idx_t>(0, 0);

// Use it
data_file.id = DataFileIndex(next_id);

// At commit, create new snapshot with incremented counter
INSERT INTO ducklake_snapshot (snapshot_id, ..., next_file_id)
VALUES (new_snapshot_id, ..., next_id + files_created);
```

Since every commit creates a new snapshot with updated counters, and snapshots are global, all IDs are globally unique.

### Time Travel

Query the entire database at any snapshot:

```sql
-- What catalogs existed at snapshot 50?
SELECT * FROM ducklake_catalog
WHERE begin_snapshot <= 50 AND (end_snapshot IS NULL OR end_snapshot > 50);

-- What files did catalog 2 have at snapshot 50?
SELECT * FROM ducklake_data_file
WHERE catalog_id = 2
  AND begin_snapshot <= 50
  AND (end_snapshot IS NULL OR end_snapshot > 50);
```

### Immutable Data Optimization

For immutable (append-only) data:
- Rows only have `begin_snapshot` set (when created)
- `end_snapshot` stays NULL forever (never retired)
- No UPDATE operations needed
- Snapshots are very cheap (just INSERTs)

```sql
-- Immutable query simplifies to:
SELECT * FROM ducklake_data_file
WHERE catalog_id = 2 AND begin_snapshot <= 50;
```

## Fork Implementation

### Fork is Trivial

With global snapshots and composite keys:

```sql
-- 1. Get next catalog_id from latest snapshot
SELECT next_catalog_id FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;
-- Returns: 5

-- 2. Create new snapshot for fork operation
INSERT INTO ducklake_snapshot (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
SELECT MAX(snapshot_id) + 1, NOW(), schema_version, next_catalog_id + 1, next_file_id
FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;
-- Creates: snapshot 54 with next_catalog_id=6

-- 3. Create catalog entry
INSERT INTO ducklake_catalog (catalog_id, catalog_uuid, catalog_name, begin_snapshot, end_snapshot)
VALUES (5, UUID(), 'fork_name', 54, NULL);

-- 4. Copy all metadata (same IDs, new catalog_id, new begin_snapshot)
INSERT INTO ducklake_schema (catalog_id, schema_id, ..., begin_snapshot, end_snapshot)
SELECT 5, schema_id, ..., 54, NULL
FROM ducklake_schema
WHERE catalog_id = parent_catalog_id
  AND end_snapshot IS NULL;  -- Only current rows

INSERT INTO ducklake_table (catalog_id, table_id, ..., begin_snapshot, end_snapshot)
SELECT 5, table_id, ..., 54, NULL
FROM ducklake_table
WHERE catalog_id = parent_catalog_id
  AND end_snapshot IS NULL;

INSERT INTO ducklake_data_file (catalog_id, data_file_id, ..., begin_snapshot, end_snapshot)
SELECT 5, data_file_id, ..., 54, NULL
FROM ducklake_data_file
WHERE catalog_id = parent_catalog_id
  AND end_snapshot IS NULL;

-- ... same for all entity tables

-- 5. Convert relative paths to absolute
UPDATE ducklake_data_file
SET path = parent_data_path || path, path_is_relative = FALSE
WHERE catalog_id = 5 AND path_is_relative = TRUE;

-- 6. Record fork in snapshot_changes
INSERT INTO ducklake_snapshot_changes (snapshot_id, catalog_id, changes_made)
VALUES (54, 5, 'forked_from:' || parent_catalog_id);
```

No ID remapping. IDs preserve lineage. Fork's `data_file_id=3` is the same file as parent's `data_file_id=3`.

### Fork Function

```sql
CALL ducklake_fork_catalog('parent_catalog', 'new_catalog_name', '/new/data/path');
```

**Steps:**

1. Validate inputs (parent exists, new name unique, paths don't overlap)
2. Create new global snapshot
3. Allocate new catalog_id from snapshot's counter
4. Create catalog entry with `begin_snapshot = new_snapshot`
5. Copy all current entity rows (WHERE end_snapshot IS NULL)
6. Convert relative paths to absolute
7. Record fork in snapshot_changes

### Tables to Copy During Fork

| Table | Copy Strategy |
|-------|---------------|
| `ducklake_catalog` | Create new (don't copy) |
| `ducklake_schema` | Copy current rows |
| `ducklake_schema_versions` | Copy current rows |
| `ducklake_table` | Copy current rows |
| `ducklake_view` | Copy current rows |
| `ducklake_column` | Copy current rows |
| `ducklake_data_file` | Copy current rows, convert relative paths |
| `ducklake_delete_file` | Copy current rows, convert relative paths |
| `ducklake_file_column_stats` | Copy current rows |
| `ducklake_table_stats` | Copy current rows |
| `ducklake_table_column_stats` | Copy current rows |
| `ducklake_partition_info` | Copy current rows |
| `ducklake_partition_column` | Copy current rows |
| `ducklake_file_partition_value` | Copy current rows |
| `ducklake_tag` | Copy current rows |
| `ducklake_column_tag` | Copy current rows |
| `ducklake_column_mapping` | Copy current rows |
| `ducklake_name_mapping` | Copy current rows |
| `ducklake_macro` | Copy current rows |
| `ducklake_macro_impl` | Copy current rows |
| `ducklake_macro_parameters` | Copy current rows |
| `ducklake_inlined_data_tables` | Copy current rows + copy actual data tables |
| `ducklake_snapshot` | Don't copy (global) |
| `ducklake_snapshot_changes` | Don't copy (global history) |
| `ducklake_metadata` | Don't copy (global config) |
| `ducklake_files_scheduled_for_deletion` | Don't copy (start empty) |

## Cross-Catalog File Safety

### The Problem

When catalogs share physical files, cleanup operations could delete files other catalogs need.

### The Solution

With global counters, `data_file_id` uniquely identifies a physical file. Check by ID:

```sql
-- Before deleting physical file for data_file_id=3
SELECT COUNT(*) FROM ducklake_data_file WHERE data_file_id = 3 AND end_snapshot IS NULL;
-- If count > 1, another catalog still references this file
```

### Cleanup Code Changes

**`ducklake_cleanup_old_files`:**

Before `TryRemoveFile()`, check all catalogs:

```cpp
auto check_query = StringUtil::Format(
    "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_data_file "
    "WHERE data_file_id = %llu AND end_snapshot IS NULL",
    file.data_file_id);
auto result = transaction.Query(check_query);
if (result->GetValue<int64_t>(0, 0) > 0) {
    // Another catalog still references this file - skip deletion
    continue;
}
fs.TryRemoveFile(file.path);
```

### Orphan File Detection

Orphan detection should only delete files not referenced by ANY catalog:

```sql
SELECT filename FROM read_blob({DATA_PATH} || '**')
WHERE filename NOT IN (
    SELECT path FROM ducklake_data_file WHERE end_snapshot IS NULL
    UNION ALL
    SELECT path FROM ducklake_delete_file WHERE end_snapshot IS NULL
)
```

## Query Changes

### JOINs Must Include catalog_id

```sql
-- Before (assumed global uniqueness):
SELECT * FROM ducklake_data_file df
JOIN ducklake_table t ON df.table_id = t.table_id

-- After (explicit catalog scope):
SELECT * FROM ducklake_data_file df
JOIN ducklake_table t ON df.catalog_id = t.catalog_id AND df.table_id = t.table_id
```

### Snapshot Queries Change

```sql
-- Before (per-catalog):
SELECT * FROM ducklake_snapshot WHERE catalog_id = {CATALOG_ID} ORDER BY snapshot_id DESC LIMIT 1

-- After (global):
SELECT * FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1
```

## Code References

### Schema Definitions
- `src/storage/ducklake_metadata_manager.cpp:66-91` - Table CREATE statements

### DuckLakeSnapshot Struct

From `src/include/common/ducklake_snapshot.hpp:15-30`:
```cpp
struct DuckLakeSnapshot {
    idx_t snapshot_id;
    idx_t schema_version;
    idx_t next_catalog_id;  // Global counter for entity IDs
    idx_t next_file_id;     // Global counter for file IDs
};
```

### Current Snapshot Query (Needs Change)

From `src/storage/ducklake_metadata_manager.cpp:2654`:
```sql
-- Current (per-catalog):
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM ducklake_snapshot
WHERE catalog_id = {CATALOG_ID}
AND snapshot_id = (SELECT MAX(snapshot_id) FROM ducklake_snapshot WHERE catalog_id = {CATALOG_ID});

-- New (global):
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM ducklake_snapshot
ORDER BY snapshot_id DESC LIMIT 1;
```

### Counter Usage
- `src/storage/ducklake_transaction.cpp:685` - `next_catalog_id++` for schema_id
- `src/storage/ducklake_transaction.cpp:784` - `next_catalog_id++` for table_id
- `src/storage/ducklake_transaction.cpp:956` - `next_catalog_id++` for view_id
- `src/storage/ducklake_transaction.cpp:977` - `next_catalog_id++` for macro_id
- `src/storage/ducklake_transaction.cpp:1162` - `next_file_id++` for data_file_id
- `src/storage/ducklake_transaction.cpp:1336` - `next_file_id++` for delete_file_id
- `src/storage/ducklake_transaction.cpp:1379` - `next_file_id++` for mapping_id

### Cleanup Operations
- `src/storage/ducklake_metadata_manager.cpp:2975-2998` - GetOldFilesForCleanup
- `src/storage/ducklake_metadata_manager.cpp:3000-3055` - GetOrphanFilesForCleanup
- `src/functions/ducklake_cleanup_files.cpp:125-142` - DuckLakeCleanupExecute

## Files to Modify

| File | Changes |
|------|---------|
| `src/storage/ducklake_metadata_manager.cpp:66-91` | Remove catalog_id from snapshot tables, add to catalog, change PKs |
| `src/storage/ducklake_metadata_manager.cpp:272-334` | CreateCatalog - create versioned catalog entry |
| `src/storage/ducklake_metadata_manager.cpp:2654` | GetSnapshot - query global snapshot |
| `src/storage/ducklake_transaction.cpp` | Commit creates global snapshot |
| `src/storage/ducklake_metadata_manager.cpp` | All queries - add catalog_id to JOINs |
| `src/functions/ducklake_cleanup_files.cpp` | Add cross-catalog ID check before TryRemoveFile |
| `src/functions/ducklake_fork_catalog.cpp` | New file - fork function implementation |
| `src/ducklake_extension.cpp` | Register ducklake_fork_catalog function |

## Schema Management

### Design Decision: SQL Files Over C++ Migrations

**We do not use C++ migration code.** Schema is managed via SQL files, not embedded C++ strings.

**Why:**

1. **Primary keys cannot be altered in migrations** - The v05 schema changes primary keys on 20+ tables. You can't `ALTER` a primary key; you must recreate the table. This makes migration from v04→v05 impractical.

2. **Migrations miss indexes and constraints** - C++ migration code typically only does bare `CREATE TABLE` and `ALTER TABLE`. It doesn't include indexes, partial indexes, or database-specific optimizations.

3. **SQL files are readable and auditable** - DBAs can review `schema/postgresql.sql` directly. No need to parse C++ strings.

4. **Separation of concerns** - Schema creation is a deployment/ops concern, not an extension runtime concern.

### Schema Files

```
schema/
├── postgresql.sql   # Full schema for PostgreSQL metadata backend
└── duckdb.sql       # Full schema for DuckDB file metadata backend
```

These files contain:
- All `CREATE TABLE` statements with correct primary keys
- All indexes (including partial indexes like `WHERE end_snapshot IS NULL`)
- Version metadata insert (`0.5-dev1`)
- PostgreSQL-specific syntax (`gen_random_uuid()`) vs DuckDB syntax (`UUID()`)

### Deployment Model

**For PostgreSQL (production):**
```bash
psql -d your_database -f schema/postgresql.sql
```
Or use your migration tool (Flyway, Alembic, etc.) to run it as a versioned migration.

**For DuckDB (development/testing):**
```bash
duckdb your_database.db < schema/duckdb.sql
```
Or run via the test harness before tests.

**The C++ extension:**
- Does NOT create schema
- Does NOT run migrations
- Only checks version: if `ducklake_metadata.version != '0.5-dev1'`, fail with error
- Assumes schema exists and is correct

### Version Check

```cpp
void DuckLakeMetadataManager::ValidateVersion() {
    auto result = transaction.Query("SELECT value FROM ducklake_metadata WHERE key = 'version'");
    if (result->HasError() || result->RowCount() == 0) {
        throw IOException("DuckLake metadata not found. Run schema/postgresql.sql or schema/duckdb.sql first.");
    }
    auto version = result->GetValue(0, 0).ToString();
    if (version != "0.5-dev1") {
        throw IOException("Unsupported DuckLake version: " + version + ". Expected 0.5-dev1. "
                          "This version requires a fresh schema. Migration from older versions is not supported.");
    }
}
```

### No Migration Path

**v04 → v05 migration is not supported.** The schema changes are too significant:
- Primary key changes on 20+ tables
- Global snapshots (restructured)
- New composite keys everywhere

Users with v04 databases must recreate with the v05 schema. For our internal use case, this is acceptable since we control all deployments.

### C++ Code Cleanup

Remove from `src/storage/ducklake_metadata_manager.cpp`:
- All `CREATE TABLE` statements embedded in C++ strings
- `MigrateV01()`, `MigrateV02()`, `MigrateV03()`, `MigrateV04()`
- Schema creation in `Initialize()`

Keep:
- Query execution infrastructure
- Version validation
- All other metadata operations

### Test Harness Changes

**Delete:**
- `test/sql/migration/` directory
- `test/data/old_ducklake/` directory (v01.db.gz, v02.db.gz, etc.)

**Update:**
- Test harness initializes fresh databases using `schema/duckdb.sql`
- All 250+ non-migration tests continue to work (they create fresh DBs)

### Trade-offs

| What we lose | What we gain |
|--------------|--------------|
| Migration from v01/v02/v03/v04 | Clean schema in SQL files |
| Backwards compatibility | Proper indexes and constraints |
| Auto-schema-creation | Simpler C++ code |
| | DBA-readable schema |
| | Separation of concerns |

For our internal fork with controlled deployments, this is the right trade-off.

## Testing Requirements

### Unit Tests

1. Global snapshot creation on commit
2. Counter allocation is globally unique
3. Fork creates all metadata correctly
4. Forked catalog can read parent's files
5. Fork's new files get globally unique IDs
6. Cross-catalog file check prevents deletion of shared files

### Integration Tests

1. Multiple catalogs committing concurrently get unique snapshot IDs
2. Fork can read parent's data
3. Fork writes don't affect parent
4. Parent writes don't affect fork
5. Cleanup in fork doesn't delete parent's files
6. DROP TABLE in fork doesn't delete shared files
7. Time travel works at global level
8. Concurrent forks from same parent work correctly

### Stress Tests

1. Create 1000 forks from single parent
2. Concurrent commits from many catalogs (snapshot sequencing)
3. Concurrent reads across all forks
4. Memory and connection handling at scale

## Scalability Analysis

### Scenarios Where This Design Excels

#### 1. Immutable Shared Data + Many Readers (⭐⭐⭐⭐⭐)

**The sweet spot.** This is the hyper-tenancy use case:
- Parent catalog has large, immutable datasets
- Millions of forks read from shared data
- Each fork writes only to its own `DATA_PATH`
- Forks are ephemeral (created, used, deleted)

**Why it scales:**
- Fork is O(metadata rows), not O(data size)
- Shared parquet files = no storage amplification
- Immutable data = no `end_snapshot` updates = minimal write amplification
- Each fork's writes are isolated = no cross-catalog coordination for writes
- Global snapshots are infrequent (parent rarely changes)

**Concrete numbers:**
```
Parent: 1TB data, 10,000 tables, 100,000 files
Fork metadata: ~500KB per fork (just row copies)
1 million forks: ~500GB metadata (PostgreSQL can handle this)
Storage: Still 1TB (shared files) + fork-specific writes
```

#### 2. High Read Concurrency Across Catalogs (⭐⭐⭐⭐⭐)

**Why it scales:**
- Reads don't create snapshots
- Each catalog reads its own view via `WHERE catalog_id = ?`
- PostgreSQL indexes on `catalog_id` make lookups O(log n)
- No cross-catalog coordination for reads

**Bottleneck:** PostgreSQL connection pooling at extreme scale (100,000+ concurrent readers)

#### 3. Append-Only Workloads (⭐⭐⭐⭐⭐)

**Why it scales:**
- INSERT creates files + metadata rows with `begin_snapshot`
- No `end_snapshot` updates needed (immutable data)
- Global snapshots only increment a counter
- Each catalog's writes are independent

**Perfect for:** Event logs, audit trails, time-series data, agent activity logs

#### 4. Fork-Per-Request Pattern (⭐⭐⭐⭐)

**Pattern:** Spin up agent → fork catalog → do work → maybe persist → delete fork

**Why it scales:**
- Fork creation: ~10-100ms (metadata INSERT only)
- Work: isolated to fork's catalog_id
- Deletion: mark catalog with `end_snapshot`, files cleaned up later
- No coordination between concurrent forks

**Limitation:** If forks frequently commit, global snapshot contention increases

---

### Scenarios With Limitations

#### 1. High Commit Concurrency Across Many Catalogs (⭐⭐⭐)

**Important distinction:** The bottleneck is **commits**, not writes.

```
Within a transaction (no contention):
  Agent A: INSERT 1000 rows, UPDATE 500 rows, DELETE 200 rows
  Agent B: INSERT 5000 rows
  Agent C: INSERT 1 row
  -- All happening in parallel, no coordination needed

At commit time (global coordination):
  Agent A commits → snapshot 51
  Agent B commits → snapshot 52  (must wait for 51)
  Agent C commits → snapshot 53  (must wait for 52)
```

Writes within a transaction are buffered locally. The global snapshot is only created when the transaction commits. So an agent can do unlimited INSERTs, UPDATEs, DELETEs within a transaction without any cross-catalog coordination.

**Bottleneck:** Commits per second across all catalogs (snapshot sequencing)

**Typical patterns and their commit load:**

| Pattern | Commits | Impact |
|---------|---------|--------|
| Fork → read-only work → delete | 1 (fork creation) | Minimal |
| Fork → batch work → commit → delete | 2 (fork + final) | Minimal |
| Fork → commit after every operation | Many | Potential issue |

**Mitigation strategies:**
1. **Batch commits:** Catalogs batch changes, commit less frequently
2. **Async commit:** Return to user before global snapshot finalized
3. **Optimistic concurrency:** Allow parallel snapshot allocation, resolve conflicts

**Concrete impact:**
```
1,000 catalogs committing 1/second each = 1,000 snapshots/second
PostgreSQL can handle this with proper indexing
10,000 catalogs committing 1/second = potential bottleneck
```

**For hyper-tenancy use case:** Usually fine because:
- Parent commits rarely (data is stable)
- Forks often read-only or short-lived
- Fork writes are batched into single commit before fork deletion

#### 2. Frequent Schema Evolution in Parent (⭐⭐)

**The challenge:** Parent schema changes require fork updates

**Scenario:** Parent adds column to table. Forks that reference that table are now out of sync.

**Options:**
1. Forks get frozen snapshot (don't see parent changes) - **current design**
2. Forks inherit parent changes automatically - **complex, requires foreign keys across catalog_id**
3. Manual re-fork to pick up changes

**For hyper-tenancy:** Usually fine because parent is immutable training data, rarely changes schema

#### 3. Cross-Catalog Queries (⭐⭐)

**The challenge:** Joining data across catalogs

```sql
SELECT * FROM fork_a.table1 f
JOIN parent.table1 p ON f.id = p.id;
```

**Current behavior:** Works but requires two catalog attachments

**Limitation:** No cross-catalog transactions (each catalog has its own snapshot)

**For hyper-tenancy:** Usually fine - agents typically work within their own fork

#### 4. Very Large Forks (⭐⭐)

**The challenge:** Fork metadata size

**Scenario:** Parent has 1M files, fork copies all metadata rows

```
1M files × 20 columns × 50 bytes avg = ~1GB metadata per fork
1,000 forks = 1TB metadata
```

**Mitigations:**
1. **Lazy fork:** Only copy metadata on first access/modification
2. **Reference-based fork:** Fork stores "inherits from parent at snapshot X"
3. **Sparse fork:** Only copy modified rows, inherit others

**Trade-off:** Current design (full copy) is simpler but has higher metadata overhead

#### 5. Long-Running Time Travel Queries (⭐⭐⭐)

**The challenge:** Global snapshots mean more history to traverse

```sql
SELECT * FROM table AT SNAPSHOT 100;
-- Must check: begin_snapshot <= 100 AND (end_snapshot IS NULL OR end_snapshot > 100)
```

**With millions of catalogs:** Snapshot table gets very large

**Mitigations:**
1. **Index on (catalog_id, begin_snapshot, end_snapshot)**
2. **Partition snapshot tables by catalog_id**
3. **Snapshot expiration per catalog**

---

### PostgreSQL Scalability Considerations

#### Table Sizes at Scale

| Catalogs | Tables/Cat | Files/Table | `ducklake_data_file` rows |
|----------|------------|-------------|---------------------------|
| 1,000 | 100 | 100 | 10M |
| 10,000 | 100 | 100 | 100M |
| 100,000 | 100 | 100 | 1B |
| 1,000,000 | 10 | 10 | 100M |

**PostgreSQL limits:**
- Single table: ~32TB
- Row count: Billions OK with proper indexing
- Connection count: 10,000 max (use pgBouncer for more)

#### Required Indexes

```sql
-- Critical for catalog isolation
CREATE INDEX idx_data_file_catalog ON ducklake_data_file(catalog_id);
CREATE INDEX idx_table_catalog ON ducklake_table(catalog_id);
CREATE INDEX idx_schema_catalog ON ducklake_schema(catalog_id);
CREATE INDEX idx_snapshot_global ON ducklake_snapshot(snapshot_id DESC);

-- Critical for time travel
CREATE INDEX idx_data_file_snapshot ON ducklake_data_file(catalog_id, begin_snapshot, end_snapshot);

-- Critical for cross-catalog file safety
CREATE INDEX idx_data_file_id ON ducklake_data_file(data_file_id);
```

#### Partitioning Strategy

For extreme scale (100,000+ catalogs):

```sql
-- Partition by catalog_id range
CREATE TABLE ducklake_data_file (
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    ...
) PARTITION BY RANGE (catalog_id);

CREATE TABLE ducklake_data_file_0_10000 PARTITION OF ducklake_data_file
    FOR VALUES FROM (0) TO (10000);
-- etc.
```

---

### Comparison to Alternative Designs

#### Alternative 1: Per-Catalog Snapshots (Current DuckLake)

```
Catalog A: snapshot 1, 2, 3, 4
Catalog B: snapshot 1, 2, 3
```

**Pros:**
- No global coordination
- Simpler snapshot sequencing

**Cons:**
- Counter collision across catalogs (the bug we found)
- Fork requires ID remapping
- Cross-catalog file safety needs path comparison

**Verdict:** Doesn't work for forking

#### Alternative 2: Reference-Based Forking

```sql
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT,
    parent_catalog_id BIGINT,      -- Fork reference
    fork_snapshot BIGINT,          -- Snapshot forked from
    ...
);
```

Fork doesn't copy rows - queries resolve parent at runtime:
```sql
SELECT * FROM ducklake_data_file WHERE catalog_id = ?
UNION ALL
SELECT * FROM ducklake_data_file WHERE catalog_id = parent_id AND begin_snapshot <= fork_snapshot;
```

**Pros:**
- O(1) fork creation
- No metadata duplication

**Cons:**
- Every query more complex
- Deep fork chains = slow queries
- Parent deletion complex

**Verdict:** Better for metadata size, worse for query performance

#### Alternative 3: Copy-on-Write Fork

Fork inherits parent's metadata. On first modification, copy relevant portion:
```
Fork created: empty metadata, reference to parent
Fork reads table X: read from parent
Fork modifies table X: copy table X's rows, then modify
```

**Pros:**
- Fast fork
- Only copy what's needed
- Works well for sparse modifications

**Cons:**
- Complex read path (check fork, then parent)
- Partial copies = complex consistency

**Verdict:** Good middle ground, could be future optimization

---

### Recommendations for Hyper-Tenancy

#### Configuration for Millions of Catalogs

```sql
-- PostgreSQL settings
max_connections = 500  -- Use pgBouncer for pooling
shared_buffers = 16GB
effective_cache_size = 48GB
work_mem = 256MB
maintenance_work_mem = 2GB

-- Partitioning
-- Partition tables by catalog_id ranges

-- Connection pooling
-- Use pgBouncer in transaction mode
```

#### Operational Patterns

1. **Cleanup frequently:** Run `expire_snapshots` and `cleanup_old_files` per-catalog
2. **Monitor snapshot table:** Track growth, partition if needed
3. **Index maintenance:** REINDEX periodically on large tables
4. **Batch fork deletion:** Mark many forks for deletion, clean up in batch

#### When to Use This Design

| Use Case | Fit |
|----------|-----|
| AI agent per-request isolation | ⭐⭐⭐⭐⭐ |
| Shared training data + agent workspaces | ⭐⭐⭐⭐⭐ |
| Multi-tenant SaaS with tenant-isolated data | ⭐⭐⭐⭐ |
| Branching for experimentation | ⭐⭐⭐⭐ |
| High-throughput OLTP across many tenants | ⭐⭐⭐ |
| Collaborative editing (many writers) | ⭐⭐ |

---

### Key Trade-offs Made

#### What We Pay (Infrastructure Complexity)

| Cost | Why We Pay It |
|------|---------------|
| Snapshot rows we don't read for time travel | Every commit creates one for bookkeeping (counters, conflict detection) |
| Global snapshot sequencing | To keep counters globally unique across catalogs |
| Metadata duplication on fork | Full copy for simple queries, no chain traversal |
| Composite key complexity | To allow same IDs across catalogs (fork preserves lineage) |
| No migration from v04 | PK changes make migration impractical |
| Schema managed externally | SQL files instead of embedded C++ strings |

#### What Our Users Get

| Benefit | What It Means |
|---------|---------------|
| Instant workspace provisioning | Fork a 1TB catalog in milliseconds, not hours |
| No storage duplication | Million agents share the same parquet files |
| Complete isolation | Agent A can't see or affect Agent B's work |
| Safe cleanup | Delete a fork without breaking others |
| Simple mental model | "My workspace" just works, no visible complexity |
| Predictable performance | No chain traversal, no lazy loading surprises |

#### The Deal

We eat the infrastructure complexity (snapshots, global coordination, metadata overhead) so our users get instant, isolated, safe workspaces that "just work."

**They see:** "I have my own database."
**We see:** Millions of catalogs sharing petabytes of parquet files in one metadata store.

#### What We Don't Care About

| Feature | Status | Why |
|---------|--------|-----|
| Time travel | Not a priority | Snapshots exist for bookkeeping, not user-facing time travel |
| Migration from old versions | Not supported | Internal fork, we control all deployments |
| Upstream DuckLake compatibility | Breaking | Our schema diverges significantly |

---

### Horizontal Scaling

If one PostgreSQL instance hits limits, shard by account:

```
Account A, B, C → Stamp 1 (PostgreSQL + S3 region 1)
Account D, E, F → Stamp 2 (PostgreSQL + S3 region 2)
...
```

Within each stamp:
- Global snapshots work fine
- Millions of catalogs supported
- Independent scaling per stamp

The forking model works identically on each stamp. Sharding is an ops concern, not a schema concern.

---

## Summary

The design combines:

1. **Global snapshots** - One sequence for entire metadata database, counters naturally global
2. **Versioned catalogs** - Catalogs have begin_snapshot/end_snapshot like other entities
3. **Composite primary keys** - Same ID can exist in multiple catalogs (for fork)
4. **Cross-catalog file safety** - Check by globally unique data_file_id

This enables:
- **Trivial fork** - Copy rows, change catalog_id, no remapping
- **Lineage preservation** - Fork's `data_file_id=3` is same file as parent's
- **Safe cleanup** - Global IDs enable reliable cross-catalog checks
- **Whole-database time travel** - See state of everything at any snapshot
- **Cheap snapshots** - For immutable data, just INSERTs with begin_snapshot
- **Millions of catalogs** - No ID collisions, natural global sequencing
