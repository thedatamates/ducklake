# DuckLake Fork - Implementation Guide

This document covers the internal implementation of the DuckLake fork. For usage and design, see [README.md](../README.md).

## Overview

This fork adds **catalog-based multi-tenant isolation** using a `catalog_id` column instead of PostgreSQL schema-per-tenant isolation. Key architectural decisions:

1. **Global snapshots** - One snapshot sequence for the entire metadata database
2. **Composite primary keys** - Entities scoped to `(catalog_id, entity_id)` for isolation
3. **Catalogs are versioned** - `ducklake_catalog` has `begin_snapshot/end_snapshot` like other entities
4. **Unified ID counter** - All entity IDs come from `next_catalog_id` on snapshots

---

## Core Design: Global Snapshots + Composite Keys

### Why Global Snapshots?

With per-catalog snapshots, each catalog has its own snapshot sequence:
```
Catalog A: snapshot 1, 2, 3, 4
Catalog B: snapshot 1, 2, 3
```

This causes problems for forking:
- Counter collision across catalogs (both might allocate `file_id=5`)
- Fork requires ID remapping
- Cross-catalog file safety needs path comparison

With global snapshots, one sequence for all catalogs:
```
Catalog A commits  → snapshot 51 (next_file_id=100)
Catalog B commits  → snapshot 52 (next_file_id=103)
Catalog A commits  → snapshot 53 (next_file_id=105)
Fork C created     → snapshot 54 (next_file_id=105)
```

Benefits:
- Counters are naturally global (no collision)
- Fork preserves IDs (same `data_file_id` = same physical file)
- Cross-catalog file check is trivial (`WHERE data_file_id = ?`)

### The Architecture

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

---

## Schema

### Global Tables (No catalog_id)

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
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);
```

### Catalog Table (Versioned)

```sql
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT,
    catalog_uuid UUID NOT NULL,
    catalog_name VARCHAR NOT NULL,
    begin_snapshot BIGINT,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, begin_snapshot)
);
```

### Entity Tables (With catalog_id + Composite Keys)

```sql
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
```

### All Tables with Composite Keys

| Table | Primary Key |
|-------|-------------|
| `ducklake_catalog` | `(catalog_id, begin_snapshot)` |
| `ducklake_schema` | `(catalog_id, schema_id)` |
| `ducklake_table` | `(catalog_id, table_id, begin_snapshot)` |
| `ducklake_view` | `(catalog_id, view_id, begin_snapshot)` |
| `ducklake_data_file` | `(catalog_id, data_file_id)` |
| `ducklake_delete_file` | `(catalog_id, delete_file_id)` |
| `ducklake_column` | `(catalog_id, table_id, column_id, begin_snapshot)` |
| `ducklake_table_stats` | `(catalog_id, table_id)` |
| `ducklake_partition_info` | `(catalog_id, partition_id)` |
| `ducklake_schema_versions` | `(catalog_id, begin_snapshot)` |
| `ducklake_macro` | `(catalog_id, macro_id, begin_snapshot)` |
| `ducklake_macro_impl` | `(catalog_id, macro_id, impl_id)` |
| `ducklake_tag` | `(catalog_id, object_id, key, begin_snapshot)` |
| `ducklake_column_tag` | `(catalog_id, table_id, column_id, key, begin_snapshot)` |
| `ducklake_inlined_data_tables` | `(catalog_id, table_id)` |

---

## Implementation Details

### Query Placeholder Replacement

All queries use placeholders that are replaced at runtime in `DuckLakeTransaction::Execute()`:

| Placeholder | Replacement | Example |
|-------------|-------------|---------|
| `{CATALOG_ID}` | Numeric catalog ID (BIGINT) | `0`, `5`, `123` |
| `{CATALOG_NAME}` | SQL-quoted catalog name | `'my-catalog'` |
| `{METADATA_CATALOG}` | Schema prefix for metadata | `__ducklake_meta_lake.public` |
| `{SNAPSHOT_ID}` | Current snapshot ID | `42` |
| `{NEXT_CATALOG_ID}` | Next entity ID counter | `15` |
| `{NEXT_FILE_ID}` | Next file ID counter | `100` |

Example query before replacement:
```sql
SELECT * FROM {METADATA_CATALOG}.ducklake_table
WHERE catalog_id = {CATALOG_ID} AND end_snapshot IS NULL
```

After replacement:
```sql
SELECT * FROM __ducklake_meta_lake.public.ducklake_table
WHERE catalog_id = 0 AND end_snapshot IS NULL
```

### Catalog Lookup and Creation

When attaching to DuckLake, the system looks up or creates the catalog:

```cpp
auto catalog_id = metadata_manager.LookupCatalogByName(options.catalog_name);
if (catalog_id.IsValid()) {
    options.catalog_id = catalog_id.GetIndex();
} else if (options.create_if_not_exists) {
    options.catalog_id = metadata_manager.CreateCatalog(options.catalog_name);
} else {
    throw InvalidInputException("Catalog '%s' does not exist", options.catalog_name);
}
```

**CreateCatalog steps:**
1. Get next available ID from `MAX(next_catalog_id)` across all snapshots
2. Assign `new_catalog_id` from that counter
3. Assign `main_schema_id = new_catalog_id + 1`
4. Set `next_catalog_id = new_catalog_id + 2` for future entities
5. Compute next available `snapshot_id` (MAX + 1)
6. Compute next available `file_id` to avoid conflicts
7. Insert into `ducklake_catalog` with `begin_snapshot`
8. Insert initial 'main' schema into `ducklake_schema`
9. Insert initial snapshot into `ducklake_snapshot`
10. Insert snapshot changes into `ducklake_snapshot_changes`
11. Insert schema version into `ducklake_schema_versions`

### Tables That NEED catalog_id Filtering

These tables are queried in bulk without foreign key filtering:

| Table | Reason |
|-------|--------|
| `ducklake_schema` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_table` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_view` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_macro` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_partition_info` | Loaded in bulk by GetCatalogForSnapshot |
| `ducklake_schema_versions` | Loaded for schema version tracking |
| `ducklake_table_stats` | Queried without FK filter |
| `ducklake_files_scheduled_for_deletion` | Queried without FK filter |

### Tables That DON'T Need catalog_id in WHERE

These tables are always queried with a foreign key filter and inherit isolation:

| Table | Filtered By |
|-------|-------------|
| `ducklake_column` | `table_id` |
| `ducklake_data_file` | `table_id` |
| `ducklake_delete_file` | `table_id` |
| `ducklake_file_column_stats` | `data_file_id` |
| `ducklake_table_column_stats` | `table_id` (joined with ducklake_table_stats) |
| `ducklake_partition_column` | `partition_id` |
| `ducklake_file_partition_value` | `data_file_id` |
| `ducklake_macro_impl` | `macro_id` |
| `ducklake_macro_parameters` | `macro_id` |

**Note:** These tables still HAVE `catalog_id` for composite primary keys and JOIN conditions, but queries don't need explicit `WHERE catalog_id = ?` because they're already filtered by FK.

### JOINs Must Include catalog_id

```sql
-- WRONG (assumes global uniqueness):
SELECT * FROM ducklake_data_file df
JOIN ducklake_table t ON df.table_id = t.table_id

-- CORRECT (explicit catalog scope):
SELECT * FROM ducklake_data_file df
JOIN ducklake_table t ON df.catalog_id = t.catalog_id AND df.table_id = t.table_id
```

---

## Snapshot and Counter Behavior

### How Snapshots Are Created

Snapshots are automatic. Every transaction commit that makes changes creates a new snapshot:

```
DuckLakeTransaction::Commit()
  └── if ChangesMade():
        └── FlushChanges()
              └── snapshot_id++
              └── INSERT INTO ducklake_snapshot (...)
```

**What counts as "changes made":**
- Schema changes: CREATE/DROP TABLE, CREATE/DROP SCHEMA, CREATE/DROP VIEW, CREATE/DROP MACRO
- Data changes: INSERT, UPDATE, DELETE
- Dropped files
- New name maps

**Read-only transactions don't create snapshots.**

### Transaction Batching

DuckDB uses auto-commit by default:
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

### Counter Allocation

To allocate an ID:

```cpp
auto result = transaction.Query(
    "SELECT next_file_id FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1");
idx_t next_id = result->GetValue<idx_t>(0, 0);

data_file.id = DataFileIndex(next_id);

// At commit, create new snapshot with incremented counter
INSERT INTO ducklake_snapshot (snapshot_id, ..., next_file_id)
VALUES (new_snapshot_id, ..., next_id + files_created);
```

### Initial Snapshot Sequence

```
Snapshot 0: Initial empty state
Snapshot 1: CreateCatalog (creates 'test' catalog with 'main' schema)
Snapshot 2: First user operation (CREATE TABLE)
Snapshot 3: Next operation (INSERT)
...
```

---

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
  AND end_snapshot IS NULL;

INSERT INTO ducklake_data_file (catalog_id, data_file_id, ..., begin_snapshot, end_snapshot)
SELECT 5, data_file_id, ..., 54, NULL
FROM ducklake_data_file
WHERE catalog_id = parent_catalog_id
  AND end_snapshot IS NULL;

-- ... same for all entity tables
```

**No ID remapping.** IDs preserve lineage. Fork's `data_file_id=3` is the same file as parent's `data_file_id=3`.

### Tables to Copy During Fork

| Table | Copy Strategy |
|-------|---------------|
| `ducklake_catalog` | Create new (don't copy) |
| `ducklake_schema` | Copy current rows |
| `ducklake_schema_versions` | Copy current rows |
| `ducklake_table` | Copy current rows |
| `ducklake_view` | Copy current rows |
| `ducklake_column` | Copy current rows |
| `ducklake_data_file` | Copy current rows |
| `ducklake_delete_file` | Copy current rows |
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

---

## Cross-Catalog File Safety

### The Problem

When catalogs share physical files (via forking), cleanup operations could delete files other catalogs need.

### The Solution

With global counters, `data_file_id` uniquely identifies a physical file. Check by ID:

```sql
-- Before deleting physical file for data_file_id=3
SELECT COUNT(*) FROM ducklake_data_file WHERE data_file_id = 3 AND end_snapshot IS NULL;
-- If count > 1, another catalog still references this file
```

### Cleanup Code

Before `TryRemoveFile()`:

```cpp
auto check_query = StringUtil::Format(
    "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_data_file "
    "WHERE data_file_id = %llu AND end_snapshot IS NULL",
    file.data_file_id);
auto result = transaction.Query(check_query);
if (result->GetValue<int64_t>(0, 0) > 0) {
    continue;  // Another catalog still references this file
}
fs.TryRemoveFile(file.path);
```

---

## Path Storage Strategy

Paths are stored **relative to `DATA_PATH`** (not `effective_data_path`), including the catalog subdirectory:

```
DATA_PATH = /data/
CATALOG = parent
effective_data_path = /data/parent/  (used for writing files)

File written to: /data/parent/main/users/file.parquet
Path stored:     parent/main/users/file.parquet  (relative to DATA_PATH)
Path resolved:   DATA_PATH + stored_path = /data/parent/main/users/file.parquet
```

**Why this matters for forking:**

When a catalog is forked, the stored paths don't need conversion:

```
Parent (CATALOG=parent):  /data/ + parent/main/users/file.parquet
Fork (CATALOG=fork):      /data/ + parent/main/users/file.parquet  ← Same!
```

Fork's NEW files go to a different location:
```
Fork writes to:  /data/fork/main/users/newfile.parquet
Path stored:     fork/main/users/newfile.parquet
```

Implemented in `GetRelativePath()` and `FromRelativePath()` using `BaseDataPath()`.

---

## Schema Management

### SQL Files Over C++ Migrations

Schema is managed via SQL files, not embedded C++ strings.

**Why:**
1. Primary keys cannot be altered in migrations
2. Migrations miss indexes and constraints
3. SQL files are readable and auditable
4. Separation of concerns

### Schema Files

```
schema/
├── postgresql.sql   # Full schema for PostgreSQL metadata backend
└── duckdb.sql       # Full schema for DuckDB file metadata backend
```

### Deployment

**For PostgreSQL (production):**
```bash
psql -d your_database -f schema/postgresql.sql
```

**For DuckDB (development/testing):**
The DuckDB backend auto-creates schema with `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS`.

### METADATA_SCHEMA Auto-Creation (DuckDB Only)

When using `METADATA_SCHEMA` option with DuckDB, the schema is auto-created:

```sql
ATTACH 'ducklake:shared.db' AS ducklake_1 (METADATA_SCHEMA 'metadata_s1', CATALOG 'cat1');
ATTACH 'ducklake:shared.db' AS ducklake_2 (METADATA_SCHEMA 'metadata_s2', CATALOG 'cat2');
```

For PostgreSQL, the schema must be pre-created via the SQL file.

---

## Direct PostgreSQL Access

With `catalog_id` column-based isolation, webapps can query metadata directly:

```sql
-- List catalogs
SELECT catalog_id, catalog_uuid, catalog_name, begin_snapshot
FROM ducklake.ducklake_catalog
WHERE end_snapshot IS NULL;

-- List tables for a catalog
SELECT t.table_name, t.table_uuid, s.schema_name
FROM ducklake.ducklake_table t
JOIN ducklake.ducklake_schema s ON t.catalog_id = s.catalog_id AND t.schema_id = s.schema_id
WHERE t.catalog_id = 0
  AND t.end_snapshot IS NULL
  AND s.end_snapshot IS NULL;

-- Lookup by catalog name
SELECT t.table_name, t.table_uuid, s.schema_name
FROM ducklake.ducklake_table t
JOIN ducklake.ducklake_schema s ON t.catalog_id = s.catalog_id AND t.schema_id = s.schema_id
JOIN ducklake.ducklake_catalog c ON t.catalog_id = c.catalog_id
WHERE c.catalog_name = 'my-catalog'
  AND c.end_snapshot IS NULL
  AND t.end_snapshot IS NULL
  AND s.end_snapshot IS NULL;
```

---

## Scalability Analysis

### Scenarios Where This Design Excels

#### Immutable Shared Data + Many Readers

The sweet spot for hyper-tenancy:
- Parent catalog has large, immutable datasets
- Millions of forks read from shared data
- Each fork writes only to its own `DATA_PATH`
- Forks are ephemeral (created, used, deleted)

```
Parent: 1TB data, 10,000 tables, 100,000 files
Fork metadata: ~500KB per fork (just row copies)
1 million forks: ~500GB metadata (PostgreSQL can handle this)
Storage: Still 1TB (shared files) + fork-specific writes
```

#### High Read Concurrency

- Reads don't create snapshots
- Each catalog reads its own view via `WHERE catalog_id = ?`
- PostgreSQL indexes on `catalog_id` make lookups O(log n)

#### Append-Only Workloads

- INSERT creates files + metadata rows with `begin_snapshot`
- No `end_snapshot` updates needed (immutable data)
- Each catalog's writes are independent

### Limitations

#### High Commit Concurrency

The bottleneck is **commits**, not writes:

```
Within a transaction (no contention):
  Agent A: INSERT 1000 rows, UPDATE 500 rows, DELETE 200 rows
  Agent B: INSERT 5000 rows
  -- All happening in parallel

At commit time (global coordination):
  Agent A commits → snapshot 51
  Agent B commits → snapshot 52  (must wait for 51)
```

**Mitigation:** Batch commits, use explicit transactions.

### PostgreSQL Configuration

```sql
-- Required indexes
CREATE INDEX idx_data_file_catalog ON ducklake_data_file(catalog_id);
CREATE INDEX idx_table_catalog ON ducklake_table(catalog_id);
CREATE INDEX idx_schema_catalog ON ducklake_schema(catalog_id);
CREATE INDEX idx_snapshot_global ON ducklake_snapshot(snapshot_id DESC);
CREATE INDEX idx_data_file_snapshot ON ducklake_data_file(catalog_id, begin_snapshot, end_snapshot);
CREATE INDEX idx_data_file_id ON ducklake_data_file(data_file_id);
```

For extreme scale (100,000+ catalogs), partition tables by `catalog_id` range.

---

## Test Adjustments for Global Snapshots

### Snapshot Numbering Change

With global snapshots, the initial sequence shifted:

| Snapshot | Old Behavior | New Behavior |
|----------|--------------|--------------|
| 0 | N/A | Initial empty state |
| 1 | First user operation | CreateCatalog |
| 2+ | Subsequent operations | User operations |

### Test Adjustment Patterns

| What | Adjustment | Reason |
|------|------------|--------|
| `snapshot_id` in SELECT results | +1 | Actual snapshot IDs shifted |
| `VERSION => N` in time travel | +1 | Data shifted |
| `versions => [N]` in expire/cleanup | +1 | Explicit snapshot ID references |
| `ducklake_table_insertions(..., start, end)` | +1 to end | Snapshot range parameters |
| Error messages with snapshot numbers | Update numbers | e.g., "version 10" → "version 11" |
| `partial_file_info` column values | +1 to snapshot parts | e.g., "3:100\|4:200" → "4:100\|5:200" |

---

## Code References

### Key Files

| File | Purpose |
|------|---------|
| `src/storage/ducklake_metadata_manager.cpp` | Schema, queries, CreateCatalog |
| `src/storage/ducklake_transaction.cpp` | Placeholder replacement, FlushChanges |
| `src/storage/ducklake_initializer.cpp` | Catalog lookup/creation on ATTACH |
| `src/functions/ducklake_fork_catalog.cpp` | Fork function implementation |
| `src/functions/ducklake_expire_snapshots.cpp` | Per-catalog snapshot expiration |

### DuckLakeSnapshot Struct

From `src/include/common/ducklake_snapshot.hpp`:
```cpp
struct DuckLakeSnapshot {
    idx_t snapshot_id;
    idx_t schema_version;
    idx_t next_catalog_id;  // Global counter for entity IDs
    idx_t next_file_id;     // Global counter for file IDs
};
```

### Counter Usage Locations

- `src/storage/ducklake_transaction.cpp:685` - `next_catalog_id++` for schema_id
- `src/storage/ducklake_transaction.cpp:784` - `next_catalog_id++` for table_id
- `src/storage/ducklake_transaction.cpp:956` - `next_catalog_id++` for view_id
- `src/storage/ducklake_transaction.cpp:977` - `next_catalog_id++` for macro_id
- `src/storage/ducklake_transaction.cpp:1162` - `next_file_id++` for data_file_id
- `src/storage/ducklake_transaction.cpp:1336` - `next_file_id++` for delete_file_id

---

## Version History

| Version | Changes |
|---------|---------|
| 0.5-dev1 | Global snapshots, composite keys, catalog forking |
| 0.4-dev1 | Added catalog-based multi-tenant isolation |
| 0.3 | Upstream DuckLake version |
