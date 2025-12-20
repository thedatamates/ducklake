# Catalog ID Column Reordering Plan

## Goal

Reorder the `catalog_id` column to appear BEFORE `schema_id` in all relevant tables, following the natural hierarchy:

```
catalog → schema → table → column/file/etc.
```

This makes the schema more intuitive and self-documenting.

## Current State

Currently, `catalog_id` appears at the END of each table definition because:
1. `ALTER TABLE ADD COLUMN` places new columns at the end
2. We matched `CREATE TABLE` statements to this behavior for consistency

**Tables with catalog_id (10 tables):**
- `ducklake_snapshot`
- `ducklake_snapshot_changes`
- `ducklake_schema`
- `ducklake_table`
- `ducklake_view`
- `ducklake_table_stats`
- `ducklake_partition_info`
- `ducklake_files_scheduled_for_deletion`
- `ducklake_schema_versions`
- `ducklake_macro`

## The Challenge

**Migrated databases** (via `MigrateV04`) will have `catalog_id` at the END of each table (ALTER TABLE behavior).

**New databases** (via `InitializeDuckLake`) will have `catalog_id` wherever we put it in CREATE TABLE.

If we put `catalog_id` at the BEGINNING in CREATE TABLE but it's at the END in migrated DBs, positional INSERTs will break.

## Solution: Explicit Column Names

Use explicit column names in ALL INSERT statements. This makes the physical column order irrelevant.

**Before (positional):**
```sql
INSERT INTO ducklake_schema VALUES (0, UUID(), 0, NULL, 'main', 'main/', true, '');
```

**After (explicit columns):**
```sql
INSERT INTO ducklake_schema (catalog_id, schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative)
VALUES ({CATALOG_ID}, 0, UUID(), 0, NULL, 'main', 'main/', true);
```

---

## Step-by-Step Implementation

### Step 1: Update CREATE TABLE Statements in InitializeDuckLake

File: `src/storage/ducklake_metadata_manager.cpp`
Function: `InitializeDuckLake` (around line 52-95)

For each of the 10 tables, move `catalog_id` to be the FIRST column:

#### 1.1 ducklake_snapshot
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(catalog_id VARCHAR DEFAULT '', snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
```

#### 1.2 ducklake_snapshot_changes
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot_changes(catalog_id VARCHAR DEFAULT '', snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
```

#### 1.3 ducklake_schema
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_schema(catalog_id VARCHAR DEFAULT '', schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
```

#### 1.4 ducklake_table
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_table(catalog_id VARCHAR DEFAULT '', table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
```

#### 1.5 ducklake_view
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_view(catalog_id VARCHAR DEFAULT '', view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR);
```

#### 1.6 ducklake_table_stats
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_table_stats(catalog_id VARCHAR DEFAULT '', table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);
```

#### 1.7 ducklake_partition_info
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_info(catalog_id VARCHAR DEFAULT '', partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
```

#### 1.8 ducklake_files_scheduled_for_deletion
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion(catalog_id VARCHAR DEFAULT '', data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ);
```

#### 1.9 ducklake_schema_versions
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_schema_versions(catalog_id VARCHAR DEFAULT '', begin_snapshot BIGINT, schema_version BIGINT);
```

#### 1.10 ducklake_macro
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, catalog_id VARCHAR DEFAULT '');

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_macro(catalog_id VARCHAR DEFAULT '', schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT);
```

---

### Step 2: Update INSERT Statements in InitializeDuckLake

Change from positional to explicit column names with catalog_id first:

#### 2.1 ducklake_schema_versions INSERT (line ~89)
```cpp
// FROM:
INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions VALUES (0, 0, '');

// TO:
INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions (catalog_id, begin_snapshot, schema_version) VALUES ({CATALOG_ID}, 0, 0);
```

#### 2.2 ducklake_snapshot INSERT (line ~90)
```cpp
// FROM:
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES (0, NOW(), 0, 1, 0, '');

// TO:
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) VALUES ({CATALOG_ID}, 0, NOW(), 0, 1, 0);
```

#### 2.3 ducklake_snapshot_changes INSERT (line ~91)
```cpp
// FROM:
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES (0, 'created_schema:"main"', NULL, NULL, NULL, '');

// TO:
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes (catalog_id, snapshot_id, changes_made, author, commit_message, commit_extra_info) VALUES ({CATALOG_ID}, 0, 'created_schema:"main"', NULL, NULL, NULL);
```

#### 2.4 ducklake_schema INSERT (line ~93)
```cpp
// FROM:
INSERT INTO {METADATA_CATALOG}.ducklake_schema VALUES (0, UUID(), 0, NULL, 'main', 'main/', true, '');

// TO:
INSERT INTO {METADATA_CATALOG}.ducklake_schema (catalog_id, schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) VALUES ({CATALOG_ID}, 0, UUID(), 0, NULL, 'main', 'main/', true);
```

---

### Step 3: Update All Dynamic INSERT Statements

Search for all `INSERT INTO {METADATA_CATALOG}` and convert to explicit column format.

Use grep to find all occurrences:
```bash
grep -n "INSERT INTO {METADATA_CATALOG}" src/storage/ducklake_metadata_manager.cpp
```

For each INSERT, add explicit column names with catalog_id first (where applicable).

#### Key locations to update:

| Line (approx) | Table | Function |
|---------------|-------|----------|
| 1544 | ducklake_schema | Schema insert |
| 1676 | ducklake_table | Table insert |
| 1744 | ducklake_macro | Macro insert |
| 1818 | ducklake_view | View insert |
| 2402 | ducklake_snapshot | Snapshot insert |
| 2416 | ducklake_snapshot_changes | Snapshot changes insert |
| 2652 | ducklake_partition_info | Partition info insert |
| 2798 | ducklake_table_stats | Table stats insert |
| 3005, 3056, 3204, 3272 | ducklake_files_scheduled_for_deletion | File deletion scheduling |
| 3339 | ducklake_schema_versions | Schema versions insert |

---

### Step 4: Update Test Files

Tests that query tables with `SELECT *` will see columns in different order. Update expected results.

#### Files to update:

**Macro tests** (ducklake_macro has catalog_id):
- `test/sql/macros/test_simple_macro.test`
- `test/sql/macros/test_default_parameter.test`
- `test/sql/macros/test_defined_types.test`
- `test/sql/macros/test_macro_tables.test`
- `test/sql/macros/test_multiple_implementations.test`
- `test/sql/macros/test_scalar_table_macros.test`
- `test/sql/macros/test_schema_dependency.test`

**Migration test:**
- `test/sql/migration/migration.test` (ducklake_schema_versions)

For each test, change expected results so `catalog_id` (shown as `(empty)`) is the FIRST column instead of the last.

**Example change:**
```
// FROM:
0	1	simple	1	NULL	(empty)

// TO:
(empty)	0	1	simple	1	NULL
```

---

### Step 5: Verify CTE in UpdateGlobalTableStats

The CTE in `UpdateGlobalTableStats` doesn't use catalog_id (ducklake_table_column_stats doesn't have it), so no changes needed there.

---

### Step 6: Build and Test

```bash
make release -j4
./build/release/test/unittest "*"
```

All 258 tests should pass.

---

## Verification Checklist

- [ ] All 10 CREATE TABLE statements have catalog_id as FIRST column
- [ ] All INSERT statements in InitializeDuckLake use explicit column names
- [ ] All dynamic INSERT statements use explicit column names
- [ ] MigrateV04 ALTER TABLE statements unchanged (they add to END, which is fine)
- [ ] All test expected results updated for new column order
- [ ] All tests pass

---

## Why This Works

1. **New databases**: CREATE TABLE puts catalog_id first, INSERTs use explicit columns → works
2. **Migrated databases**: ALTER TABLE puts catalog_id at end, but INSERTs use explicit columns → works
3. **SELECT * queries**: Will show different column order for new vs migrated, but functionality is identical
4. **Explicit column INSERTs**: Position-independent, works regardless of physical column order

---

## Files to Modify

1. `src/storage/ducklake_metadata_manager.cpp` - Main changes
2. `test/sql/macros/*.test` - 7 test files
3. `test/sql/migration/migration.test` - 1 test file

Total: ~9 files
