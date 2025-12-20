# Next Steps & System Documentation

## Current State

The CATALOG refactor is complete with the following implemented:
- `CATALOG 'name'` option replaces `CATALOG_ID 'string'`
- `catalog_id` is now BIGINT (not VARCHAR)
- `ducklake_catalog` table stores catalog metadata
- `next_catalog_id` renamed to `next_entry_id`
- All SELECT queries filter by `catalog_id`
- All 243 tests pass

## Critical Gap: Catalog Lookup Not Implemented

**The current implementation always uses `catalog_id = 0`.** There is no actual lookup of catalogs by name or auto-increment of catalog IDs.

### What's Missing

1. **No catalog lookup by name** - When attaching with `CATALOG 'my-catalog'`, we don't query `ducklake_catalog` to find the matching `catalog_id`

2. **No auto-increment for new catalogs** - New catalogs should get the next available `catalog_id`, not always 0

3. **No validation of existing catalogs** - We don't check if a catalog already exists or create it if it doesn't

4. **No cross-catalog support** - Can't have two catalogs (e.g., tenant_a and tenant_b) in the same PostgreSQL metadata database

### Current Behavior

```sql
-- Both of these get catalog_id = 0
ATTACH 'ducklake:postgres:...' AS tenant_a (CATALOG 'tenant_a');
ATTACH 'ducklake:postgres:...' AS tenant_b (CATALOG 'tenant_b');
-- They would see each other's data! (BUG)
```

---

## Next Steps (Priority Order)

### 1. Implement Catalog Lookup/Creation

**File:** `src/storage/ducklake_initializer.cpp`

On `LoadExistingDuckLake()`:
```cpp
// After migrations complete:
// 1. Look up catalog by name
auto result = transaction.Query(
    "SELECT catalog_id FROM {METADATA_CATALOG}.ducklake_catalog "
    "WHERE catalog_name = {CATALOG_NAME}");

if (result has rows) {
    options.catalog_id = result.GetValue<idx_t>(0);
} else if (options.create_if_not_exists) {
    // Get next catalog_id
    auto max_result = transaction.Query(
        "SELECT COALESCE(MAX(catalog_id), -1) + 1 FROM {METADATA_CATALOG}.ducklake_catalog");
    idx_t new_catalog_id = max_result.GetValue<idx_t>(0);

    // Insert new catalog
    transaction.Execute(
        "INSERT INTO {METADATA_CATALOG}.ducklake_catalog "
        "(catalog_id, catalog_name, created_snapshot) VALUES (?, ?, 0)",
        new_catalog_id, options.catalog_name);

    options.catalog_id = new_catalog_id;
} else {
    throw InvalidInputException("Catalog '%s' does not exist", options.catalog_name);
}
```

On `InitializeNewDuckLake()`:
- Already creates catalog with `{CATALOG_ID}` placeholder
- Need to set `options.catalog_id = 0` for the first catalog (or query for next ID)

### 2. Add Placeholder for CATALOG_NAME Lookup

**File:** `src/storage/ducklake_transaction.cpp`

Ensure `{CATALOG_NAME}` is properly SQL-quoted:
```cpp
auto catalog_name = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.CatalogName());
query = StringUtil::Replace(query, "{CATALOG_NAME}", catalog_name);
```

### 3. Test with PostgreSQL

Create integration test with actual PostgreSQL to verify multi-tenant isolation:
```sql
-- Terminal 1
ATTACH 'ducklake:postgres:host=localhost dbname=ducklake' AS tenant_a
    (CATALOG 'tenant_a', DATA_PATH '/data/');
CREATE TABLE tenant_a.main.users (id INT);
INSERT INTO tenant_a.main.users VALUES (1);

-- Terminal 2 (concurrent)
ATTACH 'ducklake:postgres:host=localhost dbname=ducklake' AS tenant_b
    (CATALOG 'tenant_b', DATA_PATH '/data/');
SELECT * FROM tenant_b.main.users;  -- Should error: table doesn't exist
```

### 4. Add Catalog Management Functions

```sql
-- List all catalogs in metadata database
SELECT * FROM ducklake_catalogs('my_attached_catalog');

-- Get current catalog info
SELECT ducklake_current_catalog();
```

### 5. Documentation Updates

- Update DuckDB extension docs with CATALOG option
- Add PostgreSQL multi-tenant setup guide
- Add migration guide from upstream DuckLake

---

## System Architecture

### Metadata Storage

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

### Data Storage

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

---

## Assumptions

### 1. Catalog Name Uniqueness
- Catalog names are unique within a metadata database
- Enforced by `UNIQUE` constraint on `ducklake_catalog.catalog_name`

### 2. Catalog ID Assignment
- First catalog gets `catalog_id = 0`
- Subsequent catalogs get incrementing IDs
- IDs are never reused (even after catalog deletion)

### 3. Data Path Isolation
- Each catalog's data is stored in `DATA_PATH/catalog_name/`
- Catalogs can share the same `DATA_PATH` base directory
- File paths are relative to the catalog's effective data path

### 4. Metadata Isolation
- All metadata queries filter by `catalog_id`
- Foreign key relationships inherit isolation from parent tables
- No cross-catalog joins in metadata queries

### 5. Snapshot Independence
- Each catalog has its own snapshot sequence
- `snapshot_id` is unique per catalog (not globally)
- Time travel operates within a single catalog

### 6. Schema/Table/View IDs
- IDs are generated from `next_entry_id` counter in `ducklake_snapshot`
- IDs are unique per catalog (not globally)
- Counter increments with each snapshot

### 7. File-Based DuckDB Limitation
- Cannot attach same DuckDB file as metadata for multiple catalogs
- DuckDB file locking prevents concurrent access
- Use PostgreSQL/MySQL for true multi-tenant shared metadata

---

## Operation Details

### ATTACH

**Syntax:**
```sql
ATTACH 'ducklake:<metadata_path>' AS <alias> (
    CATALOG '<catalog_name>',
    DATA_PATH '<base_path>',
    [other options...]
);
```

**Flow:**
1. Parse options, extract `catalog_name`
2. Attach internal metadata database (`__ducklake_metadata_<alias>`)
3. Check if DuckLake tables exist in metadata schema
4. If new: `InitializeNewDuckLake()`
   - Create all metadata tables
   - Insert into `ducklake_catalog` with new ID
   - Create default 'main' schema
5. If existing: `LoadExistingDuckLake()`
   - Run migrations if needed (MigrateV01 → V04)
   - Look up catalog by name (**TODO: not implemented**)
   - Load configuration from `ducklake_metadata`
6. Set `options.catalog_id` for query filtering
7. Compute `effective_data_path = DATA_PATH + catalog_name + /`

**Validation:**
- `CATALOG` option is required
- If `CREATE_IF_NOT_EXISTS = false`, catalog must exist
- If read-only mode, catalog must exist

### CREATE TABLE

**Syntax:**
```sql
CREATE TABLE [schema_name.]table_name (
    column_name type [constraints],
    ...
);
```

**Flow:**
1. Get next ID from `next_entry_id`
2. Insert into `ducklake_table` with `catalog_id`
3. Insert columns into `ducklake_column`
4. Create new snapshot with `schema_version` incremented
5. Record change in `ducklake_snapshot_changes`

**Metadata Written:**
```sql
INSERT INTO ducklake_table (catalog_id, table_id, table_uuid, begin_snapshot,
    end_snapshot, schema_id, table_name, path, path_is_relative)
VALUES ({CATALOG_ID}, ?, UUID(), ?, NULL, ?, ?, ?, true);

INSERT INTO ducklake_column (column_id, begin_snapshot, end_snapshot, table_id,
    column_order, column_name, column_type, ...)
VALUES (?, ?, NULL, ?, ?, ?, ?, ...);
```

### INSERT

**Syntax:**
```sql
INSERT INTO table_name [(columns)] VALUES (...) | SELECT ...;
```

**Flow:**
1. Resolve table from catalog (filtered by `catalog_id`)
2. Generate new snapshot ID
3. Write data to Parquet file(s) in `effective_data_path/schema/table/`
4. Insert file metadata into `ducklake_data_file`
5. Update `ducklake_table_stats`
6. Record snapshot with changes

**Data Inlining:**
- If row count ≤ `DATA_INLINING_ROW_LIMIT`, data stored in metadata
- Creates entry in `ducklake_inlined_data_tables`
- No Parquet file written

**Partitioning:**
- If table has partitions, files organized by partition values
- Partition values recorded in `ducklake_file_partition_value`

### SELECT

**Syntax:**
```sql
SELECT ... FROM [catalog.]schema.table WHERE ...;
```

**Flow:**
1. Look up table metadata (filtered by `catalog_id`)
2. Get active data files for current snapshot
3. Apply filter pushdown to file-level stats
4. Read matching Parquet files
5. Apply remaining filters

**Time Travel:**
```sql
SELECT * FROM table AT SNAPSHOT 5;
SELECT * FROM table AT TIMESTAMP '2024-01-01';
```

### UPDATE

**Syntax:**
```sql
UPDATE table SET column = value WHERE condition;
```

**Flow:**
1. Identify affected rows (by file)
2. For each affected file:
   - Read file, apply updates in memory
   - Write new file with updated data
   - Mark old file's `end_snapshot`
   - Insert new file record
3. Create delete file for old rows (if using delete files)
4. Update statistics
5. Create new snapshot

**Copy-on-Write:**
- DuckLake uses copy-on-write, not merge-on-read
- Updated files are rewritten completely
- Old files remain until snapshot expiration

### DELETE

**Syntax:**
```sql
DELETE FROM table WHERE condition;
```

**Flow:**
1. Identify affected rows (by file)
2. For fully deleted files: mark `end_snapshot`
3. For partially deleted files:
   - Create delete file listing deleted row IDs
   - Insert into `ducklake_delete_file`
4. Update statistics (record_count, etc.)
5. Create new snapshot

**Truncate:**
```sql
TRUNCATE TABLE table_name;
```
- Marks all files' `end_snapshot`
- Resets table statistics to zero
- Faster than DELETE (no row-by-row processing)

### ALTER TABLE

**Add Column:**
```sql
ALTER TABLE table ADD COLUMN name type [DEFAULT value];
```
- Inserts new column in `ducklake_column`
- Existing files remain unchanged (NULL for new column)
- New writes include new column

**Drop Column:**
```sql
ALTER TABLE table DROP COLUMN name;
```
- Sets column's `end_snapshot`
- Existing files remain unchanged
- Column excluded from future reads

**Rename Column:**
```sql
ALTER TABLE table RENAME COLUMN old TO new;
```
- Updates column metadata
- Files unchanged (column referenced by ID internally)

**Type Promotion:**
```sql
ALTER TABLE table ALTER COLUMN name SET DATA TYPE new_type;
```
- Only safe promotions allowed (INT → BIGINT, etc.)
- Files unchanged, type cast on read

### DROP TABLE

**Syntax:**
```sql
DROP TABLE [IF EXISTS] table_name;
```

**Flow:**
1. Set table's `end_snapshot` in `ducklake_table`
2. Set all columns' `end_snapshot`
3. Files NOT immediately deleted
4. Files scheduled for deletion after snapshot expiration
5. Create new snapshot

**File Cleanup:**
- Files remain until `ducklake_expire_snapshots()` called
- Expired files moved to `ducklake_files_scheduled_for_deletion`
- `ducklake_cleanup_old_files()` actually deletes files

### CREATE VIEW

**Syntax:**
```sql
CREATE VIEW view_name AS SELECT ...;
```

**Flow:**
1. Get next ID from `next_entry_id`
2. Store SQL definition in `ducklake_view`
3. Column aliases stored for result column names
4. Create new snapshot

**Storage:**
```sql
INSERT INTO ducklake_view (catalog_id, view_id, view_uuid, begin_snapshot,
    end_snapshot, schema_id, view_name, dialect, sql, column_aliases)
VALUES ({CATALOG_ID}, ?, UUID(), ?, NULL, ?, ?, 'duckdb', ?, ?);
```

### CREATE MACRO

**Scalar Macro:**
```sql
CREATE MACRO add_one(x) AS x + 1;
```

**Table Macro:**
```sql
CREATE MACRO filtered_users(min_age) AS TABLE
    SELECT * FROM users WHERE age >= min_age;
```

**Flow:**
1. Get next ID for macro
2. Insert into `ducklake_macro` with `catalog_id`
3. Insert implementation into `ducklake_macro_impl`
4. Insert parameters into `ducklake_macro_parameters`
5. Create new snapshot

### Time Travel

**By Version:**
```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', SNAPSHOT_VERSION 5);
-- or
SELECT * FROM table AT SNAPSHOT 5;
```

**By Timestamp:**
```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', SNAPSHOT_TIME '2024-01-01 12:00:00');
-- or
SELECT * FROM table AT TIMESTAMP '2024-01-01';
```

**Flow:**
1. Find snapshot matching version or timestamp
2. Load schema as of that snapshot (`schema_version`)
3. Query filters: `snapshot_id >= begin_snapshot AND (snapshot_id < end_snapshot OR end_snapshot IS NULL)`

### Snapshots

**Automatic Creation:**
- New snapshot created on each DML commit
- `snapshot_id` increments
- `schema_version` increments only on DDL

**Manual Inspection:**
```sql
SELECT * FROM ducklake_snapshots('catalog_alias');
```

**Snapshot Expiration:**
```sql
CALL ducklake_expire_snapshots('catalog_alias', older_than := INTERVAL '7 days');
```
- Marks old files for deletion
- Updates `end_snapshot` on expired file records

### Compaction

**Merge Small Files:**
```sql
CALL ducklake_merge_adjacent_files('catalog_alias', 'schema.table');
```
- Combines small files into larger ones
- Reduces file count, improves query performance

**Rewrite Data Files:**
```sql
CALL ducklake_rewrite_data_files('catalog_alias', 'schema.table');
```
- Rewrites files to remove deleted rows
- Reclaims storage from delete files

### Data Inlining

**Configuration:**
```sql
ATTACH 'ducklake:...' AS lake (
    CATALOG 'x',
    DATA_INLINING_ROW_LIMIT 1000  -- Default: 0 (disabled)
);
```

**Behavior:**
- Tables with ≤ row limit stored in metadata, not Parquet
- Faster for small lookup tables
- Data stored in `ducklake_inlined_data_tables`

### Partitioning

**Create Partitioned Table:**
```sql
CREATE TABLE events (
    event_date DATE,
    event_type VARCHAR,
    data VARCHAR
) PARTITION BY (year(event_date), month(event_date));
```

**Partition Transforms:**
- `identity(column)` - exact value
- `year(column)` - extract year
- `month(column)` - extract month
- `day(column)` - extract day
- `hour(column)` - extract hour

**File Organization:**
```
table_name/
├── event_date_year=2024/
│   ├── event_date_month=01/
│   │   └── data.parquet
│   └── event_date_month=02/
│       └── data.parquet
└── event_date_year=2025/
    └── ...
```

### Encryption

**Enable:**
```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', ENCRYPTED true);
```

**Behavior:**
- Each Parquet file encrypted with unique key
- Keys stored in `ducklake_data_file.encryption_key`
- Requires DuckDB encryption extension

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| "CATALOG is required" | Missing CATALOG option | Add `CATALOG 'name'` to ATTACH |
| "Catalog 'x' does not exist" | Catalog not in database | Use `CREATE_IF_NOT_EXISTS true` or create catalog first |
| "Unique file handle conflict" | Same DuckDB file attached twice | Use PostgreSQL for shared metadata |
| "Table does not exist" | Wrong catalog or table not created | Check catalog name, create table |

### Transaction Conflicts

- DuckLake uses optimistic concurrency
- Conflicts detected at commit time
- Retry logic configurable via `MAX_RETRY_COUNT`

---

## Performance Considerations

### Indexing Recommendations (PostgreSQL)

```sql
CREATE INDEX idx_schema_catalog ON ducklake_schema(catalog_id);
CREATE INDEX idx_table_catalog ON ducklake_table(catalog_id);
CREATE INDEX idx_snapshot_catalog ON ducklake_snapshot(catalog_id);
-- Add for all tables with catalog_id
```

### Query Patterns

- All queries filter by `catalog_id` first
- Add catalog_id to composite indexes
- Consider partitioning metadata tables by catalog_id for very large deployments

### File Size Tuning

```sql
-- Control Parquet file size
SET ducklake_target_file_size = '512MB';

-- Control row group size
SET ducklake_parquet_row_group_size_bytes = '128MB';
```

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

## Version Compatibility

| DuckLake Version | Catalog Support | Migration |
|------------------|-----------------|-----------|
| 0.1 - 0.3 | None | MigrateV01-V03 + V04 |
| 0.4-dev1 | VARCHAR catalog_id | MigrateV04 |
| 0.5-dev1 | BIGINT catalog_id | Current |

Migration is automatic when `MIGRATE_IF_REQUIRED = true` (default).
