# Catalog ID NOT NULL Constraint Plan

## Important: No Single-Tenant Mode

There is NO "single-tenant" or "default catalog" mode. Every row in every table MUST belong to an explicitly identified catalog. The empty string `''` used in the current implementation is a placeholder that must be replaced with actual catalog identifiers.

A catalog_id is required for:
- Every snapshot
- Every schema
- Every table
- Every view
- Every macro
- All associated metadata (stats, partitions, scheduled deletions, etc.)

## Goal

Change `catalog_id` from `VARCHAR DEFAULT ''` to `VARCHAR NOT NULL` across all 10 tables. Every row must explicitly belong to a catalog - there is no default or fallback mode.

## Current State

All 10 tables currently define catalog_id as:
```sql
catalog_id VARCHAR DEFAULT ''
```

This allows NULL values and defaults to empty string, which is incorrect for a multi-tenant system.

## Target State

All 10 tables should define catalog_id as:
```sql
catalog_id VARCHAR NOT NULL
```

No default value - the catalog_id must be explicitly provided on every INSERT.

---

## Tables to Update

### In InitializeDuckLake (CREATE TABLE statements)

| Table | Current | Target |
|-------|---------|--------|
| ducklake_snapshot | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_snapshot_changes | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_schema | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_table | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_view | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_table_stats | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_partition_info | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_files_scheduled_for_deletion | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_schema_versions | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |
| ducklake_macro | `catalog_id VARCHAR DEFAULT ''` | `catalog_id VARCHAR NOT NULL` |

---

## Step-by-Step Implementation

### Step 1: Update CREATE TABLE Statements

File: `src/storage/ducklake_metadata_manager.cpp`
Function: `InitializeDuckLake` (lines 65-86)

Change all 10 occurrences of:
```cpp
catalog_id VARCHAR DEFAULT ''
```
To:
```cpp
catalog_id VARCHAR NOT NULL
```

### Step 2: Update MigrateV04 ALTER TABLE Statements

File: `src/storage/ducklake_metadata_manager.cpp`
Function: `MigrateV04` (lines 173-188)

Current:
```cpp
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot ADD COLUMN {IF_NOT_EXISTS} catalog_id VARCHAR DEFAULT '';
```

Target:
```cpp
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot ADD COLUMN {IF_NOT_EXISTS} catalog_id VARCHAR NOT NULL DEFAULT '';
```

**Note:** Migration needs `DEFAULT ''` temporarily to populate existing rows, then we could add a follow-up migration to remove the default. Or we accept that migrated databases will have empty string for pre-existing rows.

### Step 3: Update INSERT Statements in InitializeDuckLake

Current INSERT statements use empty string `''`:
```cpp
INSERT INTO ... VALUES ('', 0, ...);
```

These need to use `{CATALOG_ID}` placeholder:
```cpp
INSERT INTO ... VALUES ({CATALOG_ID}, 0, ...);
```

**Locations to update:**
- Line 89: `ducklake_schema_versions`
- Line 90: `ducklake_snapshot`
- Line 91: `ducklake_snapshot_changes`
- Line 93: `ducklake_schema`

### Step 4: Ensure {CATALOG_ID} is Provided

The `{CATALOG_ID}` placeholder must be replaced with an actual value before query execution. This requires:

1. Determining where `{CATALOG_ID}` gets its value
2. Ensuring it's never empty/NULL
3. Passing catalog_id through the initialization flow

---

## Migration Considerations

### User-Provided Catalog ID Required for Migration

Migration IS possible, but the **user must provide a catalog_id** for their existing data. We cannot guess or assign a default - the user must tell us which catalog their existing data belongs to.

**Migration flow:**
1. User initiates migration and provides their catalog_id (e.g., `'my-catalog-uuid'`)
2. MigrateV04 runs:
   ```sql
   ALTER TABLE ... ADD COLUMN catalog_id VARCHAR NOT NULL DEFAULT 'user_provided_catalog_id';
   ```
3. All existing rows get the user-provided catalog_id
4. Database is now multi-tenant ready

**Implementation requirements:**
- MigrateV04 must accept a catalog_id parameter
- Migration should fail/error if no catalog_id is provided
- Cannot proceed with empty string or placeholder

### New Databases
- Created with `catalog_id VARCHAR NOT NULL`
- Must provide catalog_id on initialization

---

## Test Updates Required

Tests that insert data with empty catalog_id `''` will need to be updated to provide a real catalog_id value, OR we need to set up test fixtures that establish a catalog context.

**Files likely affected:**
- `test/sql/macros/*.test` (7 files)
- `test/sql/migration/migration.test`
- Potentially many other test files that create tables/schemas

---

## Verification Checklist

- [ ] All 10 CREATE TABLE statements use `VARCHAR NOT NULL`
- [ ] All INSERT statements in InitializeDuckLake use `{CATALOG_ID}` (not `''`)
- [ ] MigrateV04 handles existing rows appropriately
- [ ] `{CATALOG_ID}` placeholder is resolved to actual value before execution
- [ ] Tests updated to provide catalog_id values
- [ ] All tests pass

---

## Open Questions

1. **Migration strategy:** How to handle existing rows in migrated databases that would have NULL/empty catalog_id?

2. **Catalog ID source:** Where does the catalog_id value come from during:
   - Initial database creation
   - Schema/table creation
   - Data insertion

3. **Test fixtures:** Should tests use a mock catalog_id, or should the test framework establish a catalog context?
