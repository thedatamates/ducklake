# Adding catalog_uuid to DuckLake

## Current Schema

```sql
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT PRIMARY KEY,
    catalog_name VARCHAR NOT NULL UNIQUE,
    created_snapshot BIGINT NOT NULL
);
```

## Target Schema

```sql
CREATE TABLE ducklake_catalog(
    catalog_id BIGINT PRIMARY KEY,
    catalog_uuid UUID NOT NULL,
    catalog_name VARCHAR NOT NULL UNIQUE,
    created_snapshot BIGINT NOT NULL
);
```

## Files to Modify

### 1. `src/storage/ducklake_metadata_manager.cpp`

**Line 67 - Schema definition:**
```cpp
// FROM:
CREATE TABLE {METADATA_CATALOG}.ducklake_catalog(catalog_id BIGINT PRIMARY KEY, catalog_name VARCHAR NOT NULL UNIQUE, created_snapshot BIGINT NOT NULL);

// TO:
CREATE TABLE {METADATA_CATALOG}.ducklake_catalog(catalog_id BIGINT PRIMARY KEY, catalog_uuid UUID NOT NULL, catalog_name VARCHAR NOT NULL UNIQUE, created_snapshot BIGINT NOT NULL);
```

**Line 92 - Initial catalog insert:**
```cpp
// FROM:
INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_name, created_snapshot) VALUES ({CATALOG_ID}, {CATALOG_NAME}, 0);

// TO:
INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_uuid, catalog_name, created_snapshot) VALUES ({CATALOG_ID}, UUID(), {CATALOG_NAME}, 0);
```

**Line 185-186 - Migration (MigrateV04):**
```cpp
// FROM:
CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_catalog(catalog_id BIGINT PRIMARY KEY, catalog_name VARCHAR NOT NULL UNIQUE, created_snapshot BIGINT NOT NULL);
INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_name, created_snapshot) VALUES (%llu, {CATALOG_NAME}, 0) ON CONFLICT DO NOTHING;

// TO:
CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_catalog(catalog_id BIGINT PRIMARY KEY, catalog_uuid UUID NOT NULL DEFAULT UUID(), catalog_name VARCHAR NOT NULL UNIQUE, created_snapshot BIGINT NOT NULL);
INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_uuid, catalog_name, created_snapshot) VALUES (%llu, UUID(), {CATALOG_NAME}, 0) ON CONFLICT DO NOTHING;
```

**Line 302-304 - CreateCatalog:**
```cpp
// FROM:
query = StringUtil::Format(
    "INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_name, created_snapshot) "
    "VALUES (%llu, %s, %llu)",
    new_catalog_id, catalog_name_literal, new_snapshot_id);

// TO:
query = StringUtil::Format(
    "INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_uuid, catalog_name, created_snapshot) "
    "VALUES (%llu, UUID(), %s, %llu)",
    new_catalog_id, catalog_name_literal, new_snapshot_id);
```

### 2. New Migration (MigrateV05)

Add a new migration to add the column to existing databases:

```cpp
void DuckLakeMetadataManager::MigrateV05(bool allow_failures) {
    string migrate_query = R"(
ALTER TABLE {METADATA_CATALOG}.ducklake_catalog ADD COLUMN {IF_NOT_EXISTS} catalog_uuid UUID DEFAULT UUID();
UPDATE {METADATA_CATALOG}.ducklake_catalog SET catalog_uuid = UUID() WHERE catalog_uuid IS NULL;
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.5-dev1' WHERE key = 'version';
    )";
    auto result = transaction.Execute(migrate_query);
    if (result->HasError() && !allow_failures) {
        result->GetErrorObject().Throw("Failed to migrate to version 0.5-dev1: ");
    }
}
```

### 3. `src/include/storage/ducklake_catalog.hpp`

Add getter for catalog UUID:

```cpp
const string &CatalogUUID() const {
    return catalog_uuid;
}
```

Add member:

```cpp
string catalog_uuid;
```

### 4. `src/storage/ducklake_catalog.cpp`

Load catalog_uuid during initialization:

```cpp
// In Initialize() or FinalizeLoad(), query for catalog_uuid:
auto result = transaction.Query("SELECT catalog_uuid::VARCHAR FROM {METADATA_CATALOG}.ducklake_catalog WHERE catalog_id = {CATALOG_ID}");
// Store result in catalog_uuid member
```

### 5. `src/include/common/ducklake_options.hpp`

Add catalog_uuid to DuckLakeOptions if needed for attach info.

## Query Changes

Any queries that SELECT from `ducklake_catalog` may need updating if they use `SELECT *`.

**Search for:**
```
FROM.*ducklake_catalog
SELECT.*ducklake_catalog
```

## Testing

1. New catalog creation generates UUID
2. Migration adds UUID to existing catalogs
3. UUID is queryable via `ducklake_catalog_info()` or similar
4. UUID persists across detach/attach

## Version Bump

Update version string from `0.4-dev1` to `0.5-dev1` (or appropriate version).

## Summary of Changes

| Location | Change |
|----------|--------|
| Schema definition (line 67) | Add `catalog_uuid UUID NOT NULL` |
| Initial insert (line 92) | Add `UUID()` value |
| MigrateV04 (lines 185-186) | Add column with default |
| CreateCatalog (lines 302-304) | Add `UUID()` value |
| New MigrateV05 | Add column to existing databases |
| ducklake_catalog.hpp | Add member and getter |
| ducklake_catalog.cpp | Load UUID on init |
