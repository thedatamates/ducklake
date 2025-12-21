# DuckLake Forking Implementation - Remaining TODO

## Current Status
- **Tests passing:** 193/258 (75%)
- **Tests failing:** 65
- **Tests skipped:** 11 (require httpfs, spatial, or LOCAL_EXTENSION_REPO)

## Summary of Changes Made

### Schema Changes (per FORKING.md)
- Added `catalog_id` column to all entity tables
- Added `ducklake_catalog` table for versioned catalog entries
- Added `ducklake_schema_versions` table for per-catalog schema versioning
- Changed path storage to use `BaseDataPath()` (DATA_PATH without catalog subdirectory)

### Code Fixes Completed
1. Added `{CATALOG_ID}` to INSERT statements for:
   - `ducklake_column`
   - `ducklake_table_column_stats`
   - `ducklake_data_file`
   - `ducklake_file_column_stats`
   - `ducklake_file_partition_value`
   - `ducklake_delete_file`
   - `ducklake_partition_info`
   - `ducklake_partition_column`
   - `ducklake_tag`
   - `ducklake_column_tag`
   - `ducklake_inlined_data_tables`

2. Fixed `ducklake_inlined_data_tables` to use `INSERT OR REPLACE` (prevents duplicate key on ALTER TABLE)

3. Fixed `CreateCatalog` to not create a separate snapshot (was shifting version numbers)

4. Fixed `GetAllSnapshots` query (was referencing non-existent `catalog_id` in snapshot table)

5. Fixed tag INSERT column ordering (key before begin_snapshot)

6. Added `catalog_id` filters to various SELECT queries

## Remaining Issues

### 1. Missing catalog_id in Macro INSERT (~10 failures)
**Error:** `table ducklake_macro_impl has 6 columns but 5 values were supplied`

**Files affected:**
- test/sql/macros/test_simple_macro.test
- test/sql/macros/test_schema_dependency.test
- test/sql/macros/test_scalar_table_macros.test
- test/sql/macros/test_multiple_implementations.test
- test/sql/macros/test_macro_transactions.test
- test/sql/macros/test_macro_tables.test
- test/sql/macros/test_macro_multiple_connections.test
- test/sql/macros/test_defined_types.test
- test/sql/macros/test_default_parameter.test
- test/sql/macros/test_attach_timetravel.test

**Fix needed:** Add `{CATALOG_ID}` to the INSERT statement for `ducklake_macro_impl` in `WriteNewMacros()` function.

### 2. Path/Directory Issues (~20 failures)
**Error:** `Cannot open file "...": No such file or directory`

**Files affected:** Most `test/sql/add_files/*.test` files:
- add_files.test
- add_files_hive.test
- add_files_nested.test
- add_files_list.test
- add_files_missing_fields.test
- add_files_table_changes.test
- add_files_type_check_*.test (multiple)
- add_files_transaction_local.test
- add_files_rename.test
- add_files_compaction.test
- add_files_extra_columns.test
- add_files_missing_columns.test
- add_file_specific_schema.test
- add_file_footer_size.test
- add_empty_file.test
- add_old_list.test
- add_removed_files.test

**Likely cause:** The change from `DataPath()` to `BaseDataPath()` for path storage may have affected how directories are created or resolved. Need to verify that directory creation still works correctly.

### 3. Other Failures (categorized)

#### Compaction Tests (~5 failures)
- test/sql/compaction/expire_snapshots.test
- test/sql/compaction/expire_snapshots_drop_table.test
- test/sql/compaction/expire_snapshot_global_option.test
- test/sql/compaction/merge_files_expired_snapshots.test
- test/sql/compaction/compaction_size_limit.test

#### Data Inlining Tests (~3 failures)
- test/sql/data_inlining/data_inlining_partitions.test
- test/sql/data_inlining/data_inlining_option_transaction_local.test
- test/sql/data_inlining/data_inlining_alter.test

#### Partitioning Tests (~4 failures)
- test/sql/partitioning/basic_partitioning.test
- test/sql/partitioning/multi_key_partition.test
- test/sql/partitioning/disable_hive_partitioning.test
- test/sql/partitioning/partition_tpch.test_slow

#### Delete Tests (~3 failures)
- test/sql/delete/delete_ignore_extra_columns.test
- test/sql/delete/delete_same_transaction.test
- test/sql/delete/multi_deletes.test

#### Remove Orphans Tests (~2 failures)
- test/sql/remove_orphans/remove_orphaned_files.test
- test/sql/remove_orphans/mixed_paths.test

#### Rewrite Data Files Tests (~2 failures)
- test/sql/rewrite_data_files/test_last_snapshot_rewrite.test
- test/sql/rewrite_data_files/last_snapshot_multiple_inserts.test

#### Single Test Failures (~16)
- test/sql/update/update_rollback.test
- test/sql/transaction/transaction_schema.test
- test/sql/settings/per_table_settings.test
- test/sql/secrets/ducklake_secrets.test
- test/sql/schema_evolution/field_ids.test
- test/sql/insert/insert_file_size.test
- test/sql/functions/ducklake_snapshots.test
- test/sql/cleanup/create_drop_cleanup.test
- test/sql/checkpoint/checkpoint_ducklake.test
- test/sql/catalog/quoted_identifiers.test
- test/sql/alter/expire_snapshot_bug.test

## Tables That May Need catalog_id Added to Queries

Check these tables for missing `catalog_id` in INSERT/SELECT/UPDATE statements:
- `ducklake_macro` - likely OK (was updated)
- `ducklake_macro_impl` - **NEEDS FIX** (INSERT missing catalog_id)
- `ducklake_view` - check if updated
- `ducklake_column_mapping` - check if updated

## Testing Commands

Run all tests:
```bash
./build/release/test/unittest "test/sql/*"
```

Run specific test:
```bash
./build/release/test/unittest "test/sql/macros/test_simple_macro.test"
```

Run fork tests (should pass):
```bash
./build/release/test/unittest "test/sql/fork/*"
```

## Files Modified

Main files changed for forking implementation:
- `src/storage/ducklake_metadata_manager.cpp` - Schema, queries, INSERT statements
- `src/storage/ducklake_initializer.cpp` - Catalog creation on ATTACH
- `src/storage/ducklake_transaction.cpp` - Query placeholder substitution
- `src/functions/ducklake_fork_catalog.cpp` - Fork function implementation
- `test/sql/fork/basic_fork.test` - New test
- `test/sql/fork/fork_isolation.test` - New test
