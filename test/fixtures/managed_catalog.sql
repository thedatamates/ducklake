# Test-only fresh metabase. Keep relation definitions aligned with Crucible's schema.sql.
# Caller supplies fixture_path, fixture_schema and fixture_data_path in a foreach.
# Catalog 0 and main schema 0 preserve upstream analytical test object IDs.
statement ok
ATTACH '{fixture_path}' AS managed_fixture;
CREATE SCHEMA IF NOT EXISTS managed_fixture."{fixture_schema}";
USE managed_fixture."{fixture_schema}";
BEGIN;
CREATE TABLE ducklake_column(catalog_id BIGINT NOT NULL, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR, PRIMARY KEY ("catalog_id", "table_id", "column_id", "begin_snapshot"));
CREATE TABLE ducklake_column_mapping(catalog_id BIGINT NOT NULL, mapping_id BIGINT, table_id BIGINT, "type" VARCHAR, PRIMARY KEY ("catalog_id", "mapping_id"));
CREATE TABLE ducklake_column_tag(catalog_id BIGINT NOT NULL, table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "key" VARCHAR, "value" VARCHAR, PRIMARY KEY ("catalog_id", "table_id", "column_id", "key", "begin_snapshot"));
CREATE TABLE ducklake_data_file(catalog_id BIGINT NOT NULL, data_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT, row_group_count BIGINT, PRIMARY KEY ("catalog_id", "data_file_id"));
CREATE TABLE ducklake_delete_file(catalog_id BIGINT NOT NULL, delete_file_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT, row_group_count BIGINT, PRIMARY KEY ("catalog_id", "delete_file_id"));
CREATE TABLE ducklake_file_column_stats(catalog_id BIGINT NOT NULL, data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR, min_is_exact BOOLEAN, max_is_exact BOOLEAN, PRIMARY KEY ("catalog_id", "data_file_id", "column_id"));
CREATE TABLE ducklake_file_partition_value(catalog_id BIGINT NOT NULL, data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR, PRIMARY KEY ("catalog_id", "data_file_id", "partition_key_index"));
CREATE TABLE ducklake_file_variant_stats(catalog_id BIGINT NOT NULL, data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR, PRIMARY KEY ("catalog_id", "data_file_id", "column_id", "variant_path"));
CREATE TABLE ducklake_files_scheduled_for_deletion(catalog_id BIGINT NOT NULL, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMP WITH TIME ZONE, PRIMARY KEY ("catalog_id", "data_file_id"));
CREATE TABLE ducklake_inlined_data_tables(catalog_id BIGINT NOT NULL, table_id BIGINT, table_name VARCHAR, schema_version BIGINT, PRIMARY KEY ("catalog_id", "table_id", "schema_version"));
CREATE TABLE ducklake_macro(catalog_id BIGINT NOT NULL, schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, PRIMARY KEY ("catalog_id", "macro_id", "begin_snapshot"));
CREATE TABLE ducklake_macro_impl(catalog_id BIGINT NOT NULL, macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, "sql" VARCHAR, "type" VARCHAR, PRIMARY KEY ("catalog_id", "macro_id", "impl_id"));
CREATE TABLE ducklake_macro_parameters(catalog_id BIGINT NOT NULL, macro_id BIGINT, impl_id BIGINT, column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR, PRIMARY KEY ("catalog_id", "macro_id", "impl_id", "column_id"));
CREATE TABLE ducklake_metadata(catalog_id BIGINT, "key" VARCHAR NOT NULL, "value" VARCHAR NOT NULL, "scope" VARCHAR, scope_id BIGINT);
CREATE TABLE ducklake_name_mapping(catalog_id BIGINT NOT NULL, mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN, PRIMARY KEY ("catalog_id", "mapping_id", "column_id"));
CREATE TABLE ducklake_partition_column(catalog_id BIGINT NOT NULL, partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, "transform" VARCHAR, PRIMARY KEY ("catalog_id", "partition_id", "partition_key_index"));
CREATE TABLE ducklake_partition_info(catalog_id BIGINT NOT NULL, partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, PRIMARY KEY ("catalog_id", "partition_id"));
CREATE TABLE ducklake_schema(catalog_id BIGINT NOT NULL, schema_id BIGINT, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, PRIMARY KEY ("catalog_id", "schema_id"));
CREATE TABLE ducklake_schema_versions(catalog_id BIGINT NOT NULL, begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT NOT NULL, PRIMARY KEY (catalog_id, table_id, begin_snapshot));
CREATE TABLE ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
CREATE TABLE ducklake_snapshot_changes(catalog_id BIGINT NOT NULL, snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
CREATE TABLE ducklake_sort_expression(catalog_id BIGINT NOT NULL, sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR, PRIMARY KEY ("catalog_id", "sort_id", "sort_key_index"));
CREATE TABLE ducklake_sort_info(catalog_id BIGINT NOT NULL, sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, PRIMARY KEY ("catalog_id", "sort_id", "begin_snapshot"));
CREATE TABLE ducklake_table(catalog_id BIGINT NOT NULL, table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, PRIMARY KEY ("catalog_id", "table_id", "begin_snapshot"));
CREATE TABLE ducklake_table_column_stats(catalog_id BIGINT NOT NULL, table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR, min_is_exact BOOLEAN, max_is_exact BOOLEAN, PRIMARY KEY ("catalog_id", "table_id", "column_id"));
CREATE TABLE ducklake_table_stats(catalog_id BIGINT NOT NULL, table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT, PRIMARY KEY ("catalog_id", "table_id"));
CREATE TABLE ducklake_tag(catalog_id BIGINT NOT NULL, object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "key" VARCHAR, "value" VARCHAR, PRIMARY KEY ("catalog_id", "object_id", "key", "begin_snapshot"));
CREATE TABLE ducklake_view(catalog_id BIGINT NOT NULL, view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, "sql" VARCHAR, column_aliases VARCHAR, PRIMARY KEY ("catalog_id", "view_id", "begin_snapshot"));
CREATE TABLE ducklake_view_column_tag(catalog_id BIGINT NOT NULL, view_id BIGINT, column_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT, "key" VARCHAR, "value" VARCHAR, PRIMARY KEY ("catalog_id", "view_id", "column_name", "key", "begin_snapshot"));
CREATE TABLE ducklake_file (catalog_id BIGINT NOT NULL, file_id BIGINT NOT NULL, file_uuid UUID NOT NULL, begin_snapshot BIGINT NOT NULL, end_snapshot BIGINT, schema_id BIGINT NOT NULL, file_name VARCHAR NOT NULL, path VARCHAR NOT NULL, path_is_relative BOOLEAN NOT NULL, file_size_bytes BIGINT NOT NULL, mime_type VARCHAR NOT NULL, content_hash VARCHAR NOT NULL, PRIMARY KEY(catalog_id, file_id, begin_snapshot));
CREATE TABLE ducklake_catalog (
    catalog_id BIGINT NOT NULL,
    catalog_uuid UUID NOT NULL,
    catalog_name VARCHAR NOT NULL,
    parent_catalog_id BIGINT,
    parent_snapshot_id BIGINT,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, begin_snapshot)
);
CREATE TABLE ducklake_snapshot_lineage (
    catalog_id BIGINT NOT NULL,
    previous_snapshot_id BIGINT NOT NULL,
    snapshot_id BIGINT NOT NULL,
    PRIMARY KEY (catalog_id, previous_snapshot_id)
);
CREATE SEQUENCE ducklake_snapshot_id_seq START 1;
INSERT INTO ducklake_metadata (key, value) VALUES ('version', '1.1-dev1-catalog2'), ('encrypted', 'false');
INSERT INTO ducklake_metadata (catalog_id, key, value) VALUES (0, 'data_path', rtrim('{fixture_data_path}', '/') || '/');
INSERT INTO ducklake_snapshot VALUES (0, now(), 0, 1, 0);
INSERT INTO ducklake_catalog (catalog_id, catalog_uuid, catalog_name, parent_catalog_id, begin_snapshot, end_snapshot) VALUES (0, '00000000-0000-0000-0000-000000000001', 'test', NULL, 0, NULL);
INSERT INTO ducklake_schema VALUES (0, 0, '00000000-0000-0000-0000-000000000002', 0, NULL, 'main', 'main/', true);
INSERT INTO ducklake_snapshot_changes (catalog_id, snapshot_id, changes_made) VALUES (0, 0, 'created_schema:"main"');
COMMIT;
USE memory;
DETACH managed_fixture;
