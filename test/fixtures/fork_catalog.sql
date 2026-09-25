# The caller provides a PostgreSQL attachment `inspect` and fixture_schema.
# Copy current data references, retaining the layouts needed to decode inline rows.
# This models external provisioning; it does not invoke Crucible.
statement ok
SET VARIABLE parent_snapshot = (SELECT MAX(snapshot_id) FROM inspect.{fixture_schema}.ducklake_snapshot);
SET VARIABLE fork_snapshot = (SELECT id FROM postgres_query('inspect', 'SELECT nextval(''{fixture_schema}.ducklake_snapshot_id_seq'') AS id'));
BEGIN;
INSERT INTO inspect.{fixture_schema}.ducklake_snapshot SELECT getvariable('fork_snapshot'), now(), schema_version, next_catalog_id, next_file_id FROM inspect.{fixture_schema}.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;
UPDATE inspect.{fixture_schema}.ducklake_catalog SET parent_catalog_id=0, parent_snapshot_id=getvariable('parent_snapshot'), begin_snapshot=getvariable('fork_snapshot') WHERE catalog_id=1;
DELETE FROM inspect.{fixture_schema}.ducklake_schema WHERE catalog_id=1;
DELETE FROM inspect.{fixture_schema}.ducklake_metadata WHERE catalog_id=1;
INSERT INTO inspect.{fixture_schema}.ducklake_snapshot_changes VALUES (1, getvariable('fork_snapshot'), 'forked_from:0', NULL, NULL, NULL);
COMMIT;

statement ok
INSERT INTO inspect.{fixture_schema}.ducklake_file
SELECT * REPLACE (1 AS catalog_id, uuid() AS file_uuid)
FROM inspect.{fixture_schema}.ducklake_file WHERE catalog_id=0 AND end_snapshot IS NULL;

statement ok
INSERT INTO inspect.{fixture_schema}.ducklake_alias
SELECT * REPLACE (1 AS catalog_id, uuid() AS alias_uuid)
FROM inspect.{fixture_schema}.ducklake_alias WHERE catalog_id=0 AND end_snapshot IS NULL;

foreach relation ducklake_schema ducklake_table ducklake_column ducklake_schema_versions ducklake_metadata ducklake_table_stats ducklake_table_column_stats ducklake_view ducklake_view_column_tag ducklake_tag ducklake_column_tag ducklake_macro ducklake_macro_impl ducklake_macro_parameters ducklake_column_mapping ducklake_name_mapping ducklake_partition_info ducklake_partition_column ducklake_sort_info ducklake_sort_expression ducklake_file_column_stats ducklake_file_variant_stats ducklake_file_partition_value

statement ok
INSERT INTO inspect.{fixture_schema}.{relation} SELECT * REPLACE (1 AS catalog_id) FROM inspect.{fixture_schema}.{relation} WHERE catalog_id=0;

endloop

foreach relation ducklake_data_file ducklake_delete_file

statement ok
INSERT INTO inspect.{fixture_schema}.{relation} SELECT * REPLACE (1 AS catalog_id, getvariable('fork_snapshot') AS begin_snapshot) FROM inspect.{fixture_schema}.{relation} WHERE catalog_id=0 AND end_snapshot IS NULL;

endloop
