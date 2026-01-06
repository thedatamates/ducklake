CREATE TABLE ducklake_metadata(
    key VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    scope VARCHAR,
    scope_id BIGINT
);

CREATE TABLE ducklake_snapshot(
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    schema_version BIGINT NOT NULL,
    next_catalog_id BIGINT NOT NULL,
    next_file_id BIGINT NOT NULL
);

CREATE TABLE ducklake_snapshot_changes(
    snapshot_id BIGINT PRIMARY KEY,
    catalog_id BIGINT NOT NULL,
    changes_made VARCHAR,
    author VARCHAR,
    commit_message VARCHAR,
    commit_extra_info VARCHAR
);

CREATE TABLE ducklake_catalog(
    catalog_id BIGINT NOT NULL,
    catalog_uuid UUID NOT NULL DEFAULT gen_random_uuid(),
    catalog_name VARCHAR NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, begin_snapshot)
);

CREATE TABLE ducklake_schema(
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    schema_uuid UUID DEFAULT gen_random_uuid(),
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_name VARCHAR,
    path VARCHAR,
    path_is_relative BOOLEAN,
    PRIMARY KEY (catalog_id, schema_id)
);

CREATE TABLE ducklake_schema_versions(
    catalog_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    schema_version BIGINT NOT NULL,
    PRIMARY KEY (catalog_id, begin_snapshot)
);

CREATE TABLE ducklake_table(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    table_uuid UUID DEFAULT gen_random_uuid(),
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_id BIGINT NOT NULL,
    table_name VARCHAR,
    path VARCHAR,
    path_is_relative BOOLEAN,
    PRIMARY KEY (catalog_id, table_id, begin_snapshot)
);

CREATE TABLE ducklake_view(
    catalog_id BIGINT NOT NULL,
    view_id BIGINT NOT NULL,
    view_uuid UUID DEFAULT gen_random_uuid(),
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_id BIGINT NOT NULL,
    view_name VARCHAR,
    dialect VARCHAR,
    sql VARCHAR,
    column_aliases VARCHAR,
    PRIMARY KEY (catalog_id, view_id, begin_snapshot)
);

CREATE TABLE ducklake_column(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    column_order BIGINT,
    column_name VARCHAR,
    column_type VARCHAR,
    initial_default VARCHAR,
    default_value VARCHAR,
    nulls_allowed BOOLEAN,
    parent_column BIGINT,
    default_value_type VARCHAR,
    default_value_dialect VARCHAR,
    PRIMARY KEY (catalog_id, table_id, column_id, begin_snapshot)
);

CREATE TABLE ducklake_data_file(
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
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

CREATE TABLE ducklake_delete_file(
    catalog_id BIGINT NOT NULL,
    delete_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    data_file_id BIGINT NOT NULL,
    path VARCHAR,
    path_is_relative BOOLEAN,
    format VARCHAR,
    delete_count BIGINT,
    file_size_bytes BIGINT,
    footer_size BIGINT,
    encryption_key VARCHAR,
    PRIMARY KEY (catalog_id, delete_file_id)
);

CREATE TABLE ducklake_table_stats(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    record_count BIGINT,
    next_row_id BIGINT,
    file_size_bytes BIGINT,
    PRIMARY KEY (catalog_id, table_id)
);

CREATE TABLE ducklake_table_column_stats(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    contains_null BOOLEAN,
    contains_nan BOOLEAN,
    min_value VARCHAR,
    max_value VARCHAR,
    extra_stats VARCHAR,
    PRIMARY KEY (catalog_id, table_id, column_id)
);

CREATE TABLE ducklake_file_column_stats(
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    column_size_bytes BIGINT,
    value_count BIGINT,
    null_count BIGINT,
    min_value VARCHAR,
    max_value VARCHAR,
    contains_nan BOOLEAN,
    extra_stats VARCHAR,
    PRIMARY KEY (catalog_id, data_file_id, column_id)
);

CREATE TABLE ducklake_partition_info(
    catalog_id BIGINT NOT NULL,
    partition_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, partition_id)
);

CREATE TABLE ducklake_partition_column(
    catalog_id BIGINT NOT NULL,
    partition_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    partition_key_index BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    transform VARCHAR,
    PRIMARY KEY (catalog_id, partition_id, partition_key_index)
);

CREATE TABLE ducklake_file_partition_value(
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    partition_key_index BIGINT NOT NULL,
    partition_value VARCHAR,
    PRIMARY KEY (catalog_id, data_file_id, partition_key_index)
);

CREATE TABLE ducklake_column_mapping(
    catalog_id BIGINT NOT NULL,
    mapping_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    type VARCHAR,
    PRIMARY KEY (catalog_id, mapping_id)
);

CREATE TABLE ducklake_name_mapping(
    catalog_id BIGINT NOT NULL,
    mapping_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    source_name VARCHAR,
    target_field_id BIGINT,
    parent_column BIGINT,
    is_partition BOOLEAN,
    PRIMARY KEY (catalog_id, mapping_id, column_id)
);

CREATE TABLE ducklake_tag(
    catalog_id BIGINT NOT NULL,
    object_id BIGINT NOT NULL,
    key VARCHAR NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    value VARCHAR,
    PRIMARY KEY (catalog_id, object_id, key, begin_snapshot)
);

CREATE TABLE ducklake_column_tag(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    key VARCHAR NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    value VARCHAR,
    PRIMARY KEY (catalog_id, table_id, column_id, key, begin_snapshot)
);

CREATE TABLE ducklake_inlined_data_tables(
    catalog_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    table_name VARCHAR NOT NULL,
    schema_version BIGINT NOT NULL,
    PRIMARY KEY (catalog_id, table_id, schema_version)
);

CREATE TABLE ducklake_macro(
    catalog_id BIGINT NOT NULL,
    schema_id BIGINT NOT NULL,
    macro_id BIGINT NOT NULL,
    macro_name VARCHAR,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    PRIMARY KEY (catalog_id, macro_id, begin_snapshot)
);

CREATE TABLE ducklake_macro_impl(
    catalog_id BIGINT NOT NULL,
    macro_id BIGINT NOT NULL,
    impl_id BIGINT NOT NULL,
    dialect VARCHAR,
    sql VARCHAR,
    type VARCHAR,
    PRIMARY KEY (catalog_id, macro_id, impl_id)
);

CREATE TABLE ducklake_macro_parameters(
    catalog_id BIGINT NOT NULL,
    macro_id BIGINT NOT NULL,
    impl_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    parameter_name VARCHAR,
    parameter_type VARCHAR,
    default_value VARCHAR,
    default_value_type VARCHAR,
    PRIMARY KEY (catalog_id, macro_id, impl_id, column_id)
);

CREATE TABLE ducklake_files_scheduled_for_deletion(
    catalog_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    path VARCHAR NOT NULL,
    path_is_relative BOOLEAN,
    schedule_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (catalog_id, data_file_id)
);

-- ducklake_catalog: PK (catalog_id, begin_snapshot)
-- Unique catalog names among active catalogs
CREATE UNIQUE INDEX idx_catalog_name_unique ON ducklake_catalog(catalog_name) WHERE end_snapshot IS NULL;

-- ducklake_table: PK (catalog_id, table_id, begin_snapshot)
-- Need index for schema_id lookups (not in PK)
CREATE INDEX idx_table_schema ON ducklake_table(catalog_id, schema_id);

-- ducklake_view: PK (catalog_id, view_id, begin_snapshot)
-- Need index for schema_id lookups (not in PK)
CREATE INDEX idx_view_schema ON ducklake_view(catalog_id, schema_id);

-- ducklake_data_file: PK (catalog_id, data_file_id)
-- Need index for table_id lookups (not in PK)
-- Need index for data_file_id alone (some queries don't have catalog_id)
CREATE INDEX idx_data_file_table ON ducklake_data_file(catalog_id, table_id);
CREATE INDEX idx_data_file_id ON ducklake_data_file(data_file_id);

-- ducklake_delete_file: PK (catalog_id, delete_file_id)
-- Need index for table_id and data_file_id lookups (not in PK)
CREATE INDEX idx_delete_file_table ON ducklake_delete_file(catalog_id, table_id);
CREATE INDEX idx_delete_file_data ON ducklake_delete_file(catalog_id, data_file_id);

-- ducklake_file_column_stats: PK (catalog_id, data_file_id, column_id)
-- Need index for table_id lookups (not in PK)
CREATE INDEX idx_file_column_stats_table ON ducklake_file_column_stats(catalog_id, table_id);

-- ducklake_partition_info: PK (catalog_id, partition_id)
-- Need index for table_id lookups (not in PK)
CREATE INDEX idx_partition_info_table ON ducklake_partition_info(catalog_id, table_id);

-- ducklake_partition_column: PK (catalog_id, partition_id, partition_key_index)
-- Need index for table_id lookups (not in PK)
CREATE INDEX idx_partition_column_table ON ducklake_partition_column(catalog_id, table_id);

-- ducklake_file_partition_value: PK (catalog_id, data_file_id, partition_key_index)
-- Need index for table_id lookups (not in PK)
CREATE INDEX idx_file_partition_value_table ON ducklake_file_partition_value(catalog_id, table_id);

-- ducklake_macro: PK (catalog_id, macro_id, begin_snapshot)
-- Need index for schema_id lookups (not in PK)
CREATE INDEX idx_macro_schema ON ducklake_macro(catalog_id, schema_id);

-- Name uniqueness constraints (partial indexes on active entries only)
-- Prevents duplicate names within their parent scope
CREATE UNIQUE INDEX idx_schema_name_unique ON ducklake_schema(catalog_id, schema_name) WHERE end_snapshot IS NULL;
CREATE UNIQUE INDEX idx_table_name_unique ON ducklake_table(catalog_id, schema_id, table_name) WHERE end_snapshot IS NULL;
CREATE UNIQUE INDEX idx_column_name_unique ON ducklake_column(catalog_id, table_id, column_name) WHERE end_snapshot IS NULL;
CREATE UNIQUE INDEX idx_view_name_unique ON ducklake_view(catalog_id, schema_id, view_name) WHERE end_snapshot IS NULL;
CREATE UNIQUE INDEX idx_macro_name_unique ON ducklake_macro(catalog_id, schema_id, macro_name) WHERE end_snapshot IS NULL;

INSERT INTO ducklake_metadata (key, value) VALUES ('version', '0.5-dev1');

-- Bootstrap snapshot required for DuckLake to create catalogs
INSERT INTO ducklake_snapshot (snapshot_id, schema_version, next_catalog_id, next_file_id)
VALUES (0, 0, 0, 0);
