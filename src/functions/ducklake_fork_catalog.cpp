#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct ForkCatalogBindData : public TableFunctionData {
	explicit ForkCatalogBindData(Catalog &catalog) : catalog(catalog) {
	}

	Catalog &catalog;
	string new_catalog_name;
	bool if_not_exists = false;
};

static unique_ptr<FunctionData> DuckLakeForkCatalogBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto result = make_uniq<ForkCatalogBindData>(catalog);

	result->new_catalog_name = input.inputs[1].GetValue<string>();

	// Handle if_not_exists named parameter
	for (auto &entry : input.named_parameters) {
		if (StringUtil::CIEquals(entry.first, "if_not_exists")) {
			result->if_not_exists = BooleanValue::Get(entry.second);
		}
	}

	names.emplace_back("catalog_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("catalog_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("created");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(result);
}

struct ForkCatalogState : public GlobalTableFunctionState {
	ForkCatalogState() : executed(false) {
	}

	bool executed;
};

static unique_ptr<GlobalTableFunctionState> DuckLakeForkCatalogInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	return make_uniq<ForkCatalogState>();
}

static void DuckLakeForkCatalogExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<ForkCatalogBindData>();
	auto &state = data_p.global_state->Cast<ForkCatalogState>();

	if (state.executed) {
		return;
	}
	state.executed = true;

	auto &ducklake_catalog = reinterpret_cast<DuckLakeCatalog &>(data.catalog);
	auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
	auto parent_catalog_id = ducklake_catalog.CatalogId();

	auto new_catalog_name_literal = DuckLakeUtil::SQLLiteralToString(data.new_catalog_name);

	// Check if catalog with this name already exists (include parent_catalog_id for lineage check)
	string check_query = StringUtil::Format(
	    "SELECT catalog_id, parent_catalog_id FROM {METADATA_CATALOG}.ducklake_catalog "
	    "WHERE catalog_name = %s AND end_snapshot IS NULL",
	    new_catalog_name_literal);
	auto check_result = transaction.Query(check_query);
	if (check_result->HasError()) {
		check_result->GetErrorObject().Throw("Failed to check for existing catalog: ");
	}

	// Collect all matching rows to detect duplicates
	vector<pair<idx_t, Value>> existing_catalogs;
	while (true) {
		auto check_chunk = check_result->Fetch();
		if (!check_chunk || check_chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < check_chunk->size(); i++) {
			idx_t cat_id = check_chunk->GetValue(0, i).GetValue<idx_t>();
			Value parent_val = check_chunk->GetValue(1, i);
			existing_catalogs.emplace_back(cat_id, parent_val);
		}
	}

	if (!existing_catalogs.empty()) {
		// Check for duplicates - this indicates data integrity issues
		if (existing_catalogs.size() > 1) {
			throw InvalidInputException(
			    "Multiple active catalogs named '%s' exist (catalog_ids: %llu, %llu, ...). "
			    "This indicates a data integrity issue. Please clean up duplicate catalogs before proceeding.",
			    data.new_catalog_name, existing_catalogs[0].first, existing_catalogs[1].first);
		}

		idx_t existing_catalog_id = existing_catalogs[0].first;
		Value existing_parent_val = existing_catalogs[0].second;

		if (!data.if_not_exists) {
			throw InvalidInputException("Catalog '%s' already exists. Use if_not_exists := true to return existing catalog.",
			                            data.new_catalog_name);
		}

		// if_not_exists is true - verify parent relationship using parent_catalog_id column
		bool is_fork = !existing_parent_val.IsNull();

		if (!is_fork) {
			throw InvalidInputException("Catalog '%s' exists but is not a fork (it's a root catalog).",
			                            data.new_catalog_name);
		}

		idx_t existing_parent_id = existing_parent_val.GetValue<idx_t>();
		if (existing_parent_id != parent_catalog_id) {
			throw InvalidInputException("Catalog '%s' exists but was forked from a different parent (catalog_id %llu, not %llu).",
			                            data.new_catalog_name, existing_parent_id, parent_catalog_id);
		}

		// Parent matches - return existing catalog with created=false
		output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(existing_catalog_id)));
		output.SetValue(1, 0, Value(data.new_catalog_name));
		output.SetValue(2, 0, Value::BOOLEAN(false));  // created = false
		output.SetCardinality(1);
		return;
	}

	string query = R"(
SELECT snapshot_id, next_catalog_id, next_file_id, schema_version
FROM {METADATA_CATALOG}.ducklake_snapshot
ORDER BY snapshot_id DESC LIMIT 1
)";
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get latest snapshot for fork: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("No snapshots found. Cannot fork.");
	}
	idx_t current_snapshot_id = chunk->GetValue(0, 0).GetValue<idx_t>();
	idx_t new_snapshot_id = current_snapshot_id + 1;
	idx_t new_catalog_id = chunk->GetValue(1, 0).GetValue<idx_t>();
	idx_t next_catalog_id = new_catalog_id + 1;
	idx_t next_file_id = chunk->GetValue(2, 0).GetValue<idx_t>();
	idx_t schema_version = chunk->GetValue(3, 0).GetValue<idx_t>();

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_catalog (catalog_id, catalog_uuid, catalog_name, parent_catalog_id, begin_snapshot, end_snapshot) "
	    "VALUES (%llu, UUID(), %s, %llu, %llu, NULL)",
	    new_catalog_id, new_catalog_name_literal, parent_catalog_id, new_snapshot_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to create catalog entry for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_snapshot "
	    "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
	    "VALUES (%llu, NOW(), %llu, %llu, %llu)",
	    new_snapshot_id, schema_version, next_catalog_id, next_file_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to create snapshot for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes "
	    "(snapshot_id, catalog_id, changes_made, author, commit_message, commit_extra_info) "
	    "VALUES (%llu, %llu, 'forked_from:%llu', NULL, 'Fork catalog', NULL)",
	    new_snapshot_id, new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to create snapshot_changes for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_schema (catalog_id, schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) "
	    "SELECT %llu, schema_id, UUID(), %llu, NULL, schema_name, path, path_is_relative "
	    "FROM {METADATA_CATALOG}.ducklake_schema WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy schema entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions (catalog_id, begin_snapshot, schema_version) "
	    "VALUES (%llu, %llu, %llu)",
	    new_catalog_id, new_snapshot_id, schema_version);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy schema_versions for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_table (catalog_id, table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) "
	    "SELECT %llu, table_id, UUID(), %llu, NULL, schema_id, table_name, path, path_is_relative "
	    "FROM {METADATA_CATALOG}.ducklake_table WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy table entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_view (catalog_id, view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) "
	    "SELECT %llu, view_id, UUID(), %llu, NULL, schema_id, view_name, dialect, sql, column_aliases "
	    "FROM {METADATA_CATALOG}.ducklake_view WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy view entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_column (catalog_id, table_id, column_id, begin_snapshot, end_snapshot, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, default_value_type, default_value_dialect) "
	    "SELECT %llu, table_id, column_id, %llu, NULL, column_order, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column, default_value_type, default_value_dialect "
	    "FROM {METADATA_CATALOG}.ducklake_column WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy column entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id) "
	    "SELECT %llu, data_file_id, table_id, %llu, NULL, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, encryption_key, partial_file_info, mapping_id "
	    "FROM {METADATA_CATALOG}.ducklake_data_file WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy data_file entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_delete_file (catalog_id, delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key) "
	    "SELECT %llu, delete_file_id, table_id, %llu, NULL, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key "
	    "FROM {METADATA_CATALOG}.ducklake_delete_file WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy delete_file entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_file_column_stats (catalog_id, data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats) "
	    "SELECT %llu, data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, contains_nan, extra_stats "
	    "FROM {METADATA_CATALOG}.ducklake_file_column_stats WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy file_column_stats for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_table_stats (catalog_id, table_id, record_count, next_row_id, file_size_bytes) "
	    "SELECT %llu, table_id, record_count, next_row_id, file_size_bytes "
	    "FROM {METADATA_CATALOG}.ducklake_table_stats WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy table_stats for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats (catalog_id, table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) "
	    "SELECT %llu, table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats "
	    "FROM {METADATA_CATALOG}.ducklake_table_column_stats WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy table_column_stats for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_info (catalog_id, partition_id, table_id, begin_snapshot, end_snapshot) "
	    "SELECT %llu, partition_id, table_id, %llu, NULL "
	    "FROM {METADATA_CATALOG}.ducklake_partition_info WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy partition_info for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_column (catalog_id, partition_id, table_id, partition_key_index, column_id, transform) "
	    "SELECT %llu, partition_id, table_id, partition_key_index, column_id, transform "
	    "FROM {METADATA_CATALOG}.ducklake_partition_column WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy partition_column for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_file_partition_value (catalog_id, data_file_id, table_id, partition_key_index, partition_value) "
	    "SELECT %llu, data_file_id, table_id, partition_key_index, partition_value "
	    "FROM {METADATA_CATALOG}.ducklake_file_partition_value WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy file_partition_value for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_tag (catalog_id, object_id, key, begin_snapshot, end_snapshot, value) "
	    "SELECT %llu, object_id, key, %llu, NULL, value "
	    "FROM {METADATA_CATALOG}.ducklake_tag WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy tag entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_column_tag (catalog_id, table_id, column_id, key, begin_snapshot, end_snapshot, value) "
	    "SELECT %llu, table_id, column_id, key, %llu, NULL, value "
	    "FROM {METADATA_CATALOG}.ducklake_column_tag WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy column_tag entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_column_mapping (catalog_id, mapping_id, table_id, type) "
	    "SELECT %llu, mapping_id, table_id, type "
	    "FROM {METADATA_CATALOG}.ducklake_column_mapping WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy column_mapping for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_name_mapping (catalog_id, mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) "
	    "SELECT %llu, mapping_id, column_id, source_name, target_field_id, parent_column, is_partition "
	    "FROM {METADATA_CATALOG}.ducklake_name_mapping WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy name_mapping for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_macro (catalog_id, schema_id, macro_id, macro_name, begin_snapshot, end_snapshot) "
	    "SELECT %llu, schema_id, macro_id, macro_name, %llu, NULL "
	    "FROM {METADATA_CATALOG}.ducklake_macro WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    new_catalog_id, new_snapshot_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy macro entries for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_macro_impl (catalog_id, macro_id, impl_id, dialect, sql, type) "
	    "SELECT %llu, macro_id, impl_id, dialect, sql, type "
	    "FROM {METADATA_CATALOG}.ducklake_macro_impl WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy macro_impl for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_macro_parameters (catalog_id, macro_id, impl_id, column_id, parameter_name, parameter_type, default_value, default_value_type) "
	    "SELECT %llu, macro_id, impl_id, column_id, parameter_name, parameter_type, default_value, default_value_type "
	    "FROM {METADATA_CATALOG}.ducklake_macro_parameters WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy macro_parameters for fork: ");
	}

	query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables (catalog_id, table_id, table_name, schema_version) "
	    "SELECT %llu, table_id, table_name, schema_version "
	    "FROM {METADATA_CATALOG}.ducklake_inlined_data_tables WHERE catalog_id = %llu",
	    new_catalog_id, parent_catalog_id);
	result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to copy inlined_data_tables for fork: ");
	}

	output.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(new_catalog_id)));
	output.SetValue(1, 0, Value(data.new_catalog_name));
	output.SetValue(2, 0, Value::BOOLEAN(true));  // created = true
	output.SetCardinality(1);
}

DuckLakeForkCatalogFunction::DuckLakeForkCatalogFunction()
    : TableFunction("ducklake_fork_catalog", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    DuckLakeForkCatalogExecute, DuckLakeForkCatalogBind, DuckLakeForkCatalogInit) {
	named_parameters["if_not_exists"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
