#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

static unique_ptr<FunctionData> DuckLakeCatalogsBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Define return columns
	names.emplace_back("catalog_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("catalog_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("catalog_uuid");
	return_types.emplace_back(LogicalType::UUID);

	names.emplace_back("parent_catalog_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("begin_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("end_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	// Query all active catalogs (end_snapshot IS NULL)
	string query = R"(
SELECT catalog_id, catalog_name, catalog_uuid, parent_catalog_id, begin_snapshot, end_snapshot
FROM {METADATA_CATALOG}.ducklake_catalog
WHERE end_snapshot IS NULL
ORDER BY catalog_id
)";
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get catalogs: ");
	}

	auto bind_data = make_uniq<MetadataBindData>();
	unique_ptr<DataChunk> chunk;
	while ((chunk = result->Fetch()) && chunk->size() > 0) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			vector<Value> row_values;
			row_values.push_back(chunk->GetValue(0, row));  // catalog_id
			row_values.push_back(chunk->GetValue(1, row));  // catalog_name
			row_values.push_back(chunk->GetValue(2, row));  // catalog_uuid
			row_values.push_back(chunk->GetValue(3, row));  // parent_catalog_id (NULL for root)
			row_values.push_back(chunk->GetValue(4, row));  // begin_snapshot
			row_values.push_back(chunk->GetValue(5, row));  // end_snapshot (NULL for active)
			bind_data->rows.push_back(std::move(row_values));
		}
	}

	return std::move(bind_data);
}

DuckLakeCatalogsFunction::DuckLakeCatalogsFunction()
    : BaseMetadataFunction("ducklake_catalogs", DuckLakeCatalogsBind) {
}

} // namespace duckdb
