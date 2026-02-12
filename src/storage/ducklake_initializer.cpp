#include "duckdb/main/attached_database.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include "storage/ducklake_initializer.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_schema_entry.hpp"

namespace duckdb {

DuckLakeInitializer::DuckLakeInitializer(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeOptions &options_p)
    : context(context), catalog(catalog), options(options_p) {
	InitializeDataPath();
}

string DuckLakeInitializer::GetAttachOptions() {
	vector<string> attach_options;
	if (options.access_mode != AccessMode::AUTOMATIC) {
		switch (options.access_mode) {
		case AccessMode::READ_ONLY:
			attach_options.push_back("READ_ONLY");
			break;
		case AccessMode::READ_WRITE:
			attach_options.push_back("READ_WRITE");
			break;
		default:
			throw InternalException("Unsupported access mode in DuckLake attach");
		}
	}
	for (auto &option : options.metadata_parameters) {
		attach_options.push_back(option.first + " " + option.second.ToSQLString());
	}

	if (attach_options.empty()) {
		return string();
	}
	string result;
	for (auto &option : attach_options) {
		if (!result.empty()) {
			result += ", ";
		}
		result += option;
	}
	return " (" + result + ")";
}

void DuckLakeInitializer::Initialize() {
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	auto result =
	    transaction.Query("ATTACH {METADATA_PATH} AS {METADATA_CATALOG_NAME_IDENTIFIER}" + GetAttachOptions());
	if (result->HasError()) {
		auto &error_obj = result->GetErrorObject();
		error_obj.Throw("Failed to attach DuckLake MetaData \"" + catalog.MetadataDatabaseName() + "\" at path + \"" +
		                catalog.MetadataPath() + "\"");
	}
	// explicitly load all secrets - work-around to secret initialization bug
	transaction.Query("FROM duckdb_secrets()");

	if (options.metadata_schema.empty()) {
		// if the schema is not explicitly set by the user - set it to the default schema in the catalog
		options.metadata_schema = transaction.GetDefaultSchemaName();
	}
	// after the metadata database is attached initialize the ducklake
	// check if we are loading an existing DuckLake or creating a new one
	// FIXME: verify that all tables are in the correct format instead
	auto check_result = transaction.Query(
	    "SELECT COUNT(*) FROM duckdb_tables() WHERE database_name={METADATA_CATALOG_NAME_LITERAL} AND "
	    "schema_name={METADATA_SCHEMA_NAME_LITERAL} AND table_name LIKE 'ducklake_%'");
	if (check_result->HasError()) {
		auto &error_obj = check_result->GetErrorObject();
		error_obj.Throw("Failed to load DuckLake table data");
	}
	auto count = check_result->Fetch()->GetValue(0, 0).GetValue<idx_t>();
	if (count == 0) {
		throw InvalidInputException("DuckLake metadata schema is not initialized at \"%s\". "
		                            "Provision metadata with Crucible before attaching.",
		                            options.metadata_path);
	}
	LoadExistingDuckLake(transaction);
	if (options.at_clause) {
		// if the user specified a snapshot try to load it to trigger an error if it does not exist
		transaction.GetSnapshot();
	}
}

void DuckLakeInitializer::InitializeDataPath() {
	auto &data_path = options.data_path;
	if (data_path.empty()) {
		options.effective_data_path = "";
		return;
	}

	CheckAndAutoloadedRequiredExtension(data_path);

	auto &fs = FileSystem::GetFileSystem(context);
	auto separator = fs.PathSeparator(data_path);
	if (!StringUtil::EndsWith(data_path, separator)) {
		data_path += separator;
	}
	catalog.Separator() = separator;

	if (options.catalog_name.empty()) {
		options.effective_data_path = data_path;
	} else {
		options.effective_data_path = data_path + options.catalog_name + separator;
	}
}

void DuckLakeInitializer::LoadExistingDuckLake(DuckLakeTransaction &transaction) {
	// load the data path from the existing duck lake
	auto &metadata_manager = transaction.GetMetadataManager();
	auto metadata = metadata_manager.LoadDuckLake();
	for (auto &tag : metadata.tags) {
		if (tag.key == "version") {
			string version = tag.value;
			if (version != "0.5-dev1") {
				throw InvalidInputException("DuckLake Extension requires schema version 0.5-dev1, current version is %s. "
				                            "Migration from older versions is not supported. Please create a fresh v0.5 schema.",
				                            version);
			}
		}
		if (tag.key == "data_path") {
			if (options.data_path.empty()) {
				// set the data path to the value in the tag
				options.data_path = tag.value;
				InitializeDataPath();
				// load the correct path from the metadata manager
				// we need to do this after InitializeDataPath() because that sets up the correct separator
				options.data_path = metadata_manager.LoadPath(options.data_path);
			} else {
				// verify that they match if override_data_path is not set to true
				if (metadata_manager.StorePath(options.data_path) != tag.value && !options.override_data_path) {
					throw InvalidConfigurationException(
					    "DATA_PATH parameter \"%s\" does not match existing data path in the catalog \"%s\".\nYou can "
					    "override the DATA_PATH by setting OVERRIDE_DATA_PATH to True.",
					    options.data_path, tag.value);
				}
			}
		}
		if (tag.key == "encrypted") {
			if (tag.value == "true") {
				catalog.SetEncryption(DuckLakeEncryption::ENCRYPTED);
			} else if (tag.value == "false") {
				catalog.SetEncryption(DuckLakeEncryption::UNENCRYPTED);
			} else {
				throw NotImplementedException("Encrypted should be either true or false");
			}
		}
		options.config_options[tag.key] = tag.value;
	}
	for (auto &entry : metadata.schema_settings) {
		options.schema_options[entry.schema_id][entry.tag.key] = entry.tag.value;
	}
	for (auto &entry : metadata.table_settings) {
		options.table_options[entry.table_id][entry.tag.key] = entry.tag.value;
	}

	if (!options.has_catalog_id) {
		throw InvalidInputException("CATALOG_ID is required. "
		                            "Catalog lifecycle and lookup are owned by Crucible.");
	}

	auto query = StringUtil::Format(
	    "SELECT catalog_name FROM {METADATA_CATALOG}.ducklake_catalog "
	    "WHERE catalog_id = %llu AND end_snapshot IS NULL",
	    options.catalog_id);
	auto catalog_result = transaction.Query(query);
	if (catalog_result->HasError()) {
		catalog_result->GetErrorObject().Throw("Failed to resolve DuckLake catalog by CATALOG_ID: ");
	}
	auto chunk = catalog_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InvalidInputException("Catalog with CATALOG_ID %llu does not exist in this DuckLake instance",
		                            options.catalog_id);
	}
	if (chunk->GetValue(0, 0).IsNull()) {
		throw InvalidInputException("Catalog with CATALOG_ID %llu has no catalog_name", options.catalog_id);
	}
	auto resolved_catalog_name = chunk->GetValue(0, 0).GetValue<string>();
	if (options.catalog_name.empty()) {
		options.catalog_name = resolved_catalog_name;
	}
	if (!options.data_path.empty()) {
		InitializeDataPath();
	}
}

} // namespace duckdb
