#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
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
#include "common/ducklake_util.hpp"
#include "common/ducklake_version.hpp"
#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"

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
	const string metadata_type = catalog.MetadataType();
	if (metadata_type.empty() || metadata_type == "duckdb") {
		// this is duckdb, we always do latest storage
		attach_options.push_back(StringUtil::Format("STORAGE_VERSION '%s'", "latest"));
	}
	// scope the underlying Postgres attach to the metadata schema - otherwise the postgres extension reflects
	// every schema in the database on attach, which is very slow on large or multi-tenant catalogs.
	// only done when the user explicitly set METADATA_SCHEMA - the default schema is not known until after attach.
	// an explicit META_SCHEMA (or metadata_parameters 'schema') takes precedence over this.
	bool is_postgres = metadata_type == "postgres" || metadata_type == "postgres_scanner";
	bool user_set_schema = options.metadata_parameters.find("schema") != options.metadata_parameters.end();
	if (is_postgres && !user_set_schema && !options.metadata_schema.empty()) {
		attach_options.push_back("SCHEMA " +
		                         DuckLakeUtil::SQLLiteralToString(options.metadata_schema.GetIdentifierName()));
	}
	if (options.hide_metadata_catalog) {
		attach_options.push_back("HIDDEN true");
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
	auto &metadata_manager = transaction.GetMetadataManager();
	// attach the metadata database
	const string attach_query =
	    "ATTACH OR REPLACE {METADATA_PATH} AS {METADATA_CATALOG_NAME_IDENTIFIER}" + GetAttachOptions();
	auto result = metadata_manager.AttachMetadata(attach_query);
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
	// if no explicit ducklake_version was set via ATTACH, check the global setting
	if (options.ducklake_version == DuckLakeVersion::UNSET) {
		Value setting_val;
		if (context.TryGetCurrentSetting("ducklake_default_version", setting_val) && !setting_val.IsNull()) {
			auto version = DuckLakeVersionFromString(setting_val.ToString());
			if (version < DuckLakeVersion::V1_0) {
				throw InvalidInputException("ducklake_default_version must be >= '1.0', got '%s'",
				                            setting_val.ToString());
			}
			options.ducklake_version = version;
		}
	}

	if (!options.has_catalog_id) {
		throw InvalidInputException("CATALOG_ID is required. Provision catalogs with Crucible before attaching.");
	}
	if (!metadata_manager.MetadataExists()) {
		throw InvalidInputException("DuckLake metadata must be provisioned by Crucible before attaching.");
	}
	LoadExistingDuckLake(transaction);
	// note: re-fetch the metadata manager here - InitializeNewDuckLake/LoadExistingDuckLake may have
	// swapped it out via SetVersionedMetadataManager, so the `metadata_manager` reference taken at the
	// top of Initialize() would now dangle.
	auto &current_metadata_manager = transaction.GetMetadataManager();
	// probe the metadata server for optional capabilities (e.g. server-side commit retries) once per attach
	current_metadata_manager.ProbeServerCapabilities();
	current_metadata_manager.ClearCache();
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
	// pop trailing path separators
	while (!data_path.empty() && (data_path.back() == '/' || data_path.back() == '\\')) {
		data_path.pop_back();
	}
	// ensure the paths we store always end in a path separator
	data_path += separator;
	catalog.Separator() = separator;

	options.effective_data_path = data_path + options.catalog_name + separator;
}

void DuckLakeInitializer::LoadExistingDuckLake(DuckLakeTransaction &transaction) {
	// load the data path from the existing duck lake
	auto &metadata_manager = transaction.GetMetadataManager();
	auto metadata = metadata_manager.LoadDuckLake();
	DuckLakeVersion resolved_version = DuckLakeVersion::UNSET;
	for (auto &tag : metadata.tags) {
		if (tag.key == "version") {
			if (tag.value != "1.1-dev1-catalog1") {
				throw InvalidInputException("DuckLake requires Crucible metadata version 1.1-dev1-catalog1; found %s",
				                            tag.value);
			}
			resolved_version = DuckLakeVersion::V1_1_DEV_1;
		}
		if (tag.key == "data_path") {
			if (options.data_path.empty()) {
				options.data_path = metadata_manager.LoadPath(tag.value);
				InitializeDataPath();
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
	if (resolved_version == DuckLakeVersion::UNSET) {
		throw InvalidInputException("DuckLake metadata version is missing; provision metadata with Crucible.");
	}
	auto catalog_result = transaction.Query("SELECT catalog_name FROM {METADATA_CATALOG}.ducklake_catalog "
	                                        "WHERE catalog_id = {CATALOG_ID} AND end_snapshot IS NULL");
	if (catalog_result->HasError()) {
		catalog_result->GetErrorObject().Throw("Failed to resolve CATALOG_ID: ");
	}
	auto chunk = catalog_result->Fetch();
	if (!chunk || chunk->size() != 1 || chunk->GetValue(0, 0).IsNull()) {
		throw InvalidInputException("CATALOG_ID does not identify an active catalog");
	}
	auto catalog_name = chunk->GetValue(0, 0).GetValue<string>();
	if (!options.catalog_name.empty() && options.catalog_name != catalog_name) {
		throw InvalidInputException("CATALOG does not match CATALOG_ID");
	}
	options.catalog_name = catalog_name;
	InitializeDataPath();
	// set correct version metadata manager
	if (resolved_version != DuckLakeVersion::UNSET) {
		SetVersionedMetadataManager(transaction, resolved_version);
	}
}

DuckLakeVersion DuckLakeInitializer::ResolveTargetVersion(DuckLakeVersion catalog_version,
                                                          const string &catalog_version_str) {
	if (options.ducklake_version != DuckLakeVersion::UNSET) {
		// If the user pinned a version, we use that
		return options.ducklake_version;
	}
	if (options.automatic_migration) {
		// If automatic_migration is on, use to latest
		return DUCKLAKE_LATEST_VERSION;
	}
	if (catalog_version >= DuckLakeVersion::V1_0) {
		// otherwise, use the catalog's current version (must be >= V1_0)
		return catalog_version;
	}
	// pre-1.0 catalogs always require migration
	throw InvalidInputException("DuckLake catalog version mismatch: catalog version is %s, but the extension requires "
	                            "version %s. To automatically migrate, set AUTOMATIC_MIGRATION to TRUE when attaching.",
	                            catalog_version_str, DuckLakeVersionToString(DUCKLAKE_LATEST_VERSION));
}

void DuckLakeInitializer::SetVersionedMetadataManager(DuckLakeTransaction &transaction, DuckLakeVersion version) {
	catalog.SetDuckLakeVersion(version);
	if (version == DuckLakeVersion::V1_0) {
		// base metadata managers are already V1.0, nop
		return;
	}
	auto &current = transaction.GetMetadataManager();
	unique_ptr<DuckLakeMetadataManager> new_manager;
	if (version == DuckLakeVersion::V1_1_DEV_1) {
		if (dynamic_cast<QuackMetadataManager *>(&current)) {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<QuackMetadataManager>>(transaction);
		} else if (dynamic_cast<PostgresMetadataManager *>(&current)) {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<PostgresMetadataManager>>(transaction);
		} else if (dynamic_cast<SQLiteMetadataManager *>(&current)) {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<SQLiteMetadataManager>>(transaction);
		} else {
			new_manager = make_uniq<DuckLakeMetadataManagerV1_1<DuckLakeMetadataManager>>(transaction);
		}
	} else {
		throw InternalException("SetVersionedMetadataManager: unsupported version");
	}
	transaction.SetMetadataManager(std::move(new_manager));
}

} // namespace duckdb
