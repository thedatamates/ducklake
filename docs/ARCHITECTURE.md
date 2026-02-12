# DuckLake Architecture

This document provides a comprehensive overview of DuckLake's internal architecture, component relationships, and data flow.

---

## High-Level Overview

DuckLake is a **storage extension** for DuckDB that implements a lakehouse format. It intercepts DuckDB's catalog and query operations to store data in Parquet files while tracking metadata in a separate database (DuckDB file or PostgreSQL).

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Query                                      │
│                    SELECT * FROM ducklake.main.orders                        │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼───────────────────────────────────────┐
│                              DuckDB Core                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Parser    │→ │   Binder    │→ │  Planner    │→ │  Executor   │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ Catalog lookups, plan hooks
┌─────────────────────────────────────▼───────────────────────────────────────┐
│                           DuckLake Extension                                 │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                        DuckLakeCatalog                                │  │
│  │  • Intercepts CREATE TABLE, DROP TABLE, ALTER TABLE                  │  │
│  │  • Routes to DuckLakeSchemaEntry, DuckLakeTableEntry                 │  │
│  │  • Hooks PlanInsert, PlanUpdate, PlanDelete                          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│  ┌──────────────────────────────────▼───────────────────────────────────┐  │
│  │                      DuckLakeTransaction                              │  │
│  │  • Manages transaction state and snapshot versioning                  │  │
│  │  • Tracks local changes (new files, deletes, schema changes)         │  │
│  │  • Commits changes to metadata via DuckLakeMetadataManager           │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                      │                                       │
│  ┌──────────────────┐  ┌─────────────▼─────────────┐  ┌─────────────────┐  │
│  │ Physical Ops     │  │ DuckLakeMetadataManager   │  │ MultiFileReader │  │
│  │ • DuckLakeInsert │  │ • SQL generation          │  │ • Parquet scan  │  │
│  │ • DuckLakeUpdate │  │ • Schema queries          │  │ • File filtering│  │
│  │ • DuckLakeDelete │  │ • Snapshot management     │  │ • Delete files  │  │
│  └────────┬─────────┘  └─────────────┬─────────────┘  └────────┬────────┘  │
│           │                          │                          │           │
└───────────┼──────────────────────────┼──────────────────────────┼───────────┘
            │                          │                          │
            ▼                          ▼                          ▼
┌───────────────────────┐  ┌───────────────────────┐  ┌───────────────────────┐
│   Parquet Files       │  │   Metadata Database   │  │   Parquet Files       │
│   (DATA_PATH)         │  │   (DuckDB/PostgreSQL) │  │   (read via parquet   │
│   • Written by Copy   │  │   • ducklake_snapshot │  │    extension)         │
│   • Delete files      │  │   • ducklake_table    │  │                       │
└───────────────────────┘  │   • ducklake_data_file│  └───────────────────────┘
                           └───────────────────────┘
```

---

## Extension Entry Point

### Registration (`ducklake_extension.cpp`)

When DuckDB loads the extension:

```cpp
void LoadInternal(ExtensionLoader &loader) {
    // 1. Register storage extension (enables ATTACH 'ducklake:...')
    config.storage_extensions["ducklake"] = make_uniq<DuckLakeStorageExtension>();

    // 2. Register configuration options
    config.AddExtensionOption("ducklake_max_retry_count", ...);
    config.AddExtensionOption("ducklake_retry_wait_ms", ...);

    // 3. Register table functions
    loader.RegisterFunction(DuckLakeSnapshotsFunction());
    loader.RegisterFunction(DuckLakeExpireSnapshotsFunction());
    loader.RegisterFunction(DuckLakeForkCatalogFunction());
    // ... 15+ more functions

    // 4. Register secrets for connection management
    loader.RegisterSecretType(DuckLakeSecret::GetSecretType());
}
```

### Storage Extension (`ducklake_storage.cpp`)

The `DuckLakeStorageExtension` provides two callbacks:

```cpp
DuckLakeStorageExtension::DuckLakeStorageExtension() {
    attach = DuckLakeAttach;                          // Called on ATTACH
    create_transaction_manager = DuckLakeCreateTransactionManager;
}
```

**Attach Flow:**
```
ATTACH 'ducklake:metadata.db' AS lake (CATALOG 'test', DATA_PATH '/data/')
    │
    ▼
DuckLakeAttach()
    │
    ├── Parse options (CATALOG, DATA_PATH, METADATA_SCHEMA, etc.)
    ├── Handle secrets if path is a secret name
    ├── Create DuckLakeCatalog
    └── Return catalog to DuckDB
```

---

## Catalog System

### DuckLakeCatalog

The catalog is the central coordinator. It implements DuckDB's `Catalog` interface.

**Key Responsibilities:**

| Method | Purpose |
|--------|---------|
| `Initialize()` | Attach metadata database, load or create DuckLake |
| `CreateSchema()` | Create new schema in catalog |
| `LookupSchema()` | Find schema by name |
| `ScanSchemas()` | Iterate all schemas |
| `PlanInsert()` | Generate DuckLakeInsert operator |
| `PlanUpdate()` | Generate DuckLakeUpdate operator |
| `PlanDelete()` | Generate DuckLakeDelete operator |
| `GetDatabaseSize()` | Calculate total data size |

**State Management:**
```cpp
class DuckLakeCatalog : public Catalog {
    DuckLakeOptions options;              // CATALOG, DATA_PATH, etc.

    // Schema cache (snapshot_id -> catalog set)
    unordered_map<idx_t, unique_ptr<DuckLakeCatalogSet>> schemas;

    // Table stats cache
    unordered_map<idx_t, unique_ptr<DuckLakeStats>> stats;

    // Name mapping cache (for column evolution)
    DuckLakeNameMapSet name_maps;

    // Committed snapshot tracking
    optional_idx last_committed_snapshot;
};
```

### Catalog Entries

Each database object has a corresponding entry class:

| Entry Class | DuckDB Base | Purpose |
|-------------|-------------|---------|
| `DuckLakeSchemaEntry` | `SchemaCatalogEntry` | Schema with ducklake-specific metadata |
| `DuckLakeTableEntry` | `TableCatalogEntry` | Table with file references, partition info |
| `DuckLakeViewEntry` | `ViewCatalogEntry` | View definition with versioning |
| `DuckLakeScalarMacroEntry` | `ScalarMacroCatalogEntry` | Scalar macro |
| `DuckLakeTableMacroEntry` | `TableMacroCatalogEntry` | Table macro |

**DuckLakeTableEntry Structure:**
```cpp
class DuckLakeTableEntry : public TableCatalogEntry {
    TableIndex table_id;                           // Unique ID
    string table_uuid;                             // UUID for external reference
    shared_ptr<DuckLakeFieldData> field_data;      // Column metadata
    unique_ptr<DuckLakePartition> partition_data;  // Partitioning info
    vector<DuckLakeInlinedTableInfo> inlined_data_tables;  // Inlined data
    LocalChange local_change;                      // Transaction-local state
};
```

### DuckLakeCatalogSet

A versioned set of catalog entries for a specific snapshot:

```cpp
class DuckLakeCatalogSet {
    // Entries by type
    case_insensitive_map_t<unique_ptr<CatalogEntry>> schemas;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> tables;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> views;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> scalar_macros;
    case_insensitive_map_t<unique_ptr<CatalogEntry>> table_macros;
};
```

---

## Transaction System

### DuckLakeTransactionManager

Creates and manages transactions:

```cpp
class DuckLakeTransactionManager : public TransactionManager {
    Transaction &StartTransaction(ClientContext &context) override;
    ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
    void RollbackTransaction(Transaction &transaction) override;
};
```

### DuckLakeTransaction

Each transaction maintains:

```cpp
class DuckLakeTransaction : public Transaction {
    // Parent catalog
    DuckLakeCatalog &ducklake_catalog;

    // Metadata access
    unique_ptr<DuckLakeMetadataManager> metadata_manager;

    // Connection to metadata database
    unique_ptr<Connection> connection;

    // Current snapshot (lazily loaded)
    unique_ptr<DuckLakeSnapshot> snapshot;

    // Transaction-local changes
    case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> new_tables;
    unique_ptr<DuckLakeCatalogSet> new_schemas;
    set<TableIndex> dropped_tables;
    map<TableIndex, LocalTableDataChanges> table_data_changes;

    // Commit metadata
    DuckLakeSnapshotCommit commit_info;
};
```

### Transaction Lifecycle

```
BEGIN (implicit or explicit)
    │
    ▼
DuckLakeTransactionManager::StartTransaction()
    │
    ├── Create DuckLakeTransaction
    ├── Create DuckLakeMetadataManager (DuckDB or PostgreSQL)
    └── Return transaction
    │
    ▼
[User operations: INSERT, UPDATE, DELETE, CREATE TABLE, etc.]
    │
    ├── Changes accumulated in transaction-local state
    ├── Files written to DATA_PATH
    └── Metadata not yet committed
    │
    ▼
COMMIT
    │
    ▼
DuckLakeTransaction::Commit()
    │
    ├── FlushChanges()
    │   ├── Check for conflicts with other transactions
    │   ├── Allocate new snapshot_id
    │   ├── Write snapshot record
    │   ├── Write new schema/table/view records
    │   ├── Write new data_file records
    │   └── Write new delete_file records
    └── CleanupFiles() if commit fails
```

### Conflict Detection

Before committing, DuckLake checks for conflicts:

```cpp
SnapshotAndStats CheckForConflicts(DuckLakeSnapshot transaction_snapshot,
                                   const TransactionChangeInformation &changes) {
    // 1. Get all snapshots since our transaction started
    auto newer_snapshots = GetSnapshotsSince(transaction_snapshot);

    // 2. For each newer snapshot, check for conflicts
    for (auto &snapshot : newer_snapshots) {
        auto other_changes = GetChangesForSnapshot(snapshot);

        // Check: Did another transaction modify tables we modified?
        // Check: Did another transaction drop tables we read?
        // Check: Did another transaction alter schema we depend on?
        CheckForConflicts(changes, other_changes, transaction_snapshot);
    }

    // 3. Return latest snapshot + stats for commit
    return {latest_stats, latest_snapshot};
}
```

---

## Metadata Manager

### DuckLakeMetadataManager

The abstraction layer for metadata storage. All SQL queries go through here.

```cpp
class DuckLakeMetadataManager {
    DuckLakeTransaction &transaction;  // Parent transaction

    // Query execution
    virtual unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query);
    virtual unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query);

    // Schema management
    void CreateDuckLakeSchema(DuckLakeEncryption encryption);
    void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption);

    // Catalog operations
    idx_t CreateCatalog(const string &catalog_name);
    optional_idx LookupCatalogByName(const string &catalog_name);

    // Data loading
    DuckLakeCatalogSet LoadCatalogForSnapshot(DuckLakeSnapshot snapshot);
    vector<DuckLakeFileListEntry> LoadFilesForTable(TableIndex table_id, DuckLakeSnapshot snapshot);
    DuckLakeTableStats LoadTableStats(TableIndex table_id);
};
```

### PostgresMetadataManager

Subclass for PostgreSQL backend:

```cpp
class PostgresMetadataManager : public DuckLakeMetadataManager {
    // Uses postgres_query() / postgres_execute() instead of DuckDB connection
    unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
    unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;

    // Different type handling
    bool TypeIsNativelySupported(const LogicalType &type) override;
};
```

### Query Placeholder System

All queries use placeholders replaced at execution time:

| Placeholder | Purpose | Example Value |
|-------------|---------|---------------|
| `{CATALOG_ID}` | Current catalog ID | `0` |
| `{CATALOG_NAME}` | Catalog name (quoted) | `'my-catalog'` |
| `{METADATA_CATALOG}` | Full schema path | `__ducklake_meta.public` |
| `{SNAPSHOT_ID}` | Current snapshot | `42` |
| `{NEXT_CATALOG_ID}` | Next entity ID | `15` |
| `{NEXT_FILE_ID}` | Next file ID | `100` |

**Example Query:**
```sql
-- Before replacement
SELECT * FROM {METADATA_CATALOG}.ducklake_table
WHERE catalog_id = {CATALOG_ID} AND end_snapshot IS NULL

-- After replacement
SELECT * FROM __ducklake_meta.public.ducklake_table
WHERE catalog_id = 0 AND end_snapshot IS NULL
```

---

## Physical Operators

DuckLake provides custom physical operators for data modification.

### DuckLakeInsert

Handles INSERT and CREATE TABLE AS:

```cpp
class DuckLakeInsert : public PhysicalOperator {
    optional_ptr<DuckLakeTableEntry> table;    // Target table (INSERT INTO)
    unique_ptr<BoundCreateTableInfo> info;      // Or create info (CREATE TABLE AS)
    optional_idx partition_id;                  // Partition if partitioned table
    string encryption_key;                      // For encrypted tables

    // Sink interface - receives data from child operator
    SinkResultType Sink(ExecutionContext &context, DataChunk &chunk,
                        OperatorSinkInput &input) const override;

    // Finalize - commit files to transaction
    SinkFinalizeType Finalize(Pipeline &pipeline, Event &event,
                              ClientContext &context, ...) const override;
};
```

**Insert Data Flow:**
```
┌─────────────┐
│ Child Op    │  (e.g., VALUES, SELECT)
│ (DataChunk) │
└──────┬──────┘
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│                     DuckLakeInsert                            │
│                                                               │
│  Sink()                                                       │
│    └── Forward chunks to PhysicalCopyToFile (Parquet writer) │
│                                                               │
│  Finalize()                                                   │
│    ├── Collect file metadata from CopyToFile                 │
│    ├── Build DuckLakeDataFile records                        │
│    ├── Collect column statistics                             │
│    └── Append files to transaction.table_data_changes        │
└──────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────┐     ┌─────────────────────┐
│  Parquet File       │     │ Transaction State   │
│  (on disk)          │     │ (in memory)         │
│                     │     │                     │
│  /data/catalog/     │     │ new_data_files:     │
│    schema/table/    │     │   - file.parquet    │
│      file.parquet   │     │   - row_count: 1000 │
└─────────────────────┘     └─────────────────────┘
```

### DuckLakeUpdate

Copy-on-write updates:

```cpp
class DuckLakeUpdate : public PhysicalOperator {
    // 1. Read rows to update (via child operator)
    // 2. Write delete file marking old rows
    // 3. Write new parquet file with updated values
    // 4. Register both in transaction
};
```

### DuckLakeDelete

Handles DELETE and TRUNCATE:

```cpp
class DuckLakeDelete : public PhysicalOperator {
    // For partial deletes:
    //   1. Create delete file with row IDs
    //   2. Register in transaction

    // For full file deletes:
    //   1. Mark file with end_snapshot (no delete file needed)
};
```

### DuckLakeCompaction

Merges small files and removes deleted rows:

```cpp
class DuckLakeCompaction : public PhysicalOperator {
    CompactionType type;  // MERGE_ADJACENT or REWRITE

    // MERGE_ADJACENT: Combine small files
    // REWRITE: Remove deleted rows from files
};
```

---

## File Handling

### DuckLakeMultiFileList

Provides the list of files to scan for a table:

```cpp
class DuckLakeMultiFileList : public MultiFileList {
    DuckLakeFunctionInfo &read_info;          // Table and snapshot info
    vector<DuckLakeFileListEntry> files;       // Files from metadata
    vector<DuckLakeDataFile> transaction_local_files;  // Uncommitted files
    shared_ptr<DuckLakeInlinedData> transaction_local_data;  // Inlined data

    vector<OpenFileInfo> GetAllFiles() override;
    unique_ptr<MultiFileList> ComplexFilterPushdown(...) override;
};
```

### DuckLakeMultiFileReader

Customizes parquet reading:

```cpp
class DuckLakeMultiFileReader : public MultiFileReader {
    // Filter pushdown to skip files based on statistics
    // Handle delete files (skip deleted rows)
    // Column mapping for schema evolution
};
```

### File List Entry

```cpp
struct DuckLakeFileListEntry {
    DataFileIndex data_file_id;
    string path;
    idx_t file_size_bytes;
    idx_t record_count;
    optional_idx partition_id;
    string encryption_key;
    MappingIndex mapping_id;
    DuckLakeSnapshot begin_snapshot;
    optional_idx end_snapshot;

    // Delete file info (if any)
    optional_ptr<DuckLakeFileData> delete_data;

    // Column statistics for filter pushdown
    map<FieldIndex, DuckLakeColumnStats> column_stats;
};
```

### Read Path

```
SELECT * FROM ducklake.main.orders WHERE amount > 100
    │
    ▼
DuckLakeCatalog.LookupSchema("main")
    │
    ▼
DuckLakeSchemaEntry.GetTable("orders")
    │
    ▼
DuckLakeTableEntry.GetScanFunction()
    │
    ▼
DuckLakeFunctions::BindDuckLakeScan()
    │
    ├── Get current snapshot from transaction
    ├── Create DuckLakeFunctionInfo
    └── Return parquet_scan with DuckLakeMultiFileList
    │
    ▼
DuckLakeMultiFileList.GetAllFiles()
    │
    ├── Query metadata: SELECT * FROM ducklake_data_file
    │     WHERE table_id = ? AND catalog_id = ?
    │       AND begin_snapshot <= ?
    │       AND (end_snapshot IS NULL OR end_snapshot > ?)
    │
    ├── Add transaction-local files (uncommitted inserts)
    │
    └── Apply filter pushdown (skip files where max(amount) < 100)
    │
    ▼
Parquet reader scans matching files
    │
    ├── For each file with delete file:
    │   └── Load delete file, skip deleted row IDs
    │
    └── Apply column mapping if schema evolved
```

---

## Table Functions

### Categories

**Metadata Query Functions:**
| Function | Purpose |
|----------|---------|
| `ducklake_snapshots(catalog)` | List all snapshots |
| `ducklake_current_snapshot(catalog)` | Get current snapshot ID |
| `ducklake_last_committed_snapshot(catalog)` | Get last committed snapshot |
| `ducklake_catalogs(catalog)` | List all catalogs in metadata database |
| `ducklake_options(catalog)` | List configuration options |
| `ducklake_list_files(catalog, table)` | List files for a table |
| `ducklake_table_info(catalog, table)` | Get table metadata |

**Change Tracking Functions:**
| Function | Purpose |
|----------|---------|
| `ducklake_table_changes(catalog, schema, table, start, end)` | All changes between snapshots |
| `ducklake_table_insertions(catalog, schema, table, start, end)` | Inserted rows |
| `ducklake_table_deletions(catalog, schema, table, start, end)` | Deleted rows |

**Maintenance Functions:**
| Function | Purpose |
|----------|---------|
| `ducklake_expire_snapshots(catalog, ...)` | Mark old snapshots as expired |
| `ducklake_cleanup_old_files(catalog)` | Delete files from expired snapshots |
| `ducklake_delete_orphaned_files(catalog)` | Delete unreferenced files |
| `ducklake_merge_adjacent_files(catalog, table)` | Compact small files |
| `ducklake_rewrite_data_files(catalog, table)` | Remove deleted rows |
| `ducklake_flush_inlined_data(catalog)` | Write inlined data to parquet |

**Configuration Functions:**
| Function | Purpose |
|----------|---------|
| `ducklake_set_option(catalog, option, value)` | Set configuration option |
| `ducklake_set_commit_message(catalog, author, message)` | Set commit metadata |

**Data Management Functions:**
| Function | Purpose |
|----------|---------|
| `ducklake_add_data_files(catalog, table, path)` | Register external parquet files |

### Implementation Pattern

Most table functions follow this pattern:

```cpp
class DuckLakeSomeFunction : public TableFunction {
    DuckLakeSomeFunction() {
        name = "ducklake_some_function";
        arguments = {LogicalType::VARCHAR, ...};
        bind = Bind;
        function = Execute;
    }

    static unique_ptr<FunctionData> Bind(ClientContext &context,
                                          TableFunctionBindInput &input, ...) {
        // 1. Get catalog from first argument
        auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);

        // 2. Get transaction
        auto &transaction = DuckLakeTransaction::Get(context, catalog);

        // 3. Query metadata
        auto result = transaction.Query("SELECT ... FROM ...");

        // 4. Return bind data with results
        return make_uniq<BindData>(std::move(result));
    }

    static void Execute(ClientContext &context, TableFunctionInput &input,
                        DataChunk &output) {
        // Return rows from bind data
    }
};
```

---

## Data Inlining

For small tables, DuckLake can store data directly in metadata instead of parquet files.

### Configuration

```sql
CALL ducklake_set_option('lake', 'data_inlining_row_limit', 1000);
```

### Storage

```cpp
class DuckLakeInlinedData {
    ColumnDataCollection collection;  // Actual row data

    void Append(DataChunk &chunk);
    void Scan(DataChunk &result);
};

struct DuckLakeInlinedTableInfo {
    TableIndex table_id;
    string table_name;
    idx_t schema_version;
};
```

### Inlined Data Tables

Stored in `ducklake_inlined_data_tables`:
```sql
CREATE TABLE ducklake_inlined_data_tables (
    catalog_id BIGINT,
    table_id BIGINT,
    table_name VARCHAR,  -- Actually stores serialized data
    schema_version BIGINT
);
```

The `table_name` column contains the serialized `ColumnDataCollection`.

---

## Statistics

### Column Statistics

Stored per-file for filter pushdown:

```cpp
struct DuckLakeColumnStats {
    Value min_value;
    Value max_value;
    bool has_null;
    bool has_nan;
    unique_ptr<DuckLakeColumnExtraStats> extra_stats;  // e.g., string length stats
};
```

### Table Statistics

Aggregate stats stored in `ducklake_table_stats`:

```cpp
struct DuckLakeTableStats {
    TableIndex table_id;
    idx_t record_count;
    idx_t next_row_id;
    idx_t file_size_bytes;
};
```

### Filter Pushdown

The `DuckLakeMultiFileList::ComplexFilterPushdown` uses column statistics to skip files:

```cpp
unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context,
                                                 const MultiFileOptions &options,
                                                 MultiFilePushdownInfo &info,
                                                 vector<unique_ptr<Expression>> &filters) {
    vector<DuckLakeFileListEntry> filtered_files;

    for (auto &file : files) {
        bool can_skip = false;

        for (auto &filter : filters) {
            // Check if file's column stats allow skipping
            // e.g., if filter is "x > 100" and file's max(x) = 50, skip file
            if (CanSkipFile(file, filter)) {
                can_skip = true;
                break;
            }
        }

        if (!can_skip) {
            filtered_files.push_back(file);
        }
    }

    return make_uniq<DuckLakeMultiFileList>(..., filtered_files);
}
```

---

## Partitioning

### Partition Definition

```sql
CREATE TABLE events (
    event_date DATE,
    event_type VARCHAR,
    data VARCHAR
) PARTITION BY (year(event_date), month(event_date));
```

### Partition Storage

```cpp
struct DuckLakePartition {
    PartitionIndex partition_id;
    vector<DuckLakePartitionColumn> columns;
};

struct DuckLakePartitionColumn {
    idx_t partition_key_index;
    FieldIndex column_id;
    string transform;  // "identity", "year", "month", "day", "hour"
};
```

### File Partition Values

Each file records its partition values:

```sql
CREATE TABLE ducklake_file_partition_value (
    catalog_id BIGINT,
    data_file_id BIGINT,
    table_id BIGINT,
    partition_key_index BIGINT,
    partition_value VARCHAR
);
```

### Partition Pruning

During scan, files are filtered by partition values before reading.

---

## Encryption

### Enabling Encryption

```sql
ATTACH 'ducklake:...' AS lake (CATALOG 'x', ENCRYPTED true);
```

### Key Management

```cpp
string DuckLakeCatalog::GenerateEncryptionKey(ClientContext &context) const {
    if (options.encryption != DuckLakeEncryption::ENCRYPTED) {
        return "";
    }
    // Generate random key for each file
    return GenerateRandomKey();
}
```

### Storage

Encryption keys are stored per-file in `ducklake_data_file.encryption_key`.

Parquet files are encrypted using DuckDB's parquet encryption support.

---

## Snapshot and Time Travel

### Snapshot Structure

```cpp
struct DuckLakeSnapshot {
    idx_t snapshot_id;
    idx_t schema_version;
    idx_t next_catalog_id;  // Counter for entity IDs
    idx_t next_file_id;     // Counter for file IDs
};
```

### Time Travel Query

```sql
SELECT * FROM table AT (VERSION => 5);
SELECT * FROM table AT (TIMESTAMP => '2024-01-01');
```

### Implementation

```cpp
DuckLakeSnapshot DuckLakeTransaction::GetSnapshot(optional_ptr<BoundAtClause> at_clause) {
    if (!at_clause) {
        return GetLatestSnapshot();
    }

    if (at_clause->type == "version") {
        return GetSnapshotByVersion(at_clause->value.GetValue<idx_t>());
    } else {
        return GetSnapshotByTimestamp(at_clause->value.GetValue<timestamp_t>());
    }
}
```

---

## Component Relationships

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DuckLakeExtension                              │
│                                                                          │
│  Registers:                                                              │
│    • DuckLakeStorageExtension                                           │
│    • 17 TableFunctions                                                  │
│    • DuckLakeSecret                                                     │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ▼                         ▼                         ▼
┌───────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐
│DuckLakeStorage    │  │ Table Functions     │  │ DuckLakeSecret       │
│Extension          │  │                     │  │                      │
│                   │  │ • Snapshots         │  │ • Connection secrets │
│ • attach()        │  │ • Expire            │  └──────────────────────┘
│ • create_tx_mgr() │  │ • Fork              │
└─────────┬─────────┘  │ • Compact           │
          │            │ • etc.              │
          ▼            └─────────────────────┘
┌───────────────────┐
│ DuckLakeCatalog   │◄─────────────────────────────────────────────────┐
│                   │                                                   │
│ Contains:         │                                                   │
│ • Options         │                                                   │
│ • Schema cache    │                                                   │
│ • Stats cache     │                                                   │
│ • Name maps       │                                                   │
└─────────┬─────────┘                                                   │
          │                                                             │
          │ Creates                                                     │
          ▼                                                             │
┌───────────────────────────────────────────────┐                      │
│ DuckLakeTransactionManager                    │                      │
│                                               │                      │
│ Creates DuckLakeTransaction for each session  │                      │
└─────────────────────┬─────────────────────────┘                      │
                      │                                                 │
                      ▼                                                 │
┌─────────────────────────────────────────────────────────────────────┐│
│ DuckLakeTransaction                                                  ││
│                                                                      ││
│ Contains:                                                            ││
│ • DuckLakeMetadataManager (DuckDB or PostgreSQL)                    ││
│ • Connection to metadata database                                    ││
│ • Snapshot state                                                     ││
│ • Transaction-local changes                                          ││
│                                                                      ││
│ Methods:                                                             ││
│ • Query() - Execute SQL on metadata                                  ││
│ • Commit() - Flush changes to metadata                               ││
│ • CreateEntry(), DropEntry(), AlterEntry()                          ││
│ • AppendFiles(), AddDeletes()                                        ││
└─────────────────────────────────────────────────────────────────────┘│
          │                                                             │
          │ Uses                                                        │
          ▼                                                             │
┌─────────────────────────────────────────────────────────────────────┐│
│ DuckLakeMetadataManager                                              ││
│                                                                      ││
│ • SQL generation for all metadata operations                         ││
│ • Schema creation (CREATE TABLE IF NOT EXISTS ...)                  ││
│ • Catalog CRUD                                                       ││
│ • File metadata CRUD                                                 ││
│ • Snapshot management                                                ││
└─────────────────────────────────────────────────────────────────────┘│
          │                                                             │
          │ Subclassed by                                               │
          ▼                                                             │
┌─────────────────────────────────────────────────────────────────────┐│
│ PostgresMetadataManager                                              ││
│                                                                      ││
│ • Uses postgres_query()/postgres_execute() instead of Connection    ││
│ • Different type handling                                            ││
│ • Requires pre-created schema                                        ││
└─────────────────────────────────────────────────────────────────────┘│
                                                                        │
┌─────────────────────────────────────────────────────────────────────┐│
│ Catalog Entries                                           References ││
│                                                                      ││
│ DuckLakeSchemaEntry ──► Contains DuckLakeTableEntry, ViewEntry, etc.││
│ DuckLakeTableEntry ───► References DuckLakeCatalog ─────────────────┘│
│ DuckLakeViewEntry                                                    │
│ DuckLakeMacroEntry                                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Physical Operators                                                   │
│                                                                      │
│ DuckLakeInsert ──► Writes Parquet via PhysicalCopyToFile            │
│ DuckLakeUpdate ──► Delete old + Insert new                          │
│ DuckLakeDelete ──► Creates delete files                             │
│ DuckLakeCompaction ──► Merges/rewrites files                        │
│                                                                      │
│ All update Transaction.table_data_changes                           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ File Handling                                                        │
│                                                                      │
│ DuckLakeMultiFileList ──► Provides files for parquet_scan           │
│ DuckLakeMultiFileReader ──► Customizes parquet reading               │
│ DuckLakeDeleteFilter ──► Filters deleted rows                       │
│ DuckLakeInlinedDataReader ──► Reads inlined data                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## File Summary

| Directory | Files | Purpose |
|-----------|-------|---------|
| `src/` | 1 | Extension entry point |
| `src/storage/` | 25 | Core components (catalog, transaction, operators) |
| `src/functions/` | 15 | Table functions |
| `src/common/` | 5 | Shared utilities and types |
| `src/metadata_manager/` | 1 | PostgreSQL backend |
| `src/include/` | 42 | Header files |

**Total: ~18,600 lines of C++**

---

## Key Design Decisions

1. **Metadata in separate database**: Enables PostgreSQL for production, DuckDB for dev/test
2. **Global snapshots**: Single snapshot sequence enables forking without ID conflicts
3. **Copy-on-write**: Files never modified, enables time travel
4. **Parquet via DuckDB's copy function**: Reuses DuckDB's optimized parquet writer
5. **Column statistics per file**: Enables filter pushdown without reading data
6. **Delete files**: Avoid rewriting large files for small deletes
7. **Transaction-local state**: Changes visible only to current transaction until commit
8. **Placeholder-based SQL**: Same queries work for DuckDB and PostgreSQL backends
