# Building DuckLake in Rust: Feasibility Research

This document analyzes whether DuckLake could be built in Rust instead of C++, what the challenges are, and what architecture would be required.

## Executive Summary

**Can DuckLake be built in Rust?** Yes, but with significant caveats.

**The fundamental problem:** DuckDB's extension system requires **subclassing C++ classes with virtual methods**. Rust cannot directly inherit from C++ classes. A hybrid architecture is required.

**Recommended approach:** Thin C++ shim (~2000-3000 lines) that subclasses DuckDB interfaces and forwards all calls to Rust via `cxx`. Core logic (~15,000 lines) written in Rust.

---

## Current State

### DuckLake is 18,600 Lines of C++

| File | Lines | Purpose |
|------|-------|---------|
| `ducklake_metadata_manager.cpp` | 3,711 | SQL generation, schema management |
| `ducklake_transaction.cpp` | 2,487 | Transaction handling, commits |
| `ducklake_table_entry.cpp` | 1,214 | Table catalog entry |
| `ducklake_add_data_files.cpp` | 1,177 | Add files function |
| `ducklake_insert.cpp` | 824 | Insert physical operator |
| `ducklake_catalog.cpp` | 797 | Catalog implementation |
| Other files | ~8,390 | Delete, update, compaction, etc. |

### What's Available in Rust Today

The [duckdb-rs](https://github.com/duckdb/duckdb-rs) crate and [extension-template-rs](https://github.com/duckdb/extension-template-rs) provide:

```rust
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
```

**That's it.** The Rust bindings expose:
- Table-valued functions (`VTab` trait)
- Scalar functions
- Basic types (LogicalType, DataChunk)
- Connection for running queries

**Not exposed:**
- `Catalog` class
- `CatalogEntry` subclasses (Table, Schema, View, Macro)
- `Transaction` class
- `TransactionManager` class
- `StorageExtension` class
- `PhysicalOperator` class
- Query planning hooks
- Any internal DuckDB APIs

---

## What DuckLake Needs

### Classes That Must Be Subclassed (32 total)

DuckLake subclasses 32 DuckDB classes. These require a C++ shim because **Rust cannot inherit from C++ classes**.

**Core Infrastructure (3):**
```cpp
class DucklakeExtension : public Extension
class DuckLakeStorageExtension : public StorageExtension
class DuckLakeTransactionManager : public TransactionManager
```

**Catalog System (7):**
```cpp
class DuckLakeCatalog : public Catalog
class DuckLakeSchemaEntry : public SchemaCatalogEntry
class DuckLakeTableEntry : public TableCatalogEntry
class DuckLakeViewEntry : public ViewCatalogEntry
class DuckLakeScalarMacroEntry : public ScalarMacroCatalogEntry
class DuckLakeTableMacroEntry : public TableMacroCatalogEntry
class DuckLakeTransaction : public Transaction
```

**Physical Operators (6):**
```cpp
class DuckLakeInsert : public PhysicalOperator
class DuckLakeUpdate : public PhysicalOperator
class DuckLakeDelete : public PhysicalOperator
class DuckLakeCompaction : public PhysicalOperator
class DuckLakeFlushData : public PhysicalOperator
class DuckLakeInlineData : public PhysicalOperator
```

**Table Functions (12+):**
```cpp
class DuckLakeSnapshotsFunction : public TableFunction
class DuckLakeExpireSnapshotsFunction : public TableFunction
class DuckLakeForkCatalogFunction : public TableFunction
// ... 9 more
```

**Other (4):**
```cpp
class DuckLakeMultiFileList : public MultiFileList
class DuckLakeInlinedDataReader : public BaseFileReader
class DuckLakeDeleteFilter : public DeleteFilter
class DuckLakeInsertGlobalState : public GlobalSinkState
```

### Virtual Methods to Override (148 total)

DuckLake overrides 148 virtual methods across these classes. Examples:

**Catalog (20+ methods):**
```cpp
void Initialize(bool load_builtin) override;
optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction, CreateSchemaInfo&) override;
void ScanSchemas(ClientContext&, std::function<void(SchemaCatalogEntry&)>) override;
unique_ptr<PhysicalOperator> PlanInsert(ClientContext&, LogicalInsert&, ...) override;
unique_ptr<PhysicalOperator> PlanDelete(ClientContext&, LogicalDelete&, ...) override;
unique_ptr<PhysicalOperator> PlanUpdate(ClientContext&, LogicalUpdate&, ...) override;
DatabaseSize GetDatabaseSize(ClientContext&) override;
// ... 13 more
```

**PhysicalOperator (10+ methods per operator):**
```cpp
SinkResultType Sink(ExecutionContext&, DataChunk&, OperatorSinkInput&) override;
SinkFinalizeType Finalize(Pipeline&, Event&, ClientContext&, ...) override;
unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext&) override;
unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext&) override;
SourceResultType GetData(ExecutionContext&, DataChunk&, OperatorSourceInput&) override;
bool IsSink() override;
bool ParallelSink() override;
string GetName() override;
// ... more
```

**Transaction (3 methods):**
```cpp
void Start() override;
void Commit() override;
void Rollback() override;
```

---

## The Architecture Gap

### Why Pure Rust Won't Work

DuckDB's C API (what the Rust crate wraps) is designed for **clients**, not **extensions**:

```c
// C API - for running queries
duckdb_open(path, &db);
duckdb_connect(db, &conn);
duckdb_query(conn, "SELECT * FROM t", &result);
```

Extensions need the **C++ internal API** to hook into:
- Catalog management (CREATE TABLE intercepted)
- Query planning (generate custom physical operators)
- Transaction lifecycle (BEGIN/COMMIT/ROLLBACK)
- Storage layer (where data lives)

These are not exposed via C API or Rust bindings.

### The Inheritance Problem

DuckDB uses **virtual methods** for extensibility:

```cpp
class Catalog {
public:
    virtual optional_ptr<CatalogEntry> CreateSchema(...) = 0;  // MUST override
    virtual void ScanSchemas(...) = 0;                         // MUST override
    // ...
};

class DuckLakeCatalog : public Catalog {
    optional_ptr<CatalogEntry> CreateSchema(...) override {
        // DuckLake implementation
    }
};
```

Rust doesn't have inheritance. You **cannot** write:

```rust
// THIS DOES NOT WORK
impl Catalog for DuckLakeCatalog {
    fn create_schema(&self, ...) { ... }
}
```

---

## Bridge Options

### Option 1: cxx (Recommended)

[cxx](https://cxx.rs/) provides safe C++/Rust interop. You define a bridge:

```rust
#[cxx::bridge]
mod ffi {
    extern "Rust" {
        type RustCatalogImpl;
        fn create_schema(self: &RustCatalogImpl, info: &CreateSchemaInfo) -> Result<CatalogEntry>;
        fn scan_schemas(self: &RustCatalogImpl, callback: fn(&SchemaCatalogEntry));
    }

    unsafe extern "C++" {
        include!("duckdb.hpp");
        type CreateSchemaInfo;
        type CatalogEntry;
        type SchemaCatalogEntry;
    }
}
```

C++ shim subclasses and forwards to Rust:

```cpp
class DuckLakeCatalog : public Catalog {
    rust::Box<RustCatalogImpl> impl;

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction tx, CreateSchemaInfo& info) override {
        return impl->create_schema(info);
    }
};
```

**Pros:**
- Type-safe FFI
- Rust ownership semantics preserved
- Good error handling

**Cons:**
- Manual bridge definition for each type/function
- Complex types (templates, nested classes) need work

### Option 2: autocxx

[autocxx](https://google.github.io/autocxx/) auto-generates bindings from C++ headers:

```rust
include_cpp! {
    #include "duckdb/catalog/catalog.hpp"
    safety!(unsafe)
    generate!("duckdb::Catalog")
    generate!("duckdb::CreateSchemaInfo")
}
```

**Pros:**
- Less manual work
- Handles many C++ patterns automatically

**Cons:**
- Experimental subclass support ([Issue #200](https://github.com/google/autocxx/issues/200))
- Template classes are problematic ([Issue #1356](https://github.com/google/autocxx/issues/1356))
- "Limited" support for pure virtual classes

### Option 3: Manual C Wrapper

Write a C API that wraps DuckDB's C++ classes:

```c
// ducklake_c_api.h
typedef void* DuckLakeCatalogHandle;

DuckLakeCatalogHandle ducklake_create_catalog(AttachedDatabaseHandle db);
void ducklake_catalog_create_schema(DuckLakeCatalogHandle catalog, CreateSchemaInfoHandle info);
```

**Pros:**
- Maximum control
- Works with any language

**Cons:**
- Massive manual effort
- Loses type safety
- Must maintain C++ wrapper layer

---

## Proposed Architecture

### Hybrid: C++ Shim + Rust Core

```
┌────────────────────────────────────────────────────────────────┐
│                         DuckDB                                  │
│  (calls virtual methods on Catalog, PhysicalOperator, etc.)    │
└────────────────────────────────┬───────────────────────────────┘
                                 │ virtual method calls
┌────────────────────────────────▼───────────────────────────────┐
│                    C++ Shim Layer (~2500 lines)                │
│                                                                 │
│  class DuckLakeCatalog : public Catalog {                      │
│      rust::Box<RustCatalog> impl;                              │
│      CreateSchema(...) override { impl->create_schema(...); }  │
│  }                                                              │
│                                                                 │
│  class DuckLakeInsert : public PhysicalOperator {              │
│      rust::Box<RustInsert> impl;                               │
│      Sink(...) override { impl->sink(...); }                   │
│  }                                                              │
│  // ... 30 more classes                                         │
└────────────────────────────────┬───────────────────────────────┘
                                 │ cxx bridge
┌────────────────────────────────▼───────────────────────────────┐
│                    Rust Core (~15000 lines)                    │
│                                                                 │
│  struct RustCatalog { ... }                                    │
│  impl RustCatalog {                                            │
│      fn create_schema(&self, info: &CreateSchemaInfo) { ... }  │
│  }                                                              │
│                                                                 │
│  struct MetadataManager { ... }  // SQL generation             │
│  struct Transaction { ... }      // Transaction state          │
│  struct ParquetWriter { ... }    // File writing               │
│  // All business logic in Rust                                 │
└────────────────────────────────────────────────────────────────┘
```

### What Goes Where

**C++ Shim (thin, ~2500 lines):**
- Class definitions that inherit from DuckDB
- Forward all method calls to Rust
- Handle C++ ↔ Rust type conversions
- Memory management bridge

**Rust Core (thick, ~15000 lines):**
- `MetadataManager` - SQL generation, schema queries (currently 3711 lines C++)
- `Transaction` - Transaction state, commit logic (currently 2487 lines C++)
- `CatalogLogic` - Catalog operations (currently 797 lines C++)
- `InsertOperator` - Insert execution (currently 824 lines C++)
- `ParquetIO` - Parquet reading/writing
- All table functions (snapshots, expire, fork, etc.)

### Why This Split?

Looking at `ducklake_metadata_manager.cpp` (the largest file):

```cpp
void DuckLakeMetadataManager::CreateDuckLakeSchema(DuckLakeEncryption encryption) {
    auto schema_sql = R"(
        CREATE SCHEMA IF NOT EXISTS {METADATA_CATALOG};
        CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_metadata(...);
        ...
    )";
    transaction.Query(schema_sql);  // <-- Only DuckDB dependency
}
```

This is **pure business logic**:
1. Build SQL strings
2. Execute via `transaction.Query()`
3. Process results

The only C++ dependency is calling `transaction.Query()`. That's trivially bridgeable. 90% of DuckLake's code is like this.

---

## Types to Bridge

### Simple Types (Direct mapping)

| C++ Type | Rust Type |
|----------|-----------|
| `std::string` | `String` |
| `int64_t`, `idx_t` | `i64`, `u64` |
| `bool` | `bool` |
| `vector<T>` | `Vec<T>` |
| `optional<T>` | `Option<T>` |
| `unique_ptr<T>` | `Box<T>` or `cxx::UniquePtr<T>` |

### Complex Types (Need wrapper)

| C++ Type | Strategy |
|----------|----------|
| `DataChunk` | Wrap with accessor methods |
| `LogicalType` | Enum mapping |
| `Value` | Tagged union |
| `QueryResult` | Iterator wrapper |
| `ClientContext&` | Opaque handle |
| `CatalogTransaction` | Opaque handle |
| `ExecutionContext&` | Opaque handle |

### DuckDB Classes to Expose

```rust
#[cxx::bridge]
mod ffi {
    unsafe extern "C++" {
        // Core types
        type ClientContext;
        type CatalogTransaction;
        type ExecutionContext;

        // Data types
        type DataChunk;
        type LogicalType;
        type Value;
        type Vector;

        // Query execution
        type QueryResult;
        type Connection;

        // Catalog info
        type CreateSchemaInfo;
        type CreateTableInfo;
        type CreateViewInfo;
        type BoundCreateTableInfo;
        type DropInfo;
        type AlterTableInfo;

        // Entries
        type CatalogEntry;
        type SchemaCatalogEntry;
        type TableCatalogEntry;

        // Physical operators
        type OperatorSinkInput;
        type OperatorSourceInput;
        type GlobalSinkState;
        type LocalSinkState;

        // File system
        type FileSystem;
    }
}
```

---

## Risks and Mitigations

### Risk 1: DuckDB API Changes

DuckDB's internal C++ API is not stable. Virtual method signatures can change between versions.

**Mitigation:** Pin to specific DuckDB version. Update shim when upgrading.

### Risk 2: cxx Limitations

cxx doesn't handle all C++ patterns (templates, multiple inheritance, etc.).

**Mitigation:** Use opaque handles for complex types. Write manual wrapper functions where needed.

### Risk 3: Performance Overhead

FFI calls have overhead. DuckLake makes many calls per query.

**Mitigation:** Batch operations where possible. Profile and optimize hot paths. The overhead is likely negligible compared to I/O.

### Risk 4: Debugging Complexity

Mixed Rust/C++ debugging is harder than single-language.

**Mitigation:** Clear error propagation. Extensive logging. Integration tests.

---

## Alternatives Considered

### 1. Pure C++ (Current)

**Pros:** Works today, no FFI overhead
**Cons:** No Rust benefits (safety, tooling, ecosystem)

### 2. Pure Rust (Wait for DuckDB)

**Pros:** Clean architecture
**Cons:** DuckDB would need to expose Catalog/Transaction APIs via C API or Rust bindings. No timeline for this.

### 3. Fork DuckDB

**Pros:** Full control
**Cons:** Massive maintenance burden. Can't track upstream.

### 4. Different Database

**Pros:** Maybe better Rust support
**Cons:** DuckDB's features (columnar, embedded, OLAP) are unique.

---

## Recommendations

### Short Term: Stay with C++

The current C++ implementation works. Porting to Rust is a significant investment with limited immediate benefit.

### Medium Term: Evaluate Hybrid

If the team wants Rust for:
- New feature development
- Better testing (Rust's type system catches more bugs)
- Ecosystem integration (async, serde, etc.)

Then the hybrid architecture is viable. Start with:
1. Port `MetadataManager` to Rust (most isolated, most code)
2. Add cxx bridge for `Transaction.Query()`
3. Gradually port more components

### Long Term: Advocate for Rust Bindings

Push DuckDB to expose internal APIs via Rust bindings. This would enable:
- Pure Rust storage extensions
- Broader Rust ecosystem for DuckDB
- Simpler architecture

---

## References

### DuckDB
- [duckdb-rs](https://github.com/duckdb/duckdb-rs) - Rust bindings (client only)
- [extension-template-rs](https://github.com/duckdb/extension-template-rs) - Rust extension template (limited)
- [DuckDB Extension Development](https://duckdb.org/docs/extensions/overview) - C++ focused

### Rust/C++ Interop
- [cxx](https://cxx.rs/) - Safe C++/Rust interop
- [autocxx](https://google.github.io/autocxx/) - Auto-generated C++ bindings
- [autocxx subclass support](https://github.com/google/autocxx/issues/200) - Pure virtual inheritance (experimental)

### Related Projects
- [Firefox Rust Integration](https://manishearth.github.io/blog/2021/02/22/integrating-rust-and-c-plus-plus-in-firefox/) - Lessons learned from large-scale C++/Rust integration
