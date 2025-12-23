# Building DuckLake

This is a fork of [duckdb/ducklake](https://github.com/duckdb/ducklake) with additional features:
- Multi-tenant catalog isolation
- Catalog forking
- Global snapshots

## Released vs Development Version

**Released DuckLake** (works with DuckDB v1.3.0+):
```sql
INSTALL ducklake;
LOAD ducklake;
```

This installs the official released version from the DuckDB extension repository.

**This fork** (requires building from source):
```bash
make release
./build/release/duckdb -unsigned
```

The fork includes features not yet in the released version and tracks DuckDB's development branch.

## Why Build From Source?

Both the upstream `duckdb/ducklake` main branch and this fork track DuckDB's v1.5.0 development branch. The code uses APIs (like `QueryResultRow::GetRowInChunk()`) that don't exist in released DuckDB versions.

```bash
git -C duckdb describe --tags
# v1.4.2-3132-g5d5d04f418  (v1.5.0-dev)
```

Once DuckDB v1.5.0 is released, upstream will likely pin to it and release a new extension version. Until then, building from source requires using the bundled DuckDB binary.

## Build Targets

```bash
make release      # optimized build
make debug        # debug build with symbols
make clean        # remove build artifacts
make test_release # run tests against release build
make test_debug   # run tests against debug build
```

## Output Files

After building:

```
build/release/
├── duckdb                                    # DuckDB CLI binary
├── extension/
│   ├── ducklake/
│   │   └── ducklake.duckdb_extension        # DuckLake extension
│   ├── postgres_scanner/
│   │   └── postgres_scanner.duckdb_extension # PostgreSQL extension (if enabled)
│   ├── parquet/
│   ├── json/
│   └── ...
└── repository/                               # Extension repository format
```

## Building with PostgreSQL Support

PostgreSQL metadata support requires the `postgres_scanner` extension:

```bash
ENABLE_POSTGRES_SCANNER=1 make release
```

This fetches and builds `postgres_scanner` from the DuckDB postgres extension repository.

## Using the Extension

### With Built DuckDB Binary

```bash
./build/release/duckdb -unsigned
```

The `-unsigned` flag allows loading locally-built extensions.

```sql
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';

-- Create a DuckLake catalog with local metadata
ATTACH 'ducklake:/tmp/my_lakehouse.db' AS lake (DATA_PATH '/tmp/lakehouse_data');
USE lake;

CREATE TABLE events (id INT, ts TIMESTAMP);
INSERT INTO events VALUES (1, NOW());
SELECT * FROM events;
```

### With PostgreSQL Metadata

```sql
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';
LOAD 'build/release/extension/postgres_scanner/postgres_scanner.duckdb_extension';

ATTACH 'ducklake:postgres:dbname=mydb user=myuser' AS lake (
  DATA_PATH '/data/lakehouse',
  METADATA_SCHEMA 'ducklake'
);
```

See [POSTGRESQL.md](POSTGRESQL.md) for PostgreSQL setup instructions.

## Why Not System DuckDB?

DuckLake uses internal DuckDB APIs that change between versions. The extension is compiled against a specific DuckDB version and will only load in that exact version.

```bash
# This won't work - version mismatch
duckdb -unsigned -c "LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension'"
# Error: The file was built specifically for DuckDB version '5d5d04f418'
```

When DuckLake is released to the extension repository, it will be built against released DuckDB versions and `INSTALL ducklake` will work normally.

## Updating DuckDB Version

The DuckDB version is controlled by the `duckdb` submodule:

```bash
# Check current version
git -C duckdb describe --tags

# Update to a specific version
git -C duckdb fetch --tags
git -C duckdb checkout v1.4.3
make clean release
```

Note: Changing DuckDB versions may require code changes if APIs differ.

## Building Against Released DuckDB

To build against a released DuckDB version (e.g., for distribution):

```bash
git -C duckdb checkout v1.4.3
make clean release
```

If there are API incompatibilities, you'll see errors like:

```
error: no member named 'GetChunk' in 'duckdb::QueryResult::QueryResultRow'
```

These require code changes to use the API available in that DuckDB version.

## Submodules

```bash
# Initialize submodules (first time)
git submodule update --init --recursive

# Reset submodule to committed version
git submodule update --init duckdb
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ENABLE_POSTGRES_SCANNER` | Set to `1` to build postgres_scanner extension |
| `ENABLE_SQLITE_SCANNER` | Set to `1` to build sqlite_scanner extension |
| `GEN` | Set to `ninja` to use Ninja instead of Make |

## Troubleshooting

### "Extension not found"

Make sure you're using the DuckDB built with the project:

```bash
./build/release/duckdb -unsigned
```

Not the system `duckdb` command.

### "The file was built specifically for DuckDB version X"

Version mismatch. Use the bundled DuckDB binary or rebuild against your DuckDB version.

### postgres_scanner build fails

The postgres_scanner patches may not exist for all DuckDB versions. Try building without it:

```bash
make clean release  # without ENABLE_POSTGRES_SCANNER
```

### Submodule at wrong commit

Reset to the committed version:

```bash
git submodule update --init duckdb
```
