# Building DuckLake

Build the extension, DuckDB runtime and PostgreSQL scanner together. An official upstream extension or separately installed DuckDB is not a substitute for this build.

## Pinned dependencies

| Dependency | Commit |
|---|---|
| DuckDB | `ef853aebf803cc4f7738ffc34859227f5ebb6437` |
| extension-ci-tools | `795096d04b009c0d087468439ebb526a5460dfac` |
| DuckLake upstream baseline | `7963da4265c0ed09681821a0f3a158b17573ded4` |

The tested engine reports `v2.0.0-dev84705`. The PostgreSQL scanner pinned by the extension configuration requires libpq 18. Local macOS validation used Homebrew `croaring` 5.2.2 and libpq 18.6.

```bash
git submodule update --init --recursive
ENABLE_POSTGRES_SCANNER=1 CMAKE_BUILD_PARALLEL_LEVEL=8 GEN=ninja make release
```

If CMake selects an older PostgreSQL installation on macOS, configure its paths explicitly after generating the build directory:

```bash
ENABLE_POSTGRES_SCANNER=1 cmake -S duckdb -B build/release \
  -DPostgreSQL_LIBRARY=/opt/homebrew/opt/libpq/lib/libpq.dylib \
  -DPostgreSQL_INCLUDE_DIR=/opt/homebrew/opt/libpq/include
ENABLE_POSTGRES_SCANNER=1 CMAKE_BUILD_PARALLEL_LEVEL=8 cmake --build build/release
```

## Runtime outputs

| Output | Purpose |
|---|---|
| `build/release/duckdb` | Matched SQL shell |
| `build/release/src/libduckdb` with platform suffix | Runtime library linked by Crucible |
| `build/release/extension/` | Matched DuckLake, PostgreSQL and other built extensions |
| `duckdb/src/include/` | Headers for external Rust linking |

Crucible disables the bundled engine feature of `agent-data-duck`. Set `DUCKDB_LIB_DIR` to this build's `src` directory and `DUCKDB_INCLUDE_DIR` to the pinned headers. Its README documents runtime loading and isolated integration-test settings. Other Monogram packages may enable the bundled engine; build Crucible separately from those packages so Cargo feature unification does not re-enable it.

## Verification

```bash
./build/release/test/unittest test/sql/multi_catalog/managed_catalogs.test
make format-fix
```

The formatter needs Black, clang-format 11 and cmake-format. An isolated invocation used during validation was:

```bash
uv run --with 'black>=24' --with 'clang-format==11.0.1' --with cmake-format make format-fix
```

The managed-catalog test passed 45 assertions. The matched build passed 65 Crucible integration tests, including PostgreSQL forks and mixed writers. The full upstream test suite is not adapted to Crucible-managed provisioning. Production packaging and the legacy Crucible Dockerfile have not been validated for this runtime.
