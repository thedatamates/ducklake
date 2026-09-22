# Testing the managed-catalog fork

The extension tests run directly against DuckLake. They provision disposable metadata with SQL fixtures and do not call Crucible or require the Monogram repository. Build the [pinned runtime](BUILD.md) before running them; an official DuckLake extension does not implement this format.

## Commands

Run from this repository's root. `--test-dir .` is required when the runtime was built in another checkout: otherwise the runner can discover that checkout's tests.

```bash
build/release/test/unittest --test-dir . \
  --test-config test/configs/managed.json '~[.]test/sql/*'
```

`make test` passes the managed configuration through the extension test command. `unittest_relassert` uses `T`; explicitly overriding `T` also overrides its default arguments. These defaults exclude upstream `.test_slow` and coverage-only cases; they do not exclude ordinary concurrency tests.

The shared-catalog regressions require the matched `postgres_scanner` extension and a **disposable PostgreSQL database**. They reset only the `isolation`, `lineage`, `fork_storage` and `fork_inline` schemas in that database. Never point this setting at an application database.

```bash
createdb ducklake_suite
export DUCKLAKE_TEST_POSTGRES='dbname=ducklake_suite'
build/release/test/unittest --test-dir . \
  --test-config test/configs/managed.json 'test/sql/multi_catalog/*'
```

Without this environment variable, the four PostgreSQL cases report a prerequisite skip. The native `managed_catalogs`, `catalog_discovery` and `retention_guards` cases still run. The `Catalogs` CI job builds PostgreSQL support, creates its own database and sets the variable explicitly.

Other existing configurations retain their own prerequisites and exclusions and inherit the managed lifecycle exclusions:

```bash
build/release/test/unittest --test-dir . --test-config test/configs/no_inline.json '~[.]test/sql/*'
build/release/test/unittest --test-dir . --test-config test/configs/deletion_vectors.json '~[.]test/sql/*'
```

Use exact test paths or a trailing `*` when selecting cases. This Catch runner does not match a wildcard in the middle of a path. A command reporting “No tests ran” is not validation.

## Fork-specific coverage

| Behavior | Direct regression and checks |
|---|---|
| Externally provisioned lifecycle | `initialize/ducklake_create_new.test`: missing ID, name-only attach, both creation-flag values, unknown/retired IDs, mismatched name, missing/old format marker, reconnect, unchanged rejected metadata |
| Read-only access | `initialize/read_only_mode.test`: no implicit provisioning, successful read of provisioned data, write rejection |
| Catalog isolation | `multi_catalog/catalog_isolation.test`: two simultaneous attachments to one PostgreSQL schema; same-name tables, views, macros and schemas; comments, settings, statistics, updates, deletes, flush, drop and reconnect |
| Nonzero catalog and overlapping schema IDs | `multi_catalog/managed_catalogs.test`: explicitly provisioned IDs, inline and Parquet writes, DDL, comments, views, macros and historical reads |
| Discovery and stable identity | `multi_catalog/catalog_discovery.test`: IDs, UUIDs, names, parent/birth information, retired catalog filtering and independent attachment aliases |
| Global snapshot and object/file allocation | `multi_catalog/snapshot_lineage.test`: overlapping transactions on different catalogs, simultaneous writers, unique snapshots, table IDs and Parquet file IDs |
| Per-catalog predecessors | `multi_catalog/snapshot_lineage.test`: exact interleaved predecessor edges, rollback, no cross-catalog edges, scoped snapshot listings and reconnect |
| Concurrent inserts in one catalog | Restored `transaction/concurrent_snapshot_ids.test` and `concurrent/concurrent_insert_conflict.test`: final rows/sums and duplicate snapshot detection |
| Snapshot sequence gaps | `snapshot_info/ducklake_last_commit.test`, `ducklake_current_commit.test` and `audit/test_base_audit.test`: rollback/retry gaps do not lose latest-commit identity, authors, messages or extra information |
| Quoted metadata identifiers | `catalog/quoted_identifiers.test`: apostrophes, quotes and spaces in metadata paths, metadata aliases/schemas and SQL identifiers, including a committing write |
| Forked immutable storage | `multi_catalog/fork_storage.test`: shared file/table IDs and paths, inherited deletion files, views/macros, partition/sort metadata, independent writes/schema changes, reconnect and fork history |
| Forked inline storage | `multi_catalog/fork_inline_layouts.test`: independently copied physical inline data/deletion tables, pre-rename/pre-add column layouts, independent mutations, flush and reconnect |
| Catalog birth bounds | Both fork tests reject pre-birth reads; inline test reads the fork's birth version after later writes and flush |
| Retention and cleanup | `multi_catalog/fork_storage.test`: another catalog's current and historical references protect Parquet and deletion files; a re-imported path remains protected under a different file ID; orphan removal preserves referenced files; metadata errors fail closed; physical files disappear only after all references are retired |
| Expiration rejection | `multi_catalog/retention_guards.test`: failed expiration leaves snapshot/data/delete metadata unchanged; compaction, checkpoint, drop and cleanup preserve historical reads |
| Views with inlining validation | `settings/inlining_with_views.test`: global and schema inlining changes with committed and transaction-local views do not cast views to tables |

Paths in the table are relative to `test/sql/`. These tests establish the extension's behavior against a valid provisioned fork; they do not claim to test Crucible's implementation of the provisioning transaction.

## Upstream analytical coverage

The existing SQL tests now provision metadata before their first successful attachment and supply `CATALOG_ID`. Their analytical assertions remain in place. Metadata assertions include `catalog_id`; inline physical names include the catalog ID. Snapshot assertions allow sequence gaps where rollback or retries can consume IDs.

The common native fixture starts with catalog 0, schema 0, snapshot 0 and the upstream initial object/file counters. It deliberately avoids introducing a catalog-creation snapshot that would shift every historical assertion. Each test has an isolated directory; its metadata filename remains stable across the provisioning include and reconnects. `{UUID}` cannot serve this purpose because the current runner expands it anew on each substitution.

`delete/delete_ignore_extra_columns.test` still reads the original checked-in Parquet/deletion files. Its legacy metadata is copied into a fresh managed fixture; the test no longer depends on automatic migration to reach the deletion regression.

The fixtures mirror the `1.1-dev1-catalog1` relation definitions owned by Crucible. When that schema changes, update the native and PostgreSQL fixtures, the embedded `managed_catalogs` baseline and the quoted-identifier fixture together. Runtime tests remain independent of Crucible.

## Explicit exclusions

`test/configs/managed.json` lists every excluded path with its reason. It excludes **23 old-format/automatic-migration cases and 19 destructive snapshot-expiration cases**. These are upstream contracts this fork intentionally does not implement. They remain in the repository. `inlining_reserved_column_names.test` keeps its current-format assertions runnable; only its v1.0 section lives in the separately excluded migration test.

These exclusions are not passing tests. In particular, the shared-history model cannot safely run upstream expiration tests that assume exclusive ownership of global snapshots. Rejection, historical readability and shared-file cleanup have separate direct tests above. Dry-run expiration and other supported compaction/cleanup cases remain enabled.

The runner also reports conditional skips for missing optional extensions, external services or CI-only prerequisites. A native run without PostgreSQL is insufficient to validate the multi-catalog contract. SQLite and Quack are not supported metadata backends in this integration; their upstream configurations are retained as reference, while the supported CI catalog job runs native DuckDB and PostgreSQL.

## Audit of the old fork's tests

The audit compared old fork `4ccee75fe091291ad722c41363c0cdc145ae9bad` against base `8c69c9d24037f8a5acc72b5218c9e66b0e462e87` and the integrated upstream test tree.

- Restored the dropped `catalog_isolation.test` and `concurrent_snapshot_ids.test`. Isolation now uses a genuinely shared PostgreSQL metabase rather than two independent native files.
- Restored the extra duplicate-snapshot assertion in `concurrent_insert_conflict.test`.
- Preserved `data_inlining/table_stats.test`, including the newer upstream post-flush check.
- Reinstated the purpose of the old gap-aware snapshot and audit assertions and catalog-aware macro metadata checks.
- Reviewed the remaining old changes: attachment setup, snapshot/object-number offsets, catalog data-path prefixes, inline physical names and metadata column counts. The new fixture preserves upstream starting IDs, so the old catalog-creation offsets are unnecessary. The underlying row, time-travel, table-change, partition and compaction assertions remain.

## Bugs exposed during this repair

1. Overwritten delete-file cleanup inserted four values into the five-column catalog-scoped queue. The insert now supplies `catalog_id` and an explicit column list.
2. Inlining validation cast a view entry to a table, causing a native crash. It now checks the entry type.
3. Snapshot history could not parse externally written `forked_from` changes. The parser and snapshot representation now retain that provenance.
4. Snapshot allocation embedded a qualified sequence identifier in an unescaped SQL string. It now quotes the SQL literal after resolving the identifier.
5. Cleanup could delete a still-referenced physical path after re-import assigned it another file ID. It now checks resolved paths across catalogs in addition to IDs.

The metadata backend type check also accepts the uppercase `DUCKDB` spelling supplied through metadata parameters.

## Local validation, 2026-09-22

Validated on macOS arm64 with the pinned release build, PostgreSQL 17 and the matched PostgreSQL scanner/libpq 18.6. The final native pass ran the same 599 ordinary SQL files in two disjoint batches: 42 explicit lifecycle exclusions, 28 prerequisite skips and 529 executed cases. Both batches passed after the final cleanup change.

| Run | Executed cases | Passed assertions | Conditional skips |
|---|---:|---:|---:|
| Full managed native suite | 529 | 20,049 | 28 |
| Native and shared PostgreSQL feature suite | 7 | 368 | 0 |
| Inlining disabled: update, delete, compaction, transactions, concurrency, orphan cleanup and retention | 93 | 3,405 | 4 |
| Puffin: deletion, inline deletion, rewrite and retention | 56 | 2,117 | 2 |
| Final Puffin check: PostgreSQL feature suite and orphan cleanup | 12 | 546 | 1 |

The final cleanup implementation was rechecked by the full native pass, the PostgreSQL feature suite and the final Puffin check. The wider inlining-disabled and Puffin operator runs preceded that last path-normalization change.

`make format-fix`, `git diff --check`, YAML/JSON parsing, exclusion-path validation, fixture relation-definition parity and the `make test` command expansion also passed. No tracked test file was deleted.

Local runs did not exercise HTTP/S3, spatial-extension, SQLite, CI-only or slow/coverage-only cases. The full alternative-configuration matrices, Linux/Windows builds, sanitizer jobs and GitHub workflows were not executed locally. CI commands have been updated; this local report does not claim those remote jobs passed.
