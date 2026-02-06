# Snapshot Allocation and Conflict Visibility Notes

Date: February 6, 2026
Status: Implemented and validated in this branch

## 1. Goal and scope

We needed to eliminate snapshot ID races in DuckLake under concurrent writers, preserve ATTACH guardrails, and keep tests/docs aligned.

The concrete objectives were:
1. Stop allocating snapshot IDs with client-side arithmetic (`latest + 1`).
2. Move allocation to a database sequence.
3. Keep conflict detection correct under concurrency after switching to sequence semantics.
4. Keep ATTACH behavior strict (`CATALOG` required, `CREATE_IF_NOT_EXISTS` default false).
5. Make tests gap-tolerant and concurrency-focused.

## 2. Root causes we addressed

1. Duplicate snapshot IDs under concurrency:
   multiple writers computed the same next ID client-side.
2. Conflict visibility regression after moving to sequence IDs:
   sequence uniqueness removed accidental PK-collision retries, which exposed stale-read conflict checks in local metadata mode.
3. Non-obvious identifier qualification differences between local and postgres metadata modes.

## 3. Final design decisions

1. Snapshot IDs come only from `ducklake_snapshot_id_seq`.
2. Bootstrap snapshot remains `snapshot_id = 0`.
3. Sequence is initialized post-bootstrap so first runtime allocation is after bootstrap.
4. Conflict checks in local metadata mode use a fresh metadata connection/snapshot.
5. Snapshot commit lineage is persisted explicitly (`previous_snapshot_id -> snapshot_id`) per catalog.
6. Tests assert uniqueness, not contiguity (gaps are valid sequence behavior).

## 4. Non-obvious implementation detail: sequence qualification

`GetNextSnapshotId()` differs intentionally by backend mode:

1. Local metadata mode:
   uses `nextval('ducklake_snapshot_id_seq')` with no schema/catalog prefix.
   Reason: metadata search path is configured on the active transaction connection.
2. Postgres metadata mode:
   uses schema-qualified sequence inside the remote SQL string, while the catalog/database is supplied out-of-band by `postgres_query/postgres_execute` wrappers.

This split is correct and required; forcing one style everywhere causes subtle breakage.

## 5. Implemented changes (by area)

### Schema contract

1. Added `ducklake_snapshot_id_seq` to canonical postgres schema.
2. Kept bootstrap snapshot insert at `snapshot_id = 0`.
3. Added sequence initialization after bootstrap.
4. Added `ducklake_snapshot_lineage` table.

### Runtime bootstrap parity

1. `CreateDuckLakeSchema` now creates sequence, bootstrap snapshot, and post-bootstrap sequence initialization.
2. Runtime schema creation now also includes `ducklake_snapshot_lineage`.
3. Local startup path ensures lineage table exists for existing local metadata deployments.

### Allocator API

1. Added `GetNextSnapshotId()` API in metadata manager interface.
2. Implemented local backend allocator using `nextval` with clear error context.
3. Implemented postgres backend allocator with wrapper-aware qualification and clear error context.

### Removed client-side snapshot increments

Allocator usage replaced all active write-path increments in:
1. Transaction commit path.
2. Catalog creation path.
3. Fork catalog path.

### Conflict detection correctness

1. Local metadata conflict reads now use fresh connection reads for latest committed visibility.
2. Commit path performs conflict checks before each retry attempt.
3. Added snapshot lineage write before commit snapshot insert.

### Upstream parity integration

Integrated targeted upstream correctness fixes:
1. `fba21a52` (Avoid double-adding inlined data stats): manual integration due cherry-pick conflict, plus corresponding regression test coverage adaptation.
2. `ff8cb1db` (Fix GetCatalogIdForSchema to filter by table_id): table-scoped snapshot lookup for inlined schema versions, because schema-version-only lookup can resolve the wrong table snapshot when multiple tables share the same schema version.
   This prevents incorrect inlined-data snapshot resolution on fresh metadata reads.

## 6. ATTACH guardrails status

Guardrails are preserved:
1. `CATALOG` is required.
2. `CREATE_IF_NOT_EXISTS` default remains false.
3. Tests were updated/added without relaxing these defaults.

## 7. Validation executed

## Build

1. `ENABLE_POSTGRES_SCANNER=1 make release`

## Targeted suites

1. `./build/release/test/unittest "test/sql/transaction/*"`
2. `./build/release/test/unittest "test/sql/fork/*"`
3. `./build/release/test/unittest "test/sql/concurrent/*"`
4. `./build/release/test/unittest "test/sql/stats/*"`
5. `./build/release/test/unittest "test/sql/data_inlining/*"`

## Full suite

1. `make test`
2. Result summary from latest run: all executed tests passed, with expected environment-gated skips.

## Grep gate

Used grep gate to confirm no active write-path `snapshot_id++` / `+ 1` allocation patterns remain in DuckLake metadata/transaction/fork paths.

## 8. Test behavior updates worth noting

1. Concurrency tests now assert no duplicate snapshot IDs.
2. Gap-sensitive assertions were updated to avoid assuming contiguous snapshot IDs.
3. Fork and transaction concurrency tests include uniqueness checks against metadata snapshots.

## 9. Residual risks and open assumptions

1. Sequence IDs are unique but not guaranteed to reflect commit order under all interleavings.
   Consumers must not infer ordering from numeric adjacency.
2. Existing pre-fix metadata stores may require explicit bootstrap/update path to include new sequence/lineage objects.
3. Snapshot lineage is now written; follow-up work may expand read-side usage if stricter ancestry traversal becomes necessary.

## 10. Potential follow-up work

1. Evaluate whether a stricter CAS-centric commit protocol would simplify long-term conflict validation.
2. Evaluate using lineage as canonical ancestry traversal input for conflict checks.
3. Add more stress/failure-injection coverage around missed-conflict scenarios under high contention.
4. Define a concrete non-rebuild migration path for legacy pre-fix metadata stores if needed.

## 11. Latest-snapshot query shape research (`ORDER BY ... DESC LIMIT 1` vs `MAX(...)`)

### Why this section exists

We observed intermittent local metadata failures of:

1. `No snapshot found in DuckLake`

This occurred under concurrency despite snapshot rows existing. We changed latest-snapshot reads from:

1. `ORDER BY snapshot_id DESC LIMIT 1`

to:

1. `WHERE snapshot_id = (SELECT MAX(snapshot_id) ...)`

and paired local metadata reads with a fresh-connection retry before failing.

### What DuckDB source inspection showed

The two query forms are semantically equivalent SQL, but they are not execution-equivalent in DuckDB.

1. `ORDER BY ... LIMIT 1` is optimized via TopN rewrite:
   `src/optimizer/topn_optimizer.cpp` (`LogicalLimit + LogicalOrder` -> `LogicalTopN`).
2. TopN pushdown adds an optional dynamic filter on scan:
   `src/optimizer/topn_optimizer.cpp`, `src/planner/filter/dynamic_filter.cpp`.
3. With default optimizers, TopN can be transformed by late materialization into a rowid-based SEMI JOIN shape:
   `src/optimizer/late_materialization.cpp`.
4. Optional filters are pruning-only and not required for correctness:
   `src/planner/filter/optional_filter.cpp`, `src/storage/table/row_group.cpp`.
5. `MAX(...)` takes a different plan family:
   aggregate subquery (`UNGROUPED_AGGREGATE max`) + equality lookup, often hitting index scan on PK in the outer branch.

### What we validated experimentally

Using the local built DuckDB binary and C++ harnesses:

1. `EXPLAIN`/`EXPLAIN ANALYZE` confirmed distinct physical plans:
   `ORDER BY ... LIMIT 1` used TopN path; `MAX(...)` used aggregate + equality path.
2. A two-connection stale-snapshot test showed both forms respect transaction visibility and return the same snapshot in a fixed transaction.
3. A same-process threaded insert/read stress harness did not reproduce an empty result in standalone DuckDB for either shape.

### What we can conclude

1. The stabilization is not about SQL semantics; it is about avoiding a more complex operator path under concurrency-sensitive metadata workloads.
2. `MAX(...)` is plausibly more robust for this use case because it bypasses TopN + late-materialization rowid/semi-join behavior.
3. The fresh-connection fallback remains important for local metadata visibility boundaries in DuckLake integration code.

### What remains uncertain

We do not yet have a single minimal, deterministic DuckDB-core repro that proves the exact low-level mechanism of the intermittent empty result in DuckLake. The highest-probability fault domain is still the `ORDER BY ... LIMIT 1` TopN/late-materialization path under DuckLake’s attach/retry concurrency boundaries.

### Current guidance

1. Keep `MAX(...)` as the latest-snapshot query shape in DuckLake.
2. Keep fresh-connection retry on local metadata reads before terminal failure.
3. If we pursue upstream filing, include:
   - physical plan diff (`ORDER BY ... LIMIT 1` vs `MAX(...)`)
   - DuckLake-specific concurrency workload
   - empty-result diagnostics (`visible snapshot rows` vs `fresh visible snapshot rows`)

## 12. Canonical documentation map

1. This file is the authoritative snapshot-sequence implementation and validation record for this fork.
