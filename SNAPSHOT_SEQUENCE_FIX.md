# Snapshot Concurrency Elimination (DuckLake + Crucible)

> **Status:** Finalized design
>
> **Compatibility stance:** Pre-production reset is acceptable. We optimize for correctness and simplicity, not in-place backward compatibility.

This document defines the technical design to eliminate snapshot ID race conditions across DuckLake (C++) and Crucible (Rust), with Reagent workloads as the primary concurrency driver.

## 1. Why This Work Is Required

Reagent drives high-concurrency behavior by design:

- One workbook/automation can spawn multiple concurrent worktasks.
- Each worktask creates its own Crucible session.
- Sessions run writes against the same DuckLake metabase/catalog.

Today, multiple code paths still derive `snapshot_id` client-side (`latest + 1`). Under concurrency, this produces duplicate key failures on `ducklake_snapshot(snapshot_id)` and forces retries/failures.

### Business/operational impact

- Non-deterministic failures under normal multi-worker load.
- Higher latency due to retries and failed attempts.
- Lower confidence in multi-tenant isolation and correctness.
- Fragility in automated workflows where concurrent tasks are expected.

## 2. What Is Already Fixed (And Why It Is Not Enough)

The attach guardrail is in place:

- `CATALOG` is required in DuckLake attach parsing.
- `CREATE_IF_NOT_EXISTS` defaults to `false`.
- Crucible attach query explicitly passes `CATALOG` and `CREATE_IF_NOT_EXISTS false`.

This removed attach-time empty-catalog collisions, but it does **not** fix snapshot allocation races during commit/catalog/schema creation.

## 3. Remaining Race Surfaces

## 3.1 Snapshot ID race surfaces (must fix)

Client-side `snapshot_id` allocation still exists in:

- DuckLake commit path: `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_transaction.cpp`
- DuckLake catalog creation helper: `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp`
- DuckLake fork catalog function: `/Users/kevingay/srv/tdm/ducklake/src/functions/ducklake_fork_catalog.cpp`
- Crucible metabase writes:
  - `/Users/kevingay/srv/tdm/software/services/crucible/src/interactors/create_catalog.rs`
  - `/Users/kevingay/srv/tdm/software/services/crucible/src/interactors/create_schema.rs`

## 3.2 Adjacent allocator contention (must handle in Crucible)

`create_catalog` and `create_schema` also allocate from `latest_snapshot.next_catalog_id`. These paths currently have no retry loop, so any conflict becomes a request failure.

## 4. Final Technical Decision

1. Use a PostgreSQL sequence (`ducklake_snapshot_id_seq`) for **all** new snapshot IDs.
2. Call `nextval()` explicitly in code before snapshot INSERT.
3. Keep explicit `snapshot_id` in INSERT statements (no column `DEFAULT nextval`).
4. Since this is pre-production, we do a clean reset/rebuild of metabases instead of carrying complex migration logic.

### Consequences

- Snapshot IDs are globally unique and atomic under concurrency.
- Gaps in snapshot IDs are expected and acceptable (sequence semantics).
- We avoid mixed-mode complexity and reduce rollout risk.

## 5. Schema Contract (Target State)

## 5.1 Canonical DuckLake SQL

File: `/Users/kevingay/srv/tdm/ducklake/schema/postgresql.sql`

Required changes:

- Create `ducklake_snapshot_id_seq`.
- Keep `ducklake_snapshot.snapshot_id BIGINT PRIMARY KEY` as-is (no DEFAULT).
- Keep bootstrap snapshot row `(snapshot_id=0, ...)`.
- Initialize sequence after bootstrap with `setval(..., 1, false)`.

## 5.2 DuckLake runtime schema bootstrap

File: `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp`

`CreateDuckLakeSchema` must match canonical SQL:

- Create sequence before `ducklake_snapshot` table usage.
- Insert bootstrap snapshot 0.
- Set sequence to first post-bootstrap value.

## 5.3 Crucible metabase baseline schema

File: `/Users/kevingay/srv/tdm/software/services/crucible/src/migration/metabase/m20250107_000001_create_ducklake_tables.rs`

Because we accept reset/rebuild, update the baseline migration directly to include:

- Sequence creation.
- Sequence initialization after bootstrap snapshot insert.

No new incremental migration is required for this effort.

## 6. Code Changes Required

## 6.1 DuckLake C++

### A. Add snapshot sequence allocator API

Files:

- `/Users/kevingay/srv/tdm/ducklake/src/include/storage/ducklake_metadata_manager.hpp`
- `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp`

Add method:

- `idx_t GetNextSnapshotId();`

Behavior:

- Executes `SELECT nextval('{METADATA_CATALOG}.ducklake_snapshot_id_seq')`.
- Throws with contextual error on failure.
- Returns scalar snapshot ID.

### B. Commit path uses sequence allocator

File: `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_transaction.cpp`

- Remove client-side `commit_snapshot.snapshot_id++`.
- Replace with `commit_snapshot.snapshot_id = metadata_manager->GetNextSnapshotId();`.

### C. Catalog-creation helper uses sequence allocator

File: `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp`

In `CreateCatalog(...)`:

- Replace `current_snapshot_id + 1` logic with `GetNextSnapshotId()`.
- Preserve existing metadata writes for catalog/schema/bootstrap records.

### D. Fork-catalog path uses sequence allocator

File: `/Users/kevingay/srv/tdm/ducklake/src/functions/ducklake_fork_catalog.cpp`

- Replace `current_snapshot_id + 1` with allocator-based ID.
- Keep fork semantics unchanged.

## 6.2 Crucible Rust

### A. Use sequence for snapshot allocation

Files:

- `/Users/kevingay/srv/tdm/software/services/crucible/src/interactors/create_catalog.rs`
- `/Users/kevingay/srv/tdm/software/services/crucible/src/interactors/create_schema.rs`

Replace:

- `let new_snapshot_id = latest_snapshot.snapshot_id + 1;`

With:

- `SELECT nextval('ducklake_snapshot_id_seq')` via SeaORM statement.
- Parse scalar `i64` and use in `snapshot::ActiveModel`.

### B. Add retry-on-conflict for metabase allocator contention

Same files as above.

Add bounded retry loop around the metabase transaction for conflict classes expected under concurrency (e.g., unique violations caused by concurrent `next_catalog_id` consumers).

Requirements:

- Small bounded retries (e.g., 5-10) with jittered backoff.
- Retry only on known transient/conflict errors.
- Preserve clear error context when retries exhaust.

This keeps `create_catalog` / `create_schema` reliable in multi-worker scenarios.

## 7. Reagent/Crucible Interaction Implications

No Reagent API changes are required for the snapshot fix.

The fix targets the actual concurrency pattern already in production workflows:

- Workbook/automation/worktask jobs create many Crucible sessions.
- Sessions write to shared catalogs.
- Snapshot allocation must be globally atomic under this pressure.

Relevant Reagent entry points:

- `/Users/kevingay/srv/tdm/software/webapp/app/jobs/workbook_run_job.rb`
- `/Users/kevingay/srv/tdm/software/webapp/app/jobs/worktask_run_job.rb`
- `/Users/kevingay/srv/tdm/software/webapp/app/jobs/automation_run_job.rb`

## 8. Test Strategy (Required)

## 8.1 DuckLake tests

Add/extend SQL tests under:

- `/Users/kevingay/srv/tdm/ducklake/test/sql/transaction/`
- `/Users/kevingay/srv/tdm/ducklake/test/sql/fork/`
- `/Users/kevingay/srv/tdm/ducklake/test/sql/concurrent/`

Must validate:

- Concurrent commits do not fail with snapshot PK violations.
- Catalog creation/fork under contention does not duplicate snapshot IDs.
- Snapshot IDs are unique; gaps allowed.

## 8.2 Crucible tests

Add integration tests:

- `/Users/kevingay/srv/tdm/software/services/crucible/tests/create_catalog.rs`
- `/Users/kevingay/srv/tdm/software/services/crucible/tests/create_schema.rs`
- Register in `/Users/kevingay/srv/tdm/software/services/crucible/tests/lib.rs`

Must validate:

- Parallel `create_catalog` requests produce unique snapshots without snapshot PK violations.
- Parallel `create_schema` requests behave similarly.
- Retry behavior resolves transient conflicts.

## 8.3 Cross-service validation

Run an integration scenario that mirrors Reagent fan-out:

- Multiple concurrent worktasks against one workbook catalog.
- Observe no `ducklake_snapshot_pkey` errors and stable completion.

## 9. Acceptance Criteria

Implementation is complete only when all are true:

1. No client-side snapshot allocation remains in active write paths (`snapshot_id++` / `snapshot_id + 1`).
2. Sequence exists and is initialized in:
   - canonical DuckLake schema SQL
   - DuckLake runtime schema bootstrap
   - Crucible baseline metabase migration
3. DuckLake and Crucible tests pass, including new concurrency tests.
4. Reagent-style concurrency run shows no duplicate snapshot-key failures.

## 10. Explicit Non-Goals

- In-place upgrade/migration support for old pre-fix metabases.
- Supporting mixed old/new binaries against the same metabase.

If older databases must be supported later, that should be a separate scoped project.
