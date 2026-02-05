# Snapshot Concurrency Fix Work Plan (DuckLake + Crucible)

> **Goal:** Remove snapshot ID races across DuckLake and Crucible, validated under Reagent-style concurrent workload.
>
> **Deployment model:** Pre-production reset/rebuild. No compatibility guarantees for old metabases.

This is the execution plan to deliver the design in `/Users/kevingay/srv/tdm/ducklake/SNAPSHOT_SEQUENCE_FIX.md`.

## 1. Scope and Success Criteria

## 1.1 In scope

- Sequence-based snapshot ID allocation in every write path (DuckLake + Crucible).
- Baseline schema updates in both repos.
- Concurrency tests in both repos.
- Cross-service validation with concurrent Reagent runs.

## 1.2 Out of scope

- In-place migrations for existing metabases.
- Mixed old/new binary compatibility.

## 1.3 Done definition

- No remaining client-side `snapshot_id` allocation in write paths.
- No `ducklake_snapshot_pkey` failures in concurrency tests.
- DuckLake and Crucible build/tests pass.
- Reagent fan-out scenario passes consistently.

## 2. Workstream Breakdown

## 2.1 Workstream A: DuckLake schema and allocator implementation

**Owner:** DuckLake engineer(s)

### Tasks

1. Update canonical schema SQL:

- File: `/Users/kevingay/srv/tdm/ducklake/schema/postgresql.sql`
- Add `ducklake_snapshot_id_seq` creation.
- Keep bootstrap snapshot row `(snapshot_id=0, ...)`.
- Add sequence initialization (`setval(..., 1, false)`) after bootstrap.

2. Update runtime schema bootstrap:

- File: `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp`
- In `CreateDuckLakeSchema`, create/init sequence in sync with canonical SQL.

3. Add allocator API:

- Files:
  - `/Users/kevingay/srv/tdm/ducklake/src/include/storage/ducklake_metadata_manager.hpp`
  - `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp`
- Add `GetNextSnapshotId()` that executes `nextval()`.

4. Replace client-side snapshot allocation in all DuckLake write paths:

- `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_transaction.cpp`
- `/Users/kevingay/srv/tdm/ducklake/src/storage/ducklake_metadata_manager.cpp` (`CreateCatalog`)
- `/Users/kevingay/srv/tdm/ducklake/src/functions/ducklake_fork_catalog.cpp`

### Exit criteria

- `rg` confirms no `snapshot_id++` / `snapshot_id + 1` in these active paths.
- DuckLake builds successfully.

## 2.2 Workstream B: Crucible schema baseline and interactor implementation

**Owner:** Crucible engineer(s)

### Tasks

1. Update baseline metabase migration (reset-based strategy):

- File: `/Users/kevingay/srv/tdm/software/services/crucible/src/migration/metabase/m20250107_000001_create_ducklake_tables.rs`
- Add sequence creation and initialization in the baseline migration.

2. Switch `create_catalog` to sequence allocation:

- File: `/Users/kevingay/srv/tdm/software/services/crucible/src/interactors/create_catalog.rs`
- Replace `latest_snapshot.snapshot_id + 1` with `nextval('ducklake_snapshot_id_seq')`.

3. Switch `create_schema` to sequence allocation:

- File: `/Users/kevingay/srv/tdm/software/services/crucible/src/interactors/create_schema.rs`
- Replace `latest_snapshot.snapshot_id + 1` with `nextval('ducklake_snapshot_id_seq')`.

4. Add bounded retry for metabase conflict classes in both interactors:

- Retry only transient/conflict DB errors.
- Add jittered backoff.
- Preserve clear failure context after retry exhaustion.

### Exit criteria

- `cargo check` passes.
- New/updated integration tests pass.

## 2.3 Workstream C: Test coverage

**Owners:** DuckLake + Crucible + Integration

### DuckLake tests

Add/extend tests under:

- `/Users/kevingay/srv/tdm/ducklake/test/sql/transaction/`
- `/Users/kevingay/srv/tdm/ducklake/test/sql/fork/`
- `/Users/kevingay/srv/tdm/ducklake/test/sql/concurrent/`

Required assertions:

- Concurrent commits never collide on snapshot PK.
- Concurrent catalog/fork operations use unique snapshot IDs.
- Gaps in snapshot sequence are allowed.

### Crucible tests

Create:

- `/Users/kevingay/srv/tdm/software/services/crucible/tests/create_catalog.rs`
- `/Users/kevingay/srv/tdm/software/services/crucible/tests/create_schema.rs`

Update:

- `/Users/kevingay/srv/tdm/software/services/crucible/tests/lib.rs`

Required assertions:

- Parallel `create_catalog` requests do not fail from duplicate snapshot ID.
- Parallel `create_schema` requests do not fail from duplicate snapshot ID.
- Retry logic succeeds under transient conflicts.

### Cross-service concurrency test

Run scenario:

- Start Crucible.
- Trigger multiple concurrent Reagent worktasks on one workbook catalog.
- Verify no snapshot PK violations and stable completion.

## 3. Execution Order

## 3.1 Parallelization model

- Workstream A and B can proceed in parallel.
- Workstream C starts once A/B have testable branches.

## 3.2 Recommended sequence

1. Land schema contract updates (DuckLake SQL + Crucible baseline migration).
2. Land DuckLake allocator changes.
3. Land Crucible allocator + retry changes.
4. Land test additions in both repos.
5. Run integrated concurrency validation.

## 4. Reset/Rebuild Procedure (No Migration Path)

Use this in each dev/staging environment:

1. Stop services that connect to metabase.
2. Drop metabase databases/schemas.
3. Recreate fresh metabase.
4. Apply Crucible metabase migrations from baseline.
5. Rebuild/deploy new DuckLake and Crucible binaries.
6. Recreate catalogs/schemas/workbooks as needed.
7. Run full concurrency validation suite.

## 5. Validation Commands and Gates

## 5.1 Static greps

DuckLake repo:

- Ensure no stale snapshot increment patterns in active allocator paths.

Crucible repo:

- Ensure `create_catalog` and `create_schema` no longer compute `snapshot_id + 1`.

## 5.2 Build/test gates

DuckLake:

- Build and targeted SQL test suites.

Crucible:

- `cargo check`
- `cargo test` (including new concurrency tests)

Integration:

- Reagent fan-out scenario repeated multiple times without snapshot PK failures.

## 6. Risk Register

1. **Missed allocator path**
- Mitigation: grep-based gate plus code review checklist.

2. **Sequence bootstrap mismatch between schema sources**
- Mitigation: enforce lockstep updates for canonical SQL, runtime bootstrap, and Crucible baseline migration.

3. **Conflict retries too broad or too narrow**
- Mitigation: match only transient/conflict signatures; cap retries and log attempt metadata.

4. **Cross-repo drift**
- Mitigation: single owner signs off on both repositories before integration test.

## 7. Reviewer Checklist

1. All snapshot allocation paths use `nextval()`.
2. No new snapshot path relies on client-side increment.
3. Sequence exists/initialized in all schema creation paths.
4. Retry logic has bounded attempts and targeted error classification.
5. Tests include true parallel execution, not serialized mocks.
6. Reagent-driven integration run is part of final sign-off.

## 8. Deliverables

1. Updated design doc: `/Users/kevingay/srv/tdm/ducklake/SNAPSHOT_SEQUENCE_FIX.md`
2. Updated execution plan: `/Users/kevingay/srv/tdm/ducklake/SNAPSHOT_SEQUENCE_WORKPLAN.md`
3. DuckLake code + tests PR
4. Crucible code + tests PR
5. Integration test evidence (logs/report) showing no snapshot PK races under concurrency
