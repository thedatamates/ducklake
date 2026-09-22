# Snapshot allocation and concurrency

All catalogs share `ducklake_snapshot_id_seq` and `ducklake_snapshot`. Snapshot zero bootstraps the fresh metabase. Sequence values are unique but can have gaps after failed transactions; numeric adjacency is not a lineage relationship.

## Commit protocol

Crucible and the extension both acquire a transactional guard on the global format row:

```sql
UPDATE ducklake_metadata SET value = value
WHERE catalog_id IS NULL AND key = 'version';
```

The guard protects shared object/file counters as well as snapshot allocation. A unique sequence alone does not prevent two writers from using the same stale counters. Conflicting writes retry through the existing transaction retry mechanisms.

The extension's `DuckLakeTransactionState::Commit` requests allocation through its commit context. The base and PostgreSQL metadata managers implement `GetNextSnapshotId()`, acquire the guard and call `nextval`. The PostgreSQL sequence name is schema-qualified inside the remote SQL; the metadata attachment is supplied separately to `postgres_query`. Keep query preparation enabled so the result contains the allocated ID.

Latest-snapshot queries use `MAX(snapshot_id)` to resolve the visible snapshot. Conflict queries scope analytical changes by catalog. Crucible's `metabase_write` uses the same guard and snapshot/counter contract for catalog creation, schema creation and forks.

## Lineage and visibility

Each commit records `ducklake_snapshot_changes` and a catalog-scoped `ducklake_snapshot_lineage` edge. The predecessor is the previous change in that catalog, falling back to catalog creation. Do not compute it by subtracting one from the global snapshot ID.

Schema changes increment schema version. Table layout history is recorded per table. Crucible schema creation also increments the shared schema version and records lineage, allowing existing extension connections to discover the new schema.

User-visible historical reads cannot precede catalog creation. A fork retains older schema layouts needed to read its current inline data, but does not expose ancestor data-history inheritance. Snapshot expiration is rejected because snapshots are shared among catalogs.

## Validation

The managed-catalog SQL regression passed 45 assertions, including concurrent transactions. Crucible's PostgreSQL regression runs catalog creation alongside extension writes and checks that allocation remains distinct. Separate concurrent PostgreSQL writers also produced distinct table and file IDs. The selected Crucible suite passed 65 tests.

These are targeted checks, not a claim that the full upstream suite or production concurrency behavior has been validated. Provisioning and migration are owned by Crucible; there is no extension-side sequence bootstrap or automatic repair of older metabases.
