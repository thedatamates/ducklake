# DuckLake METADATA_SCHEMA Architecture

## Overview

DuckLake supports two metadata backends with different schema handling behavior:

| Backend | Schema Auto-Create | Pre-Creation Required |
|---------|-------------------|----------------------|
| **DuckDB** | Yes | No |
| **PostgreSQL** | No | Yes |

---

## DuckDB Metadata Backend

### Two-Database Architecture

When you attach a DuckLake catalog:
```sql
ATTACH 'ducklake:mydata.db' AS sales (DATA_PATH '/data/files', CATALOG 'prod')
```

DuckLake creates **two** catalog attachments:

1. **User-facing catalog**: `sales` - What you interact with
2. **Internal metadata catalog**: `__ducklake_metadata_sales` - Hidden, stores metadata tables

### METADATA_SCHEMA Option

By default, metadata tables go in the `main` schema:
```
__ducklake_metadata_sales.main.ducklake_snapshot
__ducklake_metadata_sales.main.ducklake_table
```

Use `METADATA_SCHEMA` to specify a different schema:
```sql
ATTACH 'ducklake:mydata.db' AS sales (..., METADATA_SCHEMA 'sales_meta')
```

Now metadata lives in:
```
__ducklake_metadata_sales.sales_meta.ducklake_snapshot
__ducklake_metadata_sales.sales_meta.ducklake_table
```

### Multi-Tenant in Single File

Multiple DuckLake catalogs can share the same metadata file with different schemas:

```sql
ATTACH 'ducklake:shared.db' AS catalog_a (..., METADATA_SCHEMA 'tenant_a', CATALOG 'a')
ATTACH 'ducklake:shared.db' AS catalog_b (..., METADATA_SCHEMA 'tenant_b', CATALOG 'b')
```

The schema is auto-created via `CREATE SCHEMA IF NOT EXISTS`.

---

## PostgreSQL Metadata Backend

### Direct Query Execution

PostgreSQL uses `postgres_query()` and `postgres_execute()` - no internal catalog attachment:

```sql
ATTACH 'ducklake:postgres:host=localhost dbname=mydb' AS sales (
    DATA_PATH '/data/files',
    METADATA_SCHEMA 'my_schema',
    CATALOG 'prod'
)
```

### Schema Must Be Pre-Created

PostgreSQL requires manual schema creation before attaching:

```sql
-- In psql
CREATE SCHEMA my_schema;
```

Then run the DuckLake schema setup:
```bash
psql -d your_database -f schema/postgresql.sql
```

---

## Query Placeholder Expansion

DuckLake queries use placeholders that expand differently per backend:

| Placeholder | DuckDB | PostgreSQL |
|-------------|--------|------------|
| `{METADATA_CATALOG}` | `"__ducklake_metadata_sales"."schema"` | `"schema"` |
| `{METADATA_CATALOG_NAME_IDENTIFIER}` | `"__ducklake_metadata_sales"` | N/A |
| `{METADATA_SCHEMA_NAME_IDENTIFIER}` | `"schema"` | `"schema"` |

---

## Summary

| Aspect | DuckDB | PostgreSQL |
|--------|--------|------------|
| Internal attachment | `__ducklake_metadata_<name>` | None |
| Query execution | Via internal DuckDB catalog | Via `postgres_query()` |
| Schema auto-create | Yes | No |
| `{METADATA_CATALOG}` | `catalog.schema` | `schema` only |
