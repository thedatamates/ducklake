# Metabase schema

Crucible owns schema provisioning and migrations. The authoritative fresh baseline is `macro/services/crucible/src/migration/metabase/schema.sql` in the Monogram repository, executed by `crucible mb migrate`.

The current format is `1.1-dev1-catalog1`. Catalog creation and forks also run through Crucible. This repository does not maintain a second production provisioning script. The self-contained DuckDB fixture in `test/sql/multi_catalog/managed_catalogs.test` exists to test the extension contract.
