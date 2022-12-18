# trino-iceberg-maintenance

This utility tool will run maintenance tasks on your Iceberg tables.

It will:

* Remove any orphan files that exceed the configured retention
* Expire snapshots that exceed the configured retention
* analyze table or specific columns every configured period
* optimize table every configured period

## Configure the maintenance

The configuration is stored in the `iceberg_maintenance_schedule` table.

```
table_name VARCHAR NOT NULL,
should_analyze INTEGER,
last_analyzed_on TIMESTAMP(6),
days_to_analyze INTEGER,
columns_to_analyze ARRAY(VARCHAR),
should_optimize INTEGER,
last_optimized_on TIMESTAMP(6),
days_to_optimize INTEGER,
should_expire_snapshots INTEGER,
retention_days_snapshots INTEGER,
should_remove_orphan_files INTEGER,
retention_days_orphan_files INTEGER
```

The job can be scheduled using cron and configured through environment variables (currently only basic authentication is supported).

It requires the latest trino-python-client to be installed.

```bash
export TRINO_HOST=localhost
export TRINO_PORT=8080
export TRINO_USER=admin
export TRINO_PASSWORD=...
export TRINO_CATALOG=iceberg
export TRINO_SCHEMA=default
python -m trino-iceberg-maintenance
```
