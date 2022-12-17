import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from textwrap import dedent
from typing import NamedTuple, List, Optional

from trino.dbapi import connect

# The number of maintenance jobs you want to run at the same time
NUM_WORKERS = 5
# The table that contains the maintenance configuration
MAINTENANCE_TABLE = "iceberg_maintenance_schedule"

logger = logging.getLogger("IcebergMaintenance")


def get_trino_connection():
    return connect(
        host="<host>",
        port=443,
        user="<username>",
        catalog="<catalog>",
        schema="<schema>",
        experimental_python_types=True,
    )


def create_if_not_exists_management_table():
    create_table_statement = dedent(f"""
    CREATE TABLE IF EXISTS {MAINTENANCE_TABLE} (
        table_name VARCHAR,
        should_analyze INTEGER NOT NULL,
        last_analyzed_on TIMESTAMP(6),
        columns_to_analyze ARRAY[VARCHAR],
        should_optimize INTEGER NOT NULL,
        last_optimized_on TIMESTAMP(6)
        days_to_optimize INTEGER,
        should_expire_snapshots INTEGER NOT NULL,
        retention_days_snapshots INTEGER,
        should_remove_orphan_files INTEGER NOT NULL,
        retention_days_orphan_files INTEGER
    """)
    with get_trino_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(create_table_statement)


class MaintenanceProperties(NamedTuple):
    table_name: str
    should_analyze: bool
    last_analyzed_on: Optional[datetime.datetime]
    days_to_analyze: int
    columns_to_analyze: List[str]
    should_optimize: bool
    last_optimized_on: Optional[datetime.datetime]
    days_to_optimize: int
    should_expire_snapshots: bool
    retention_days_snapshots: int
    should_remove_orphan_files: bool
    retention_days_orphan_files: int

    @classmethod
    def from_row(cls, row):
        return cls(*row)


class MaintenanceTaskException(Exception):
    def __init__(
            self,
            maintenance_properties: MaintenanceProperties,
            message="An exception occurred while running maintenance tasks"
    ):
        self.maintenance_properties = maintenance_properties
        super().__init__(message)


class MaintenanceTask:
    def __init__(self, maintenance_properties: MaintenanceProperties):
        self.maintenance_properties = maintenance_properties

    def execute(self):
        (
            table_name,
            should_analyze,
            last_analyzed_on,
            days_to_analyze,
            columns_to_analyze,
            should_optimize,
            last_optimized_on,
            days_to_optimize,
            should_expire_snapshots,
            retention_days_snapshots,
            should_remove_orphan_files,
            retention_days_orphan_files,

        ) = self.maintenance_properties
        try:
            with get_trino_connection() as conn:
                cur = conn.cursor()
                # Removing orphan files
                if should_remove_orphan_files:
                    logging.info(f"Removing orphan files for {table_name}")

                    cur.execute(dedent(f"""
                        ALTER TABLE {table_name} EXECUTE remove_orphan_files(
                            retention_threshold => '{retention_days_orphan_files}d'
                        )"""))
                    logging.info(f"Removing orphan files for {table_name} completed")

                # Expiring snapshots
                if should_expire_snapshots:
                    logging.info(f"Expiring snapshots for {table_name}")

                    cur.execute(dedent(f"""
                        ALTER TABLE {table_name} EXECUTE expire_snapshots(
                            retention_threshold => '{retention_days_snapshots}d'
                        )"""))
                    logging.info(f"Expiring snapshots for {table_name} completed")

                # Optimizing
                if (
                        should_optimize
                        and (
                        not last_optimized_on
                        or last_optimized_on + datetime.timedelta(days=days_to_optimize) > datetime.datetime.now()
                )
                ):
                    logging.info(f"Optimizing {table_name}")

                    cur.execute(f"ALTER TABLE {table_name} EXECUTE optimize")

                    cur.execute(dedent(f"""
                        UPDATE {MAINTENANCE_TABLE} 
                        SET last_optimized_on = current_timestamp(6)
                        WHERE table_name = '{table_name}'
                        """))
                    logging.info(f"Optimizing {table_name} completed")

                # Analyzing
                if (
                        should_analyze
                        and (
                        not last_analyzed_on
                        or last_analyzed_on + datetime.timedelta(days=days_to_analyze) > datetime.datetime.now()
                )
                ):
                    logging.info(f"Analyzing {table_name}")

                    with_columns = "" if len(columns_to_analyze) == 0 else f" WITH ({', '.join(columns_to_analyze)})"
                    cur.execute(f"ANALYZE {table_name}{with_columns}")

                    cur.execute(dedent(f"""
                        UPDATE {MAINTENANCE_TABLE} 
                        SET last_analyzed_on = current_timestamp(6)
                        WHERE table_name = '{table_name}'
                        """))
        except Exception as e:
            raise MaintenanceTaskException(self.maintenance_properties) from e


if __name__ == '__main__':
    create_if_not_exists_management_table()

    with get_trino_connection() as conn:
        cur = conn.cursor()
        result = cur.execute(f"SELECT * FROM {MAINTENANCE_TABLE}")
        tasks = result.fetchall()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = []
        for task in tasks:
            futures.append(executor.submit(MaintenanceTask(
                MaintenanceProperties.from_row(task)
            ).execute))

        for future in as_completed(futures):
            try:
                future.result()
            except MaintenanceTaskException as e:
                logger.exception(
                    "An exception has occurred while running maintenance tasks for "
                    f"{e.maintenance_properties.table_name}"
                )
