import datetime
import uuid
from textwrap import dedent

import pytest
import trino.dbapi
from freezegun import freeze_time

from trino_iceberg_maintenance.__main__ import (
    create_if_not_exists_management_table,
    run_maintenance,
    MAINTENANCE_TABLE,
)


@pytest.fixture(autouse=True)
def run_before_and_after_tests(trino_connection):
    create_if_not_exists_management_table(trino_connection)
    yield
    trino_connection.cursor().execute(f"DROP TABLE {MAINTENANCE_TABLE}")


def connection_factory(host, port):
    def factory():
        return trino.dbapi.connect(
            host=host,
            port=port,
            user="admin",
            catalog="iceberg",
            schema="default",
            experimental_python_types=True,
        )

    return factory


def create_random_suffix():
    return uuid.uuid4().hex


def test_optimize(trino_connection, trino_server):
    host, port = trino_server
    cur = trino_connection.cursor()
    table_name = "t_" + create_random_suffix()
    cur.execute(f"CREATE TABLE {table_name} (a VARCHAR, b VARCHAR)")

    # Let's create two files
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES ('a', 'b')")
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES ('a', 'b')")
    cur.execute(f"""SELECT * from "{table_name}$files" """)
    assert len(cur.fetchall()) == 2

    # Running maintenance without config shouldn't do anything
    run_maintenance(trino_connection, connection_factory(host, port))

    cur.execute(f"""SELECT * from "{table_name}$files" """)
    assert len(cur.fetchall()) == 2

    # Configuring maintenance
    cur.execute(dedent(f"""
    INSERT INTO {MAINTENANCE_TABLE} (table_name, should_optimize, days_to_optimize) 
    VALUES ('{table_name}', 1, 10)"""))

    run_maintenance(trino_connection, connection_factory(host, port))

    cur.execute(f"""SELECT * from "{table_name}$files" """)
    assert len(cur.fetchall()) == 1

    # Running maintenance again shouldn't optimize again
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES ('a', 'b')")
    cur.execute(f"""SELECT * from "{table_name}$files" """)
    assert len(cur.fetchall()) == 2

    # It should run after the configured delta
    with freeze_time(datetime.datetime.now() + datetime.timedelta(days=11)):
        run_maintenance(trino_connection, connection_factory(host, port))
        cur.execute(f"""SELECT * from "{table_name}$files" """)
        assert len(cur.fetchall()) == 1


def test_analyze_without_colums(trino_connection, trino_server):
    host, port = trino_server
    cur = trino_connection.cursor()
    table_name = "t_" + create_random_suffix()
    cur.execute(f"CREATE TABLE {table_name} (a VARCHAR, b VARCHAR)")

    # Let's create two files
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES (NULL, NULL)")
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES (NULL, NULL)")
    cur.execute(f"SHOW STATS FOR {table_name}")
    rows = cur.fetchall()
    assert rows[0][3] == 1.0  # null fraction should be 1 for column a

    # Running maintenance without config shouldn't do anything
    run_maintenance(trino_connection, connection_factory(host, port))

    cur.execute(f"SHOW STATS FOR {table_name}")
    rows = cur.fetchall()
    assert rows[0][3] == 1.0  # null fraction should be 1 for column a

    # Configuring maintenance
    cur.execute(dedent(f"""
    INSERT INTO {MAINTENANCE_TABLE} (table_name, should_analyze, days_to_analyze) 
    VALUES ('{table_name}', 1, 10)"""))

    run_maintenance(trino_connection, connection_factory(host, port))

    cur.execute(f"SHOW STATS FOR {table_name}")
    rows = cur.fetchall()
    assert rows[0][3] == 1.0  # null fraction should be 1 for column a

    # Running maintenance again shouldn't optimize again
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES ('a', 'b')")
    cur.execute(f"SHOW STATS FOR {table_name}")
    rows = cur.fetchall()
    assert rows[0][3] == 1.0  # null fraction should be 1 for column a

    # It should run after the configured delta
    with freeze_time(datetime.datetime.now() + datetime.timedelta(days=11)):
        run_maintenance(trino_connection, connection_factory(host, port))
        cur.execute(f"SHOW STATS FOR {table_name}")
        rows = cur.fetchall()
        assert rows[0][3] == 0.6666666666666666


def test_analyze_with_colums(trino_connection, trino_server):
    host, port = trino_server
    cur = trino_connection.cursor()
    table_name = "t_" + create_random_suffix()
    cur.execute(f"CREATE TABLE {table_name} (a VARCHAR, b VARCHAR)")

    # Let's create two files
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES (NULL, NULL)")
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES (NULL, NULL)")
    cur.execute(f"SHOW STATS FOR {table_name}")
    assert cur.fetchall()[0][3] == 1.0  # null fraction should be 1 for column a

    # Running maintenance without config shouldn't do anything
    run_maintenance(trino_connection, connection_factory(host, port))

    cur.execute(f"SHOW STATS FOR {table_name}")
    assert cur.fetchall()[0][3] == 1.0  # null fraction should be 1 for column a

    # Configuring maintenance
    cur.execute(dedent(f"""
    INSERT INTO {MAINTENANCE_TABLE} (table_name, should_analyze, days_to_analyze, columns_to_analyze) 
    VALUES ('{table_name}', 1, 10, ARRAY['a'])"""))

    run_maintenance(trino_connection, connection_factory(host, port))

    cur.execute(f"SHOW STATS FOR {table_name}")
    rows = cur.fetchall()
    assert rows[0][3] == 1.0  # null fraction should be 1 for column a
    assert rows[1][3] == 1.0  # null fraction should be 1 for column b

    # Running maintenance again shouldn't optimize again
    cur.execute(f"INSERT INTO {table_name} (a, b) VALUES ('a', 'b')")
    cur.execute(f"SHOW STATS FOR {table_name}")
    rows = cur.fetchall()
    assert rows[0][3] == 1.0  # null fraction should be 1 for column a
    assert rows[1][3] == 0.6666666666666666  # null fraction should be 1 for column b

    # It should run after the configured delta
    with freeze_time(datetime.datetime.now() + datetime.timedelta(days=11)):
        run_maintenance(trino_connection, connection_factory(host, port))
        cur.execute(f"SHOW STATS FOR {table_name}")
        rows = cur.fetchall()
        assert rows[0][3] == 0.6666666666666666
        assert rows[1][3] == 0.6666666666666666
