import os
import time

import pytest
import trino.dbapi
from testcontainers.compose import DockerCompose


@pytest.fixture(scope="session")
def trino_server():
    package_root_directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
    with DockerCompose(package_root_directory, compose_file_name="docker-compose-trino.yml", pull=True) as compose:
        host = compose.get_service_host("trino", 8080)
        port = compose.get_service_port("trino", 8080)
        timeout_start = time.time()
        while time.time() < timeout_start + 30:
            stdout, stderr = compose.get_logs()
            if stderr:
                raise RuntimeError(f"""Could not start Trino container: {stderr.decode("utf-8")}""")
            if "SERVER STARTED" in stdout.decode("utf-8"):
                yield host, port
                return
            time.sleep(2)
        raise RuntimeError("Timeout exceeded: Could not start Trino container")


@pytest.fixture(scope="session")
def trino_connection(trino_server):
    host, port = trino_server
    yield trino.dbapi.connect(
        host=host,
        port=port,
        user="admin",
        catalog="iceberg",
        schema="default",
        experimental_python_types=True
    )
