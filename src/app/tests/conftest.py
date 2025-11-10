import os
import time
import socket
import pytest
import psycopg2

# Use environment variable provided by docker-compose so tests inside the
# `tests` container always resolve the DB host correctly (typically 'db').
DB_HOST = os.getenv("POSTGRES_HOST", "db")

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": DB_HOST,
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

@pytest.fixture(scope="session")
def db_config():
    return DB_CONFIG

@pytest.fixture(scope="function")
def db_conn(db_config):
    """Devuelve una conexión limpia por cada test (evita estados 'aborted')."""
    max_retries = 12
    last_exc = None
    for attempt in range(max_retries):
        try:
            # First ensure the hostname resolves on the container/network DNS.
            host = db_config.get("host")
            try:
                socket.gethostbyname(host)
            except socket.gaierror:
                # If DNS doesn't resolve yet, wait and retry
                last_exc = psycopg2.OperationalError(f"could not resolve host {host}")
                if attempt == max_retries - 1:
                    raise last_exc
                time.sleep(5)
                continue

            conn = psycopg2.connect(**db_config)
            conn.autocommit = True
            yield conn
            try:
                conn.close()
            except Exception:
                pass
            return
        except psycopg2.OperationalError as e:
            last_exc = e
            if attempt == max_retries - 1:
                raise
            time.sleep(5)
    if last_exc:
        raise last_exc
