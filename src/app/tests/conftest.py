import os
import socket
import time
import pytest
import psycopg2

def running_in_docker():
    """Detecta si el código se ejecuta dentro de un contenedor Docker."""
    try:
        with open("/proc/1/cgroup", "rt") as f:
            return "docker" in f.read() or "containerd" in f.read()
    except Exception:
        return False

DB_HOST = "db" if running_in_docker() else "localhost"

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
