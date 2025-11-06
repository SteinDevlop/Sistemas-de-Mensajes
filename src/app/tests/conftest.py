import time
import pytest
import psycopg2

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": 5432,
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
            # Autocommit por defecto para que una excepción en una sentencia
            # no deje la conexión en estado aborted para otros tests.
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
    # Si sale del loop sin conectar
    if last_exc:
        raise last_exc