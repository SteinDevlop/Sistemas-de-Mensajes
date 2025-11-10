import psycopg2
import time
import pytest
import socket
import os

def resolve_db_host():
    try:
        socket.gethostbyname("db")
        return "db"
    except socket.error:
        return "localhost"

def running_in_docker():
    """Detecta si el entorno actual está dentro de un contenedor Docker."""
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

@pytest.fixture(scope="module")
def db_conn():
    max_retries = 12
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            yield conn
            conn.close()
            return
        except psycopg2.OperationalError:
            if attempt == max_retries - 1:
                raise
            time.sleep(5)

def test_data_persistence(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        INSERT INTO weather_stations (id, name)
        VALUES (999, 'test_station')
        ON CONFLICT (id) DO NOTHING;
    """)
    db_conn.commit()

    cursor.execute("DELETE FROM weather_logs WHERE id_station = 999;")
    db_conn.commit()
    
    cursor.execute("""
        INSERT INTO weather_logs (id_station, dates, temperature_celsius)
        VALUES (999, NOW(), 22.5) RETURNING id;
    """)
    log_id = cursor.fetchone()[0]
    db_conn.commit()

    cursor.execute("SELECT COUNT(*) FROM weather_logs WHERE id = %s;", (log_id,))
    count = cursor.fetchone()[0]
    assert count == 1, "El registro no se guardó correctamente"

    cursor.execute("DELETE FROM weather_logs WHERE id = %s;", (log_id,))
    db_conn.commit()

def test_transaction_rollback(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("INSERT INTO weather_stations (id, name) VALUES (998, 'tx_station') ON CONFLICT (id) DO NOTHING;")
    db_conn.commit()

    try:
        cursor.execute("BEGIN;")
        cursor.execute("""
            INSERT INTO weather_logs (id_station, dates, temperature_celsius)
            VALUES (998, NOW(), 10.0) RETURNING id;
        """)
        row = cursor.fetchone()
        assert row is not None
        cursor.execute("ROLLBACK;")
    finally:
        cursor.execute("SELECT COUNT(*) FROM weather_logs WHERE id_station = 998;")
        cnt = cursor.fetchone()[0]
        assert cnt == 0
        db_conn.commit()
