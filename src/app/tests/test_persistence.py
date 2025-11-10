import psycopg2
import time
import pytest
import os

# Read DB host from environment (set by docker-compose). This ensures the
# tests container connects to the compose 'db' service instead of localhost.
DB_HOST = os.getenv("POSTGRES_HOST", "db")

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": DB_HOST,
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# Use the shared `db_conn` fixture from `conftest.py` which includes DNS
# resolution and retry logic. This file's tests receive `db_conn` as a
# parameter (function-scoped connection) provided by conftest.

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
