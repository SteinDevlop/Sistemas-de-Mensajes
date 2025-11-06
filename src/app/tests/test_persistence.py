import psycopg2
import time
import pytest

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": 5432,
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
    
    # Asegurar que la estación existe para evitar violación de FK
    cursor.execute("""
        INSERT INTO weather_stations (id, name)
        VALUES (999, 'test_station')
        ON CONFLICT (id) DO NOTHING;
    """)
    db_conn.commit()

    # Limpiar datos existentes
    cursor.execute("DELETE FROM weather_logs WHERE id_station = 999;")
    db_conn.commit()
    
    # Insertar dato de prueba
    cursor.execute("""
        INSERT INTO weather_logs (id_station, dates, temperature_celsius)
        VALUES (999, NOW(), 22.5) RETURNING id;
    """)
    log_id = cursor.fetchone()[0]
    db_conn.commit()

    # Verificar que el dato existe
    cursor.execute("SELECT COUNT(*) FROM weather_logs WHERE id = %s;", (log_id,))
    count = cursor.fetchone()[0]
    assert count == 1, "El registro no se guardó correctamente"

    # limpiar
    cursor.execute("DELETE FROM weather_logs WHERE id = %s;", (log_id,))
    db_conn.commit()

def test_transaction_rollback(db_conn):
    cursor = db_conn.cursor()
    # Ensure station exists
    cursor.execute("INSERT INTO weather_stations (id, name) VALUES (998, 'tx_station') ON CONFLICT (id) DO NOTHING;")
    db_conn.commit()

    # Start a transaction, insert then rollback
    try:
        cursor.execute("BEGIN;")
        cursor.execute("""
            INSERT INTO weather_logs (id_station, dates, temperature_celsius)
            VALUES (998, NOW(), 10.0) RETURNING id;
        """)
        row = cursor.fetchone()
        assert row is not None
        # rollback
        cursor.execute("ROLLBACK;")
    finally:
        # After rollback the row should not exist
        cursor.execute("SELECT COUNT(*) FROM weather_logs WHERE id_station = 998;")
        cnt = cursor.fetchone()[0]
        assert cnt == 0
        db_conn.commit()
