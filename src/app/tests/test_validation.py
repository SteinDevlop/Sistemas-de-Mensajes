import psycopg2
import pytest
import time
import psycopg2.errors

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

def test_out_of_range_temperature(db_conn):
    cursor = db_conn.cursor()

    # Asegurar que la estación existe para evitar FK errors
    cursor.execute("""
        INSERT INTO weather_stations (id, name)
        VALUES (1, 'station_1')
        ON CONFLICT (id) DO NOTHING;
    """)
    db_conn.commit()

    # Intentar inserción con temperatura fuera de rango
    with pytest.raises(psycopg2.errors.CheckViolation):
        cursor.execute("""
            INSERT INTO weather_logs (id_station, dates, temperature_celsius)
            VALUES (1, NOW(), 200);
        """)
        db_conn.commit()

