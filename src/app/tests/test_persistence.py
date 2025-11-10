import psycopg2
import time
import pytest
import os

# Configuración de Conexión (Reutilizada de conftest.py) 

# Lee el host de la DB desde el entorno (generalmente 'db' en docker-compose).
DB_HOST = os.getenv("POSTGRES_HOST", "db")

# Diccionario con la configuración básica de la conexión.
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": DB_HOST,
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# La fixture db_conn es proporcionada por conftest.py y ofrece una conexión 
# limpia y lista para usar para cada función de prueba.

def test_data_persistence(db_conn):
    # Test para verificar que los datos se insertan y persisten correctamente.
    cursor = db_conn.cursor()
    
    # 1. Preparación: Inserta una estación de prueba (si no existe) para cumplir con la FK.
    cursor.execute("""
        INSERT INTO weather_stations (id, name)
        VALUES (999, 'test_station')
        ON CONFLICT (id) DO NOTHING;
    """)
    # Confirma la operación de preparación. La fixture db_conn tiene autocommit=True,
    # pero usamos commit() explícito aquí para asegurar que la estación exista inmediatamente.
    db_conn.commit()

    # 2. Limpieza Previa: Elimina cualquier registro de log anterior asociado a esta estación.
    cursor.execute("DELETE FROM weather_logs WHERE id_station = 999;")
    db_conn.commit()
    
    # 3. Inserción: Inserta un nuevo registro de log y obtiene su ID generado.
    cursor.execute("""
        INSERT INTO weather_logs (id_station, dates, temperature_celsius)
        VALUES (999, NOW(), 22.5) RETURNING id;
    """)
    # Captura el ID del registro insertado para poder verificarlo y limpiarlo después.
    log_id = cursor.fetchone()[0]
    db_conn.commit()

    # 4. Verificación: Cuenta el número de registros con el ID recién insertado.
    cursor.execute("SELECT COUNT(*) FROM weather_logs WHERE id = %s;", (log_id,))
    count = cursor.fetchone()[0]
    # Afirma que el conteo es exactamente 1, confirmando la persistencia.
    assert count == 1, "El registro no se guardó correctamente"

    # 5. Limpieza Posterior: Elimina el registro de log creado por el test.
    cursor.execute("DELETE FROM weather_logs WHERE id = %s;", (log_id,))
    db_conn.commit()

def test_transaction_rollback(db_conn):
    # Test para verificar que si una transacción no se confirma (COMMIT), se revierte (ROLLBACK).
    cursor = db_conn.cursor()
    
    # 1. Preparación: Asegura que la estación de prueba 998 exista.
    cursor.execute("INSERT INTO weather_stations (id, name) VALUES (998, 'tx_station') ON CONFLICT (id) DO NOTHING;")
    db_conn.commit()

    try:
        # 2. Inicio de Transacción: Inicia explícitamente una nueva transacción.
        # Necesario porque la fixture db_conn tiene autocommit=True, pero queremos probar el ROLLBACK.
        cursor.execute("BEGIN;")
        
        # 3. Operación Transaccional: Inserta un registro DENTRO de la transacción.
        cursor.execute("""
            INSERT INTO weather_logs (id_station, dates, temperature_celsius)
            VALUES (998, NOW(), 10.0) RETURNING id;
        """)
        row = cursor.fetchone()
        assert row is not None # Confirma que la inserción dentro de la TX ocurrió.
        
        # 4. Rollback: Revierte la transacción. La inserción NO debe persistir.
        cursor.execute("ROLLBACK;")
        
    finally:
        # 5. Verificación: Comprueba que el registro insertado fue eliminado por el ROLLBACK.
        cursor.execute("SELECT COUNT(*) FROM weather_logs WHERE id_station = 998;")
        cnt = cursor.fetchone()[0]
        # Afirma que el conteo de registros es cero.
        assert cnt == 0
        # Finaliza la conexión de la función con un commit de la selección (aunque ya está limpia).
        db_conn.commit()