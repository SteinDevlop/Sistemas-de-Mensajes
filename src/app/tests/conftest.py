import os
import time
import socket
import pytest
import psycopg2

# Usa la variable de entorno proporcionada por docker-compose para que los tests
# dentro del contenedor 'tests' siempre resuelvan el host de la DB correctamente.
DB_HOST = os.getenv("POSTGRES_HOST", "db")

# Diccionario que almacena la configuración de la conexión a PostgreSQL.
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": DB_HOST,
    # El puerto se convierte a entero, con 5432 como valor por defecto.
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# --- Pytest Fixtures ---

@pytest.fixture(scope="session")
def db_config():
    # Fixture con alcance de sesión, devuelve la configuración de la DB.
    # Esta configuración es la misma para todos los tests en la sesión.
    return DB_CONFIG

@pytest.fixture(scope="function")
def db_conn(db_config):
    """Devuelve una conexión limpia por cada test (evita estados 'aborted')."""
    
    # Alcance de función: se ejecuta una vez por cada test que la solicite.
    
    max_retries = 12  # Número máximo de intentos de conexión.
    last_exc = None   # Almacena la última excepción si falla la conexión.
    
    # Bucle de reintento para esperar a que la base de datos esté lista (wait-for-it pattern).
    for attempt in range(max_retries):
        try:
            # Primero asegura que el nombre del host se resuelva en el DNS del contenedor/red.
            host = db_config.get("host")
            try:
                # Intenta resolver el nombre del host a una dirección IP.
                socket.gethostbyname(host)
            except socket.gaierror:
                # Si falla la resolución DNS (gaierror), se considera que el servicio no está listo.
                last_exc = psycopg2.OperationalError(f"could not resolve host {host}")
                if attempt == max_retries - 1:
                    # Si es el último intento, lanza el error.
                    raise last_exc
                time.sleep(5) # Espera 5 segundos antes de reintentar la resolución.
                continue

            # Si la resolución del host fue exitosa, intenta establecer la conexión.
            conn = psycopg2.connect(**db_config)
            # Activa el autocommit para evitar transacciones abiertas y estados 'aborted'
            # entre tests, ideal para entornos de prueba.
            conn.autocommit = True
            
            # El uso de 'yield' convierte la fixture en un generador, permitiendo código
            # de 'setup' antes de la ejecución del test y código de 'teardown' después.
            yield conn
            
            # --- Teardown (código que se ejecuta después de que el test termina) ---
            try:
                # Cierra la conexión al finalizar el test.
                conn.close()
            except Exception:
                # Ignora cualquier error al cerrar la conexión.
                pass
            return # Finaliza el bucle y la ejecución de la fixture si la conexión fue exitosa.
        
        except psycopg2.OperationalError as e:
            # Captura errores operacionales de Psycopg2 (como conexión rechazada).
            last_exc = e
            if attempt == max_retries - 1:
                # Lanza el error si es el último intento.
                raise
            time.sleep(5) # Espera 5 segundos antes de reintentar la conexión.
            
    # Si el bucle termina sin una conexión exitosa (y sin haber lanzado el error),
    # lanza la última excepción capturada (aunque esto debería ser redundante si el
    # 'raise' del último intento funciona correctamente).
    if last_exc:
        raise last_exc