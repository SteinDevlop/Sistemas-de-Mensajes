import os
import json
import time
import logging
from datetime import datetime
from typing import Optional

import pika
import psycopg2
from psycopg2 import OperationalError, sql

# 1. IMPORTAR LA FUNCIÓN AVANZADA DE LOGGING
from utils.logger import setup_logger
# ==============================
# CONFIGURACIÓN DE LOGGING AVANZADA (¡CAMBIADO!)
# ==============================
LOG_DIR = os.getenv("LOG_DIR", "logs") # Directorio de logs dentro del contenedor
# Llama a tu función para configurar el logger.
# Los logs irán a la consola Y al archivo 'logs/consumer.log'
logger = setup_logger("CONSUMER_LOG", f"{LOG_DIR}/consumer.log")

# ==============================
# VARIABLES DE ENTORNO
# ==============================
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "weather_exchange")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "weather_queue")
ROUTING_KEY = os.getenv("ROUTING_KEY", "weather.data")

# Dead Letter Queue (DLQ)
RABBITMQ_DLQ_EXCHANGE = os.getenv("RABBITMQ_DLQ_EXCHANGE", "weather_dlq_exchange")
RABBITMQ_DLQ_QUEUE = os.getenv("RABBITMQ_DLQ_QUEUE", "weather_dlq_queue")

POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_SSLMODE = os.getenv("POSTGRES_SSLMODE", "disable") # use 'require' if using SSL/TLS

# Pool/retry settings
DB_MAX_RETRIES = int(os.getenv("DB_MAX_RETRIES", 5))
DB_RETRY_BASE_SLEEP = float(os.getenv("DB_RETRY_BASE_SLEEP", 1.0))
RABBIT_RECONNECT_DELAY = float(os.getenv("RABBIT_RECONNECT_DELAY", 5.0))

# SQL INSERT statement (parametrizado)
INSERT_SQL = """
INSERT INTO weather_logs (
  id_station, dates, temperature_celsius, humidity,
  wind, wind_speed, pressure
) VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# ==============================
# FUNCIONES DE CONEXIÓN A PostgreSQL (psycopg2)
# ==============================

def make_db_dsn() -> str:
  """Construye el DSN de conexión y no expone la contraseña en logs."""
  # No hacemos logging del DSN completo para evitar filtrar credenciales
  dsn = (
    f"dbname={POSTGRES_DB} host={POSTGRES_HOST} user={POSTGRES_USER} port={POSTGRES_PORT}"
  )
  return dsn


def connect_db_with_retry(max_retries: int = DB_MAX_RETRIES) -> psycopg2.extensions.connection:
  """Intentar conectarse a PostgreSQL con backoff exponencial.
  Lanza excepción si no es posible conectar tras max_retries.
  """
  attempt = 0
  while True:
    try:
      dsn = make_db_dsn()
      conn = psycopg2.connect(dsn, password=POSTGRES_PASSWORD, sslmode=POSTGRES_SSLMODE)
      conn.autocommit = False
      logger.info("Conectado a PostgreSQL.")
      return conn

    except OperationalError as e:
      attempt += 1
      logger.warning("Error conectando a PostgreSQL (intento %d/%d): %s", attempt, max_retries, str(e))
      if attempt >= max_retries:
        logger.error("No se pudo conectar a PostgreSQL después de %d intentos.", max_retries)
        raise
      sleep_time = DB_RETRY_BASE_SLEEP * (2 ** (attempt - 1))
      logger.info("Reintentando en %.1f segundos...", sleep_time)
      time.sleep(sleep_time)


# Conexión global reutilizable
_db_conn: Optional[psycopg2.extensions.connection] = None


def get_db_conn() -> psycopg2.extensions.connection:
  """Devuelve una conexión activa; reconecta si la conexión está cerrada o falla."""
  global _db_conn
  try:
    if _db_conn is None or _db_conn.closed != 0:
      _db_conn = connect_db_with_retry()
    # simple check: ping
    with _db_conn.cursor() as cur:
      cur.execute("SELECT 1")
    return _db_conn
  except Exception as e:
    logger.warning("Problema con la conexión DB actual: %s. Reintentando conexión...", str(e))
    _db_conn = connect_db_with_retry()
    return _db_conn


# ==============================
# VALIDACIÓN DE DATOS
# ==============================

def validar_datos(data: dict) -> bool:
  """Valida rangos y tipos mínimos. Regresa True si válido, False si no."""
  try:
    # Campos obligatorios
    required = ["id_station", "dates", "temperature_celsius", "humidity", "wind", "wind_speed", "pressure"]
    for k in required:
      if k not in data:
        raise ValueError(f"Falta campo requerido: {k}")

    id_station = int(data.get("id_station"))
    if not (1 <= id_station <= 9999): # adaptar rango si se conoce
      raise ValueError("id_station fuera de rango (1-9999).")

    # fechas: intentar parsear
    try:
      _ = datetime.fromisoformat(data.get("dates"))
    except Exception:
      raise ValueError("Formato de fecha inválido. Use ISO 8601.")

    temp = float(data.get("temperature_celsius"))
    if not (-50.0 <= temp <= 70.0):
      raise ValueError("Temperatura fuera de rango (-50 a 70°C).")

    hum = float(data.get("humidity"))
    if not (0.0 <= hum <= 100.0):
      raise ValueError("Humedad fuera de rango (0 a 100%).")

    wind = str(data.get("wind")).upper()
    allowed_winds = {"N","S","E","W","NE","NW","SE","SW"}
    if wind not in allowed_winds:
      raise ValueError("Dirección de viento inválida.")

    ws = float(data.get("wind_speed"))
    if not (0.0 <= ws <= 300.0):
      raise ValueError("Velocidad de viento fuera de rango (0 a 300 km/h).")

    pres = float(data.get("pressure"))
    if not (300.0 <= pres <= 1200.0): # rango ampliado para seguridad
      raise ValueError("Presión fuera de rango (300 a 1200 hPa).")

    return True

  except Exception as e:
    logger.error("Validación fallida: %s. Datos: %s", e, data)
    return False


# ==============================
# OPERACIONES DE BD
# ==============================

def insertar_weather(data: dict) -> None:
  """Inserta un registro en weather_logs. Realiza commit o lanza excepción en fallo."""
  conn = get_db_conn()
  try:
    with conn.cursor() as cur:
      cur.execute(
        INSERT_SQL,
        (
          int(data["id_station"]),
          datetime.fromisoformat(data["dates"]),
          float(data["temperature_celsius"]),
          float(data["humidity"]),
          str(data["wind"]),
          float(data["wind_speed"]),
          float(data["pressure"]) 
        )
      )
    conn.commit()
    logger.info("Inserción en DB exitosa (station=%s).", data.get("id_station"))
  except Exception as e:
    conn.rollback()
    logger.exception("Error al insertar en DB, se hace rollback.")
    raise


# ==============================
# RABBITMQ - DLQ PUBLISH
# ==============================

def publish_to_dlq(channel: pika.channel.Channel, body: bytes, properties: pika.spec.BasicProperties = None) -> None:
  """Publica el mensaje original en la DLQ (intercambio durable)."""
  try:
    # usamos el mismo body y propiedades mínimas
    channel.exchange_declare(exchange=RABBITMQ_DLQ_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(queue=RABBITMQ_DLQ_QUEUE, durable=True)
    channel.queue_bind(exchange=RABBITMQ_DLQ_EXCHANGE, queue=RABBITMQ_DLQ_QUEUE, routing_key=ROUTING_KEY)

    channel.basic_publish(
      exchange=RABBITMQ_DLQ_EXCHANGE,
      routing_key=ROUTING_KEY,
      body=body,
      properties=properties or pika.BasicProperties(delivery_mode=2) # persistent
    )
    logger.info("Mensaje publicado en DLQ: %s", RABBITMQ_DLQ_QUEUE)
  except Exception:
    logger.exception("Fallo al publicar en DLQ.")


# ==============================
# CALLBACK DE PROCESAMIENTO
# ==============================

def procesar_mensaje(ch: pika.channel.Channel, method, properties, body: bytes):
  """Procesa mensaje: valida, inserta y ACK/NACK según corresponda.

  Reglas:
  - Si válido: insertar en DB y ACK.
  - Si inválido: publicar en DLQ y ACK (evitar requeue infinito).
  - Si error DB transitorio: NACK con requeue=True para reintentar.
  - Errores irreparables: NACK requeue=False para no bloquear la cola.
  """
  logger.info("Mensaje recibido. Delivery tag=%s", method.delivery_tag)

  try:
    payload = json.loads(body)
  except json.JSONDecodeError:
    logger.error("JSON inválido. Enviando a DLQ.")
    # publicar en DLQ y ack para evitar reintentos por formato inválido
    try:
      publish_to_dlq(ch, body, properties)
    finally:
      ch.basic_ack(delivery_tag=method.delivery_tag)
    return

  # Validación
  if not validar_datos(payload):
    logger.warning("Datos inválidos -> DLQ. Delivery tag=%s", method.delivery_tag)
    try:
      publish_to_dlq(ch, body, properties)
    finally:
      # ACK original para no reencolar en la cola principal
      ch.basic_ack(delivery_tag=method.delivery_tag)
    return

  # Intentar insertar en DB
  try:
    insertar_weather(payload)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logger.info("Mensaje procesado y ACK enviado. Delivery tag=%s", method.delivery_tag)

  except OperationalError as e:
    # Problema de conexión a la BD: requeue para intentar después
    logger.warning("OperationalError al insertar, NACK+requeue. Error: %s", str(e))
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

  except Exception as e:
    # Error en la inserción que no sea reconectable: enviar a DLQ para análisis y ACK
    logger.exception("Error irreparable al insertar. Moviendo a DLQ.")
    try:
      publish_to_dlq(ch, body, properties)
    finally:
      ch.basic_ack(delivery_tag=method.delivery_tag)


# ==============================
# INICIALIZACIÓN Y LOOP
# ==============================

def main():
  credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)

  params = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=RABBITMQ_PORT,
    credentials=credentials,
    heartbeat=60,
    blocked_connection_timeout=300
  )

  while True:
    try:
      connection = pika.BlockingConnection(params)
      channel = connection.channel()

      # QoS: prefetch_count=1 es obligatorio para procesamiento ordenado
      channel.basic_qos(prefetch_count=1)

      # Declarar exchanges y colas (durable)
      channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='direct', durable=True)
      channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
      channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key=ROUTING_KEY)

      # Preparar DLQ
      channel.exchange_declare(exchange=RABBITMQ_DLQ_EXCHANGE, exchange_type='direct', durable=True)
      channel.queue_declare(queue=RABBITMQ_DLQ_QUEUE, durable=True)
      channel.queue_bind(exchange=RABBITMQ_DLQ_EXCHANGE, queue=RABBITMQ_DLQ_QUEUE, routing_key=ROUTING_KEY)

      logger.info("Esperando mensajes en la cola '%s'...", RABBITMQ_QUEUE)

      channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=procesar_mensaje, auto_ack=False)

      try:
        channel.start_consuming()
      except KeyboardInterrupt:
        logger.info("Consumidor detenido por teclado.")
        try:
          channel.stop_consuming()
        except Exception:
          pass
        break
      finally:
        try:
          connection.close()
        except Exception:
          pass

    except pika.exceptions.AMQPConnectionError as e:
      logger.warning("No se puede conectar a RabbitMQ: %s. Reintentando en %.1f s...", e, RABBIT_RECONNECT_DELAY)
      time.sleep(RABBIT_RECONNECT_DELAY)
    except Exception:
      logger.exception("Error inesperado en el loop principal. Reintentando en %.1f s...", RABBIT_RECONNECT_DELAY)
      time.sleep(RABBIT_RECONNECT_DELAY)


if __name__ == "__main__":
  main()