import os
import json
import pika
import logging
from datetime import datetime


# ==============================
# CONFIGURACIÓN DE LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==============================
# VARIABLES DE ENTORNO (.env)
# ==============================
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "weather_exchange")
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "weather_queue")
ROUTING_KEY = os.getenv("ROUTING_KEY", "weather.data")

POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pass")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "db")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "8080")

# ==============================
# CONEXIÓN A POSTGRES (SQLAlchemy)
# ==============================
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
Base = declarative_base()

# ==============================
# MODELO DE TABLA
# ==============================
class WeatherData(Base):
    __tablename__ = "weather_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    id_station = Column(Integer, nullable=False)
    dates = Column(DateTime, nullable=False)
    temperature_celsius = Column(Float)
    humidity = Column(Float)
    wind = Column(String(5))
    wind_speed = Column(Float)
    pressure = Column(Float)

# Crear tabla si no existe
Base.metadata.create_all(engine)

# ==============================
# VALIDACIÓN DE DATOS
# ==============================
def validar_datos(data: dict) -> bool:
    """Valida los rangos de los datos meteorológicos."""
    try:
        if not (1 <= data.get("id_station", 0) <= 5):
            raise ValueError("id_station fuera de rango (1-5).")

        if not (-50 <= data.get("temperature_celsius", 0) <= 70):
            raise ValueError("Temperatura fuera de rango (-50 a 70°C).")

        if not (0 <= data.get("humidity", 0) <= 100):
            raise ValueError("Humedad fuera de rango (0 a 100%).")

        if data.get("wind") not in ["N", "S", "E", "W", "NE", "NW", "SE", "SW"]:
            raise ValueError("Dirección de viento inválida.")

        if not (0 <= data.get("wind_speed", 0) <= 300):
            raise ValueError("Velocidad de viento fuera de rango (0 a 300 km/h).")

        if not (850 <= data.get("pressure", 0) <= 1100):
            raise ValueError("Presión fuera de rango (850 a 1100 hPa).")

        return True

    except Exception as e:
        logging.error(f"Error de validación: {e}. Datos: {data}")
        return False

# ==============================
# PROCESAMIENTO DE MENSAJE
# ==============================
def procesar_mensaje(ch, method, properties, body):
    """Callback que procesa cada mensaje recibido desde RabbitMQ."""
    session = Session()
    try:
        data = json.loads(body)
        logging.info(f"Mensaje recibido: {data}")

        if validar_datos(data):
            registro = WeatherData(
                id_station=data["id_station"],
                dates=datetime.fromisoformat(data["dates"]),
                temperature_celsius=data["temperature_celsius"],
                humidity=data["humidity"],
                wind=data["wind"],
                wind_speed=data["wind_speed"],
                pressure=data["pressure"]
            )
            session.add(registro)
            session.commit()
            logging.info(f"Datos insertados correctamente en PostgreSQL (station {data['id_station']}).")
        else:
            logging.warning(f"Datos inválidos descartados: {data}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        logging.error("Error al decodificar JSON.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except SQLAlchemyError as e:
        logging.error(f"Error al insertar en base de datos: {e}")
        session.rollback()
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    finally:
        session.close()

# ==============================
# CONSUMIDOR RABBITMQ
# ==============================
def main():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declarar exchange y cola
    channel.exchange_declare(exchange=RABBITMQ_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.queue_bind(exchange=RABBITMQ_EXCHANGE, queue=RABBITMQ_QUEUE, routing_key=ROUTING_KEY)

    logging.info(f"Esperando mensajes en la cola '{RABBITMQ_QUEUE}'... (Ctrl+C para salir)")

    channel.basic_consume(
        queue=RABBITMQ_QUEUE,
        on_message_callback=procesar_mensaje,
        auto_ack=False
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info("Consumidor detenido manualmente.")
    finally:
        connection.close()
        logging.info("Conexión cerrada con RabbitMQ.")

if __name__ == "__main__":
    main()
