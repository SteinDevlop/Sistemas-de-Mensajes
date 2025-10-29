import pika
import json
import random
import time
import logging
import os
from datetime import datetime

# ==========================
# Configuración de logging
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==========================
# Variables de entorno (.env)
# ==========================
RABBIT_USER = os.getenv("RABBIT_USER")
RABBIT_PASS = os.getenv("RABBIT_PASS")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")  # Nombre del servicio en docker-compose
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "weather_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY", "weather.data")

# ==========================
# Función generadora de datos
# ==========================
def generar_datos():
    """Genera datos meteorológicos aleatorios simulando una estación."""
    return {
        "id_station": random.randint(1, 5),
        "dates": datetime.now().isoformat(),
        "temperature_celsius": round(random.uniform(-10, 60), 2),
        "humidity": round(random.uniform(10, 100), 2),
        "wind": random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
        "wind_speed": round(random.uniform(0, 100), 2),
        "pressure": round(random.uniform(950, 1050), 2)
    }

# ==========================
# Función principal
# ==========================
def main():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials
    )

    logging.info(f"Conectando a RabbitMQ en {RABBITMQ_HOST}:{RABBITMQ_PORT} con usuario '{RABBIT_USER}'...")
    
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declarar exchange duradero
    channel.exchange_declare(
        exchange=RABBITMQ_EXCHANGE,
        exchange_type='direct',
        durable=True
    )

    logging.info(f"Conectado. Publicando mensajes en exchange '{RABBITMQ_EXCHANGE}' cada 5 segundos...")

    try:
        while True:
            data = generar_datos()
            message = json.dumps(data)

            channel.basic_publish(
                exchange=RABBITMQ_EXCHANGE,
                routing_key=ROUTING_KEY,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Persistente
                )
            )

            logging.info(f"Mensaje publicado: {message}")
            time.sleep(5)

    except KeyboardInterrupt:
        logging.info("Servicio detenido manualmente.")
    except Exception as e:
        logging.error(f"Error durante la publicación: {e}")
    finally:
        connection.close()
        logging.info("Conexión cerrada.")


if __name__ == "__main__":
    main()