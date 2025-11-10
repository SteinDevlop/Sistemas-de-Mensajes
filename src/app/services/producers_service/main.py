# Este módulo simula la función de un productor (o gateway de estaciones IoT) que genera 
# datos meteorológicos y los publica continuamente en el exchange de RabbitMQ.
# Incluye un mecanismo de reintento para la conexión inicial y métricas de Prometheus.

import pika
import json
import random
import time
import logging
import os
from datetime import datetime
from prometheus_client import Counter, start_http_server

# ==========================
# Configuración de logging
# ==========================
# Configuración básica para el productor, imprimiendo a la consola.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ==========================
# Métricas Prometheus
# ==========================
# Contador para rastrear el número total de mensajes publicados.
MESSAGES_PUBLISHED = Counter('messages_published_total', 'Total de mensajes publicados en RabbitMQ')
start_http_server(8001)  # Producer expone métricas en puerto 8001 (diferente al 8000 del consumidor)
logging.info("Servidor de métricas Prometheus iniciado en puerto 8001")

# ==========================
# Variables de entorno (.env)
# ==========================
# Carga de credenciales y host, con valores predeterminados seguros para desarrollo.
RABBIT_USER = os.getenv("RABBIT_USER")
RABBIT_PASS = os.getenv("RABBIT_PASS")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST")  # Debería ser 'rabbitmq' en docker-compose
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "weather_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY", "weather.data")

# ==========================
# Función generadora de datos
# ==========================
def generar_datos():
    """Genera datos meteorológicos aleatorios simulando una estación."""
    return {
        # Simula 5 estaciones diferentes (IDs del 1 al 5)
        "id_station": random.randint(1, 5),
        # Marca de tiempo en formato ISO 8601 (requerido por la validación del consumidor)
        "dates": datetime.now().isoformat(),
        # Datos meteorológicos simulados dentro de rangos razonables
        "temperature_celsius": round(random.uniform(-10, 60), 2),
        "humidity": round(random.uniform(10, 100), 2),
        "wind": random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
        "wind_speed": round(random.uniform(0, 100), 2),
        "pressure": round(random.uniform(950, 1050), 2)
    }

# ==========================
# Función de conexión con reintentos
# ==========================
def connect_with_retry(params, max_attempts=None):
    """Intenta conectar a RabbitMQ con backoff exponencial."""
    attempt = 0
    backoff = 0.5 # Tiempo de espera inicial
    while True:
        attempt += 1
        try:
            logging.info("Intentando conexión a RabbitMQ (intento %d)...", attempt)
            return pika.BlockingConnection(params)
        except Exception as e:
            logging.error("Conexión fallida (intento %d): %s", attempt, e)
            if max_attempts and attempt >= max_attempts:
                logging.error("Superado número máximo de intentos.")
                raise # Falla si se excede el número de reintentos
            time.sleep(backoff)
            # Duplica el tiempo de espera (hasta un máximo de 10 segundos)
            backoff = min(backoff * 2, 10)

# ==========================
# Función principal
# ==========================
def main():
    # Usa 'guest' por defecto si las variables de entorno no están configuradas
    credentials = pika.PlainCredentials(RABBIT_USER or "guest", RABBIT_PASS or "guest")
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST or "rabbitmq", 
        port=RABBITMQ_PORT, 
        credentials=credentials
    )
    logging.info("Conectando a RabbitMQ en %s:%s con usuario '%s'...", 
                 RABBITMQ_HOST or "rabbitmq", RABBITMQ_PORT, RABBIT_USER or "guest")
    
    # Intenta la conexión hasta que tenga éxito
    connection = connect_with_retry(params)
    channel = connection.channel()

    # Declarar exchange duradero
    channel.exchange_declare(
        exchange=RABBITMQ_EXCHANGE,
        exchange_type='direct',
        durable=True # El exchange persistirá incluso si el broker cae
    )

    logging.info(f"Conectado. Publicando mensajes en exchange '{RABBITMQ_EXCHANGE}' cada 5 segundos...")

    try:
        while True:
            data = generar_datos()
            message = json.dumps(data)

            # Publicación del mensaje
            channel.basic_publish(
                exchange=RABBITMQ_EXCHANGE,
                routing_key=ROUTING_KEY,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Hace que el mensaje sea persistente en la cola
                )
            )

            MESSAGES_PUBLISHED.inc() # Incrementa el contador de Prometheus
            logging.info(f"Mensaje publicado: {message}")
            time.sleep(5) # Espera 5 segundos antes de generar el siguiente dato

    except KeyboardInterrupt:
        logging.info("Servicio detenido manualmente.")
    except Exception as e:
        logging.error(f"Error durante la publicación: {e}")
    finally:
        connection.close()
        logging.info("Conexión cerrada.")


if __name__ == "__main__":
    main()