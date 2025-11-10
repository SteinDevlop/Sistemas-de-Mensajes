import pika
import json
import os
import time

# === Configuración de conexión RabbitMQ ===
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
# Usa 'rabbitmq' si se ejecuta dentro del stack de Docker
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq") 

credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
connection = None

# Esperar a que RabbitMQ esté listo con un reintento simple
for i in range(1, 11):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=5672,
            credentials=credentials
        ))
        print("Conexión a RabbitMQ exitosa.")
        break
    except Exception as e:
        print(f"RabbitMQ no disponible (intento {i}/10), reintentando en 3 segundos...")
        time.sleep(3)

if not connection:
    raise Exception("No se pudo conectar a RabbitMQ después de múltiples intentos.")

channel = connection.channel()

# Asegurar que la cola existe
queue_name = "weather_queue"
# La cola debe ser duradera, como está configurada en el consumidor.
channel.queue_declare(queue=queue_name, durable=True)

# Mensaje de prueba (similar a lo que produce normalmente tu servicio)
test_message = {
    "id_station": 1,
    "dates": "2025-11-06T12:00:00Z",
    "temperature_celsius": 25.4,
    "humidity": 70.5,
    "wind": "N",
    "wind_speed": 3.5,
    "pressure": 1013.2
}

# Enviar mensaje al exchange por defecto, usando el nombre de la cola como routing key
channel.basic_publish(
    exchange='', # Exchange por defecto
    routing_key=queue_name,
    body=json.dumps(test_message),
    properties=pika.BasicProperties(
        delivery_mode=2,  # Hacer mensaje persistente
    )
)

print(f"Mensaje de prueba enviado correctamente a RabbitMQ en la cola '{queue_name}'.")

connection.close()