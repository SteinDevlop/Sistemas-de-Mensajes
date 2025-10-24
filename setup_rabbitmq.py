import pika

RABBITMQ_HOST = "rabbitmq"
EXCHANGE_NAME = "weather.exchange"
QUEUE_NAME = "weather.queue.logs"
ROUTING_KEY = "weather.logs"

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST)
)
channel = connection.channel()

# Exchange durable
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)

# Cola durable
channel.queue_declare(queue=QUEUE_NAME, durable=True)

# Binding
channel.queue_bind(exchange=EXCHANGE_NAME, queue=QUEUE_NAME, routing_key=ROUTING_KEY)

connection.close()
