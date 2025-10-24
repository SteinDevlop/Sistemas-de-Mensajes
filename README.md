# Sistemas-de-Mensajes
sistema de gestión de logs de estaciones meteorológicas
# Servicios

[Servicios Activos]

## Productores de datos (Producers)

Servicio en Python que simule o reciba datos de estaciones (JSON). Debe publicar a un exchange de RabbitMQ con mensajes durables.

## Consumidores (Consumers)

Microservicio en Python que:

    - Procesa los mensajes con *ack* manual.
    - Persiste en PostgreSQL (tabla `weather_logs`).
    - Valida rangos de valores y gestiona errores.

# Base de datos

esquema en PostgreSQL

[Organizacion de la base de datos]

# Docker y orquestación

[Implementacioon de docker para el lanzamiento de dependencias]

# Logs y monitoreo

[Como se muestra el trazeado y los logs]

# Lanzamiento

[Guia de ejecucion de servicios con terraform]