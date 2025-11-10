# Monitoreo con Prometheus y Grafana

## Descripción

El sistema expone métricas de Prometheus para monitorear:
- **Mensajes Publicados**: Contador de mensajes enviados a RabbitMQ por el producer
- **Mensajes Guardados**: Contador de mensajes exitosamente guardados en PostgreSQL por el consumer

## Acceso

### Prometheus
- **URL**: http://localhost:9090
- **Scraped targets**: 
  - consumer:8000 (consumer metrics)
  - producer:8001 (producer metrics)

### Grafana
- **URL**: http://localhost:3000
- **Usuario**: admin
- **Contraseña**: admin

## Métricas Disponibles

### Del Producer (puerto 8001)
- `messages_published_total`: Número total de mensajes publicados en RabbitMQ

### Del Consumer (puerto 8000)
- `messages_processed_total`: Número total de mensajes guardados en PostgreSQL

## Dashboard Incluido

El dashboard automático "Sistema de Mensajes - Monitoreo" incluye:

1. **Gráfico de líneas**: Mensajes publicados vs guardados (histórico)
2. **Tasa de mensajes**: Velocidad de procesamiento (mensajes/minuto)
3. **Gráfico de torta**: Proporción entre publicados vs guardados
4. **Estadísticas**: Total de mensajes publicados y guardados

## Verificación Rápida

1. Acceder a Prometheus: http://localhost:9090
   - Ir a "Targets" para verificar que consumer y producer están siendo scrappeados
   - Ejecutar queries como:
     - `messages_published_total`
     - `messages_processed_total`
     - `rate(messages_published_total[1m])`
     - `rate(messages_processed_total[1m])`

2. Acceder a Grafana: http://localhost:3000
   - El dashboard debe cargarse automáticamente
   - Si no aparece, navegar a Home > Dashboards y buscar "Sistema de Mensajes"

## Troubleshooting

### Las métricas no aparecen en Prometheus
- Verificar que producer y consumer estén corriendo:
  ```bash
  docker-compose ps
  ```
- Revisar logs del producer y consumer:
  ```bash
  docker logs consumer | grep Prometheus
  docker logs producer | grep Prometheus
  ```

### Grafana no muestra datos
- Verificar que Prometheus está scrappeando los targets:
  - Ir a http://localhost:9090/targets
  - Verificar estado de consumer:8000 y producer:8001
- Revisar Data Sources en Grafana (http://localhost:3000/datasources)
  - Debe existir una data source "Prometheus" apuntando a http://prometheus:9090

### Error de conexión a Prometheus desde Grafana
- Asegurarse que están en la misma red Docker:
  ```bash
  docker network inspect <network_name>
  ```
- Ambos contenedores deben estar conectados a la red `backend`
