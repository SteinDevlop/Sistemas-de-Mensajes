FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Instalar dependencias del sistema necesarias si hacen falta paquetes nativos.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar requirements si existe.
COPY requirements.txt /app/requirements.txt
RUN if [ -f /app/requirements.txt ]; then \
      python -m pip install --upgrade pip && \
      pip install --no-cache-dir -r /app/requirements.txt ; \
    else \
      python -m pip install --upgrade pip && \
      pip install --no-cache-dir pika python-dotenv ; \
    fi

# Copiar código
COPY . /app

# Crear usuario no-root (opcional)
RUN adduser --disabled-password --gecos "" appuser || true
USER appuser

# Comando por defecto (puede ser sobreescrito en docker-compose)
CMD ["python", "-u", "main.py"]