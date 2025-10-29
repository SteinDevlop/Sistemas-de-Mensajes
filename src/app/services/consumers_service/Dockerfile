FROM python:3.11-slim

# Evitar buffering de stdout (logs en tiempo real).
ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Dependencias del sistema necesarias para paquetes comunes (psycopg2, etc).
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements si existe y instalar dependencias.
# Si no tienes requirements.txt, crea uno con: pika, SQLAlchemy, psycopg2-binary, python-dotenv (si aplica).
COPY requirements.txt /app/requirements.txt
RUN if [ -f /app/requirements.txt ]; then \
      python -m pip install --upgrade pip && \
      pip install --no-cache-dir -r /app/requirements.txt ; \
    else \
      python -m pip install --upgrade pip && \
      pip install --no-cache-dir pika SQLAlchemy psycopg2-binary python-dotenv ; \
    fi

# Copiar código de la aplicación.
COPY . /app

# Usuario no root (opcional): comenta si no lo deseas.
RUN adduser --disabled-password --gecos "" appuser || true
USER appuser

# Comando por defecto (puedes sobreescribirlo en docker-compose).
CMD ["python", "-u", "main.py"]