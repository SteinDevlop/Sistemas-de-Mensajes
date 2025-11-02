FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN if [ -f /app/requirements.txt ]; then \
      python -m pip install --upgrade pip && \
      pip install --no-cache-dir -r /app/requirements.txt ; \
    else \
      python -m pip install --upgrade pip && \
      pip install --no-cache-dir pika SQLAlchemy psycopg2-binary python-dotenv ; \
    fi

COPY . /app

# 🔹 Crear el directorio logs con permisos totales ANTES de cambiar de usuario
RUN mkdir -p /app/logs && chmod -R 777 /app/logs

# 🔹 Crear el usuario no root y darle propiedad del directorio
RUN adduser --disabled-password --gecos "" appuser || true && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-u", "main.py"]
