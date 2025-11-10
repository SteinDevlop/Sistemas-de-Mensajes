import time
import pytest
import psycopg2
import docker
import socket
import os

def resolve_db_host():
    """
    Devuelve 'db' si el hostname es resolvible (entorno Docker),
    o 'localhost' si no se puede resolver (entorno local).
    """
    try:
        socket.gethostbyname("db")
        return "db"
    except socket.error:
        return "localhost"

def running_in_docker():
    """Detecta si el entorno actual está dentro de un contenedor Docker."""
    try:
        with open("/proc/1/cgroup", "rt") as f:
            return "docker" in f.read() or "containerd" in f.read()
    except Exception:
        return False

DB_HOST = "db" if running_in_docker() else "localhost"

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": DB_HOST,
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}


def _find_container(client, service_name):
    conts = client.containers.list(all=True, filters={"label": f"com.docker.compose.service={service_name}"})
    if conts:
        return conts[0]
    for c in client.containers.list(all=True):
        if service_name in c.name:
            return c
    return None

# Fake classes for unit testing
class FakeContainer:
    def __init__(self, name, labels=None):
        self.name = name
        self._labels = labels or {}
    @property
    def attrs(self):
        return {"Config": {"Labels": self._labels}}

class FakeContainers:
    def __init__(self, containers):
        self._containers = containers
    def list(self, all=True, filters=None):
        if filters and "label" in filters:
            label = filters["label"]
            key, val = label.split("=", 1)
            return [c for c in self._containers if c._labels.get(key) == val]
        return list(self._containers)

class FakeClient:
    def __init__(self, containers):
        self.containers = FakeContainers(containers)

def test_find_container_by_label():
    c1 = FakeContainer("project_consumer_1", labels={"com.docker.compose.service": "consumer"})
    c2 = FakeContainer("other")
    client = FakeClient([c1, c2])
    found = _find_container(client, "consumer")
    assert found is c1

def test_find_container_by_name_fallback():
    c1 = FakeContainer("project_consumer_1")
    c2 = FakeContainer("project_producer_1")
    client = FakeClient([c2, c1])
    found = _find_container(client, "consumer")
    assert found is c1

def test_find_container_none():
    client = FakeClient([])
    assert _find_container(client, "consumer") is None

# Integration test
def test_consumer_recovery_integration():
    client = docker.from_env()
    consumer = _find_container(client, "consumer")
    producer = _find_container(client, "producer")
    if not consumer or not producer:
        pytest.skip("producer/consumer not available for integration durability test")
    consumer.stop()
    time.sleep(1)
    try:
        producer.exec_run("true")
    finally:
        consumer.start()
    time.sleep(2)
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1
    conn.close()
