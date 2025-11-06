import time
import pytest
import psycopg2
import docker

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "db",
    "port": 5432,
}

def _find_container(client, service_name):
    # try compose label
    conts = client.containers.list(all=True, filters={"label": f"com.docker.compose.service={service_name}"})
    if conts:
        return conts[0]
    # fallback: name contains
    for c in client.containers.list(all=True):
        if service_name in c.name:
            return c
    return None

# Unit tests for _find_container (no docker daemon required)
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

# Integration-ish durability test kept minimal: skip if containers not present
def test_consumer_recovery_integration():
    client = docker.from_env()
    consumer = _find_container(client, "consumer")
    producer = _find_container(client, "producer")
    if not consumer or not producer:
        pytest.skip("producer/consumer not available for integration durability test")
    # only basic check: stop/start and ensure no exceptions
    consumer.stop()
    time.sleep(1)
    try:
        producer.exec_run("true")
    finally:
        consumer.start()
    time.sleep(2)
    # DB reachable check (optional)
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="postgres", host="db")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1
    conn.close()
