import uuid

import pytest
from fastapi.testclient import TestClient

from ratchet import queue, worker
from ratchet.agent import Agent
from ratchet.api import create_app
from ratchet.llm import FakeLLM, LLMGateway
from ratchet.tools import ToolExecutor


@pytest.fixture
def client(dsn, conn):          # `conn` first so the tables are empty
    return TestClient(create_app(dsn))


def submit(client, key=None, **input):
    headers = {"Idempotency-Key": key} if key else {}
    return client.post("/runs", json={"input": input or {"rounds": 2}}, headers=headers)


def test_health(client):
    assert client.get("/health").json() == {"ok": True}


def test_submitting_creates_a_queued_run(client):
    response = submit(client)
    assert response.status_code == 202
    body = response.json()
    assert body["created"] is True
    status = client.get(f"/runs/{body['run_id']}").json()
    assert status["status"] == "queued"
    assert status["steps_finished"] == 0


def test_the_same_idempotency_key_returns_the_same_run(client):
    first = submit(client, key="order-42")
    second = submit(client, key="order-42")
    assert first.status_code == 202 and second.status_code == 200
    assert first.json()["run_id"] == second.json()["run_id"]
    assert second.json()["created"] is False


def test_different_keys_make_different_runs(client):
    assert submit(client, key="a").json()["run_id"] != submit(client, key="b").json()["run_id"]


def test_a_finished_run_shows_its_result_and_steps(client, dsn, conn):
    run_id = submit(client, key="k", rounds=2, task="demo").json()["run_id"]
    agent = Agent(LLMGateway(FakeLLM(), retries=0), ToolExecutor(use_keys=True))
    worker.run(dsn, "w1", agent, lease=5, poll=0.01, exit_when_idle=True)

    status = client.get(f"/runs/{run_id}").json()
    assert status["status"] == "done"
    assert status["steps_finished"] == 4
    assert status["result"]["rounds"] == 2


def test_an_unknown_run_is_a_404(client):
    assert client.get(f"/runs/{uuid.uuid4()}").status_code == 404


def test_a_bad_run_id_is_rejected(client):
    assert client.get("/runs/not-a-uuid").status_code == 422
