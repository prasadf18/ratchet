import threading

from ratchet import queue, worker
from ratchet.agent import Agent
from ratchet.llm import FakeLLM, LLMGateway
from ratchet.tools import ToolExecutor


class CountingLLM:
    """Gives a different answer every call, like a real model, and counts the calls."""

    def __init__(self):
        self.calls = 0

    def complete(self, prompt):
        self.calls += 1
        return f"send an email to user-{self.calls}"


class CrashOnce:
    """Wraps a model and blows up on one chosen call, as if the worker died there."""

    def __init__(self, inner, crash_on_call):
        self.inner, self.crash_on_call, self.seen, self.crashed = inner, crash_on_call, 0, False

    def complete(self, prompt):
        self.seen += 1
        if self.seen == self.crash_on_call and not self.crashed:
            self.crashed = True
            raise RuntimeError("worker died here")
        return self.inner.complete(prompt)


def make_agent(llm):
    return Agent(LLMGateway(llm, retries=0), ToolExecutor(use_keys=True), worker_id="test")


def count(conn, table):
    return conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]


def test_a_run_records_every_step_and_finishes(dsn, conn):
    run_id, _ = queue.enqueue(conn, {"rounds": 3, "task": "demo"})
    claim = queue.claim(conn, "w1")
    assert worker.process(conn, dsn, claim, make_agent(FakeLLM()), lease=10) == "done"

    steps = queue.load_steps(conn, run_id)
    assert [s["kind"] for s in steps] == ["llm", "tool"] * 3
    assert [s["seq"] for s in steps] == list(range(6))
    assert count(conn, "external_effects") == 3
    assert conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,)).fetchone()["status"] == "done"


def test_a_crashed_run_resumes_without_redoing_finished_work(dsn, conn, monkeypatch):
    monkeypatch.setattr(queue, "backoff", lambda attempt: 0)
    run_id, _ = queue.enqueue(conn, {"rounds": 3, "task": "demo"})
    model = CountingLLM()
    agent = make_agent(CrashOnce(model, crash_on_call=3))     # dies while asking for round 2

    outcomes = [worker.process(conn, dsn, queue.claim(conn, w), agent, lease=10) for w in ("w1", "w2")]

    assert outcomes == ["failed", "done"]
    assert model.calls == 3                      # each round was asked exactly once, never repeated
    assert count(conn, "external_effects") == 3  # and each email went out exactly once
    assert count(conn, "steps") == 6


def test_replay_uses_the_answer_the_model_gave_the_first_time(dsn, conn, monkeypatch):
    """A real model would answer differently the second time. The run must not."""
    monkeypatch.setattr(queue, "backoff", lambda attempt: 0)
    run_id, _ = queue.enqueue(conn, {"rounds": 2, "task": "demo"})
    agent = make_agent(CrashOnce(CountingLLM(), crash_on_call=2))

    for w in ("w1", "w2"):
        worker.process(conn, dsn, queue.claim(conn, w), agent, lease=10)

    steps = queue.load_steps(conn, run_id)
    for ask, act in zip(steps[0::2], steps[1::2]):
        assert act["output"]["to"] == ask["output"]["text"].split()[-1]


def test_a_run_stops_when_it_is_told_the_lease_is_lost(dsn, conn):
    import pytest
    from ratchet.queue import LeaseLost

    queue.enqueue(conn, {"rounds": 3})
    claim = queue.claim(conn, "w1")
    lost = threading.Event()
    lost.set()
    with pytest.raises(LeaseLost):
        make_agent(FakeLLM())(conn, claim, lost)
    assert count(conn, "steps") == 0
