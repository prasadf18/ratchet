import threading
import time

import pytest

from ratchet import db, queue, worker


def run_status(conn, run_id):
    return conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,)).fetchone()["status"]


def test_successful_handler_marks_the_run_done(dsn, conn):
    run_id, _ = queue.enqueue(conn, {"n": 1})
    claim = queue.claim(conn, "w1")
    outcome = worker.process(conn, dsn, claim, lambda c, cl, lost: {"ok": True}, lease=5)
    assert outcome == "done"
    assert run_status(conn, run_id) == "done"


def test_handler_error_requeues_and_the_next_attempt_succeeds(dsn, conn, monkeypatch):
    monkeypatch.setattr(queue, "backoff", lambda attempt: 0)
    run_id, _ = queue.enqueue(conn, {"n": 1})
    calls = []

    def flaky(c, claim, lost):
        calls.append(claim.attempt)
        if claim.attempt == 1:
            raise RuntimeError("model timed out")
        return {"ok": True}

    for worker_id in ("w1", "w2"):
        claim = queue.claim(conn, worker_id)
        worker.process(conn, dsn, claim, flaky, lease=5)

    assert calls == [1, 2]
    assert run_status(conn, run_id) == "done"


def test_heartbeat_keeps_the_lease_during_a_slow_handler(dsn, conn):
    """Lease is 0.6s but the handler takes 1.5s. Nobody else may steal the job."""
    run_id, _ = queue.enqueue(conn, {"n": 1})
    claim = queue.claim(conn, "w1", lease_seconds=0.6)
    stolen = []

    def slow(c, cl, lost):
        deadline = time.time() + 1.5
        other = db.connect(dsn)
        while time.time() < deadline:
            stolen.append(queue.claim(other, "intruder", lease_seconds=5))
            time.sleep(0.1)
        other.close()
        return {"ok": True}

    outcome = worker.process(conn, dsn, claim, slow, lease=0.6)
    assert outcome == "done"
    assert all(s is None for s in stolen)


def test_worker_that_loses_its_lease_cannot_finish_the_run(dsn, conn):
    """No heartbeat, short lease: another worker takes over, the first is fenced off."""
    run_id, _ = queue.enqueue(conn, {"n": 1})
    first = queue.claim(conn, "w1", lease_seconds=0.3)
    taken = {}

    def stalled(c, cl, lost):
        time.sleep(0.6)  # lease expires meanwhile
        other = db.connect(dsn)
        taken["claim"] = queue.claim(other, "w2", lease_seconds=30)
        other.close()
        return {"from": "stale worker"}

    outcome = worker.process(conn, dsn, first, stalled, lease=0.3, heartbeat_every=999)
    assert outcome == "lost"
    assert taken["claim"].attempt == 2
    assert run_status(conn, run_id) == "running"   # the stale worker did not finish it


def test_run_loop_drains_the_queue_and_exits(dsn, conn):
    ids = [queue.enqueue(conn, {"n": n})[0] for n in range(5)]
    worker.run(dsn, "w1", lambda c, cl, lost: {"ok": True}, lease=5, poll=0.01, exit_when_idle=True)
    assert all(run_status(conn, i) == "done" for i in ids)
