import threading

import pytest

from ratchet import db, queue
from ratchet.queue import LeaseLost


def count(conn, table):
    return conn.execute(f"SELECT count(*) AS n FROM {table}").fetchone()["n"]


def test_enqueue_creates_a_run_and_a_job(conn):
    run_id, created = queue.enqueue(conn, {"task": "summarize"})
    assert created
    assert count(conn, "runs") == 1
    assert count(conn, "jobs") == 1
    assert conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,)).fetchone()["status"] == "queued"


def test_same_request_key_returns_the_same_run(conn):
    first, created_first = queue.enqueue(conn, {"n": 1}, request_key="abc")
    second, created_second = queue.enqueue(conn, {"n": 2}, request_key="abc")
    assert first == second
    assert created_first and not created_second
    assert count(conn, "runs") == 1
    assert count(conn, "jobs") == 1


def test_each_job_is_claimed_once(conn):
    queue.enqueue(conn, {"n": 1})
    queue.enqueue(conn, {"n": 2})
    claims = [queue.claim(conn, "w1"), queue.claim(conn, "w2"), queue.claim(conn, "w3")]
    assert claims[2] is None
    assert {c.job_id for c in claims[:2]} == {1, 2}


def test_expired_lease_is_reclaimed_with_a_higher_attempt(conn):
    queue.enqueue(conn, {"n": 1})
    first = queue.claim(conn, "w1", lease_seconds=-1)  # already expired
    second = queue.claim(conn, "w2", lease_seconds=30)
    assert first.job_id == second.job_id
    assert (first.attempt, second.attempt) == (1, 2)


def test_unexpired_lease_is_not_reclaimed(conn):
    queue.enqueue(conn, {"n": 1})
    assert queue.claim(conn, "w1", lease_seconds=30) is not None
    assert queue.claim(conn, "w2") is None


def test_stale_worker_is_fenced_off(conn):
    """The failure the fencing token exists for.

    w1 stalls past its lease, w2 takes the job. When w1 wakes up it must not be
    able to write steps or finish the run.
    """
    queue.enqueue(conn, {"n": 1})
    stale = queue.claim(conn, "w1", lease_seconds=-1)
    current = queue.claim(conn, "w2", lease_seconds=30)

    with pytest.raises(LeaseLost):
        queue.record_step(conn, stale, 0, "llm", {"q": 1}, {"a": "stale"})
    with pytest.raises(LeaseLost):
        queue.heartbeat(conn, stale)
    with pytest.raises(LeaseLost):
        queue.complete(conn, stale, {"ok": True})

    assert queue.record_step(conn, current, 0, "llm", {"q": 1}, {"a": "fresh"})
    queue.complete(conn, current, {"ok": True})
    assert queue.load_steps(conn, current.run_id)[0]["output"] == {"a": "fresh"}


def test_recording_the_same_step_twice_is_a_replay(conn):
    queue.enqueue(conn, {"n": 1})
    c = queue.claim(conn, "w1")
    assert queue.record_step(conn, c, 0, "tool", {}, {"v": 1}) is True
    assert queue.record_step(conn, c, 0, "tool", {}, {"v": 2}) is False
    assert queue.load_steps(conn, c.run_id)[0]["output"] == {"v": 1}


def test_heartbeat_extends_the_lease(conn):
    queue.enqueue(conn, {"n": 1})
    c = queue.claim(conn, "w1", lease_seconds=1)
    queue.heartbeat(conn, c, lease_seconds=300)
    lease_left = conn.execute(
        "SELECT extract(epoch FROM lease_until - now()) AS s FROM jobs WHERE id = %s", (c.job_id,)
    ).fetchone()["s"]
    assert lease_left > 100


def test_failed_job_waits_before_retrying(conn):
    queue.enqueue(conn, {"n": 1})
    c = queue.claim(conn, "w1")
    queue.fail(conn, c, "model timed out", retry_in=60)
    assert queue.claim(conn, "w2") is None


def test_job_that_runs_out_of_attempts_goes_to_dead_letters(conn):
    run_id, _ = queue.enqueue(conn, {"n": 1}, max_attempts=2)
    queue.claim(conn, "w1", lease_seconds=-1)
    queue.claim(conn, "w2", lease_seconds=-1)
    assert queue.claim(conn, "w3") is None       # no attempts left
    assert queue.bury_exhausted(conn) == 1
    assert count(conn, "dead_letters") == 1
    assert conn.execute("SELECT status FROM runs WHERE id = %s", (run_id,)).fetchone()["status"] == "failed"


def test_many_workers_never_claim_the_same_job(dsn, conn):
    jobs = 60
    for n in range(jobs):
        queue.enqueue(conn, {"n": n})

    claimed, lock = [], threading.Lock()

    def worker(name):
        own = db.connect(dsn)
        while (c := queue.claim(own, name)) is not None:
            with lock:
                claimed.append(c.job_id)
        own.close()

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claimed) == jobs
    assert len(set(claimed)) == jobs
