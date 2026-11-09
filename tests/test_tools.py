import pytest

from ratchet import queue, tools
from ratchet.queue import LeaseLost
from ratchet.tools import ToolExecutor, external_send


def effects(conn, run_id=None):
    return conn.execute("SELECT count(*) AS n FROM external_effects").fetchone()["n"]


@pytest.fixture
def claim(conn):
    queue.enqueue(conn, {"n": 1})
    return queue.claim(conn, "w1", lease_seconds=60)


def test_repeating_a_tool_call_does_not_repeat_the_effect(conn, claim):
    ex = ToolExecutor(use_keys=True)
    first = ex.send_email(conn, claim, 1, {"to": "a@x.com"})
    second = ex.send_email(conn, claim, 1, {"to": "a@x.com"})
    assert first == second
    assert effects(conn) == 1


def test_crash_after_the_effect_but_before_recording_it(conn, claim):
    """The nasty case. The email went out, the worker died before writing the key down."""
    external_send(conn, claim.run_id, 1, "dead-worker", dedupe_key=f"{claim.run_id}:1")
    assert conn.execute("SELECT count(*) AS n FROM idempotency_keys").fetchone()["n"] == 0

    retry = ToolExecutor(use_keys=True).send_email(conn, claim, 1, {"to": "a@x.com"})

    assert retry["delivered"]
    assert effects(conn) == 1       # the outside system ignored the repeat
    assert conn.execute("SELECT count(*) AS n FROM idempotency_keys").fetchone()["n"] == 1


def test_without_keys_a_retry_duplicates_the_effect(conn, claim):
    """This is what a naive runtime does, and why the keys are there."""
    ex = ToolExecutor(use_keys=False)
    ex.send_email(conn, claim, 1, {"to": "a@x.com"})
    ex.send_email(conn, claim, 1, {"to": "a@x.com"})
    assert effects(conn) == 2


def test_different_steps_are_different_effects(conn, claim):
    ex = ToolExecutor(use_keys=True)
    ex.send_email(conn, claim, 1, {"to": "a@x.com"})
    ex.send_email(conn, claim, 3, {"to": "b@x.com"})
    assert effects(conn) == 2


def test_a_stale_worker_does_not_start_a_new_effect(conn):
    queue.enqueue(conn, {"n": 1})
    stale = queue.claim(conn, "w1", lease_seconds=-1)
    queue.claim(conn, "w2", lease_seconds=60)
    with pytest.raises(LeaseLost):
        ToolExecutor(use_keys=True).send_email(conn, stale, 1, {"to": "a@x.com"})
    assert effects(conn) == 0
