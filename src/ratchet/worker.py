"""The worker loop: claim a job, run a handler, keep the lease alive meanwhile.

A handler is any function (conn, claim, lost) -> result dict. `lost` is an
Event that gets set if the heartbeat finds out the lease is gone, so a
long-running handler can stop early instead of doing wasted work.
"""
import logging
import threading
import time
from typing import Callable

from ratchet import db, queue
from ratchet.queue import Claim, LeaseLost

log = logging.getLogger("ratchet.worker")

Handler = Callable[..., dict]


class Heartbeat(threading.Thread):
    """Renews the lease every `every` seconds on its own connection."""

    def __init__(self, dsn: str, claim: Claim, lease: float, every: float):
        super().__init__(daemon=True)
        self.dsn, self.claim, self.lease, self.every = dsn, claim, lease, every
        self.lost = threading.Event()
        self._halt = threading.Event()

    def run(self) -> None:
        conn = db.connect(self.dsn)
        try:
            while not self._halt.wait(self.every):
                try:
                    queue.heartbeat(conn, self.claim, self.lease)
                except LeaseLost:
                    self.lost.set()
                    return
        finally:
            conn.close()

    def stop(self) -> None:
        self._halt.set()
        self.join()


def process(conn, dsn: str, claim: Claim, handler: Handler, lease: float, heartbeat_every: float | None = None) -> str:
    """Run one claimed job. Returns 'done', 'failed' or 'lost'."""
    beat = Heartbeat(dsn, claim, lease, heartbeat_every if heartbeat_every is not None else lease / 3)
    beat.start()
    try:
        result = handler(conn, claim, beat.lost)
        queue.complete(conn, claim, result)
        return "done"
    except LeaseLost:
        log.warning("lost the lease on job %s attempt %s", claim.job_id, claim.attempt)
        return "lost"
    except Exception as exc:  # the handler is user code, so catch broadly and retry
        log.warning("job %s attempt %s failed: %r", claim.job_id, claim.attempt, exc)
        try:
            queue.fail(conn, claim, repr(exc), retry_in=queue.backoff(claim.attempt))
        except LeaseLost:
            return "lost"
        return "failed"
    finally:
        beat.stop()


def _work_remaining(conn) -> bool:
    row = conn.execute("SELECT count(*) AS n FROM jobs WHERE state IN ('queued', 'leased')").fetchone()
    return row["n"] > 0


def run(dsn: str, worker_id: str, handler: Handler, lease: float = 10.0, poll: float = 0.05,
        stop: threading.Event | None = None, exit_when_idle: bool = False) -> None:
    """Keep claiming and processing jobs until stopped (or, optionally, until the queue is empty)."""
    conn = db.connect(dsn)
    try:
        while not (stop is not None and stop.is_set()):
            claim = queue.claim(conn, worker_id, lease)
            if claim is None:
                queue.bury_exhausted(conn)
                if exit_when_idle and not _work_remaining(conn):
                    return
                time.sleep(poll)
                continue
            process(conn, dsn, claim, handler, lease)
    finally:
        conn.close()
