"""The job queue: enqueue, claim with a lease, heartbeat, complete, fail.

How a worker stays safe
-----------------------
claim() hands out a Claim with an `attempt` number that goes up on every claim.
That number is the fencing token. Everything a worker writes afterwards is
checked against it, so a worker that was slow, lost its lease, and woke up
later gets LeaseLost instead of overwriting what the new worker did.
"""
import random
from dataclasses import dataclass
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb


class LeaseLost(Exception):
    """The worker no longer holds the lease for this job (it expired and someone else took it)."""


@dataclass(frozen=True)
class Claim:
    job_id: int
    run_id: UUID
    attempt: int


def enqueue(conn: psycopg.Connection, input: dict, request_key: str | None = None,
            max_attempts: int = 5) -> tuple[UUID, bool]:
    """Create a run and its job in one transaction.

    Returns (run_id, created). If request_key was already used, nothing new is
    created and the original run comes back with created=False.
    """
    with conn.transaction():
        row = conn.execute(
            "INSERT INTO runs (request_key, input) VALUES (%s, %s) "
            "ON CONFLICT (request_key) DO NOTHING RETURNING id",
            (request_key, Jsonb(input)),
        ).fetchone()
        if row is None:
            existing = conn.execute("SELECT id FROM runs WHERE request_key = %s", (request_key,)).fetchone()
            return existing["id"], False
        conn.execute("INSERT INTO jobs (run_id, max_attempts) VALUES (%s, %s)", (row["id"], max_attempts))
        return row["id"], True


_CLAIM = """
WITH next AS (
    SELECT id FROM jobs
    WHERE attempt < max_attempts
      AND ((state = 'queued' AND available_at <= now())
        OR (state = 'leased' AND lease_until < now()))
    ORDER BY available_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs
SET state = 'leased',
    lease_owner = %(worker)s,
    lease_until = now() + make_interval(secs => %(lease)s),
    attempt = attempt + 1
FROM next
WHERE jobs.id = next.id
RETURNING jobs.id AS job_id, jobs.run_id, jobs.attempt
"""


def claim(conn: psycopg.Connection, worker: str, lease_seconds: float = 30) -> Claim | None:
    """Take the next job, or return None if there is nothing to do.

    FOR UPDATE SKIP LOCKED means two workers never fight over the same row:
    whoever locks it first gets it, the other one just moves to the next row.
    A job whose lease has expired counts as available, so a crashed worker's
    job is picked up here without any separate cleanup process.
    """
    with conn.transaction():
        row = conn.execute(_CLAIM, {"worker": worker, "lease": lease_seconds}).fetchone()
        if row is None:
            return None
        conn.execute("UPDATE runs SET status = 'running', updated_at = now() WHERE id = %s", (row["run_id"],))
        return Claim(row["job_id"], row["run_id"], row["attempt"])


def heartbeat(conn: psycopg.Connection, claim: Claim, lease_seconds: float = 30) -> None:
    """Push the lease further out. Raises LeaseLost if the job has moved on."""
    cur = conn.execute(
        "UPDATE jobs SET lease_until = now() + make_interval(secs => %s) "
        "WHERE id = %s AND attempt = %s AND state = 'leased'",
        (lease_seconds, claim.job_id, claim.attempt),
    )
    if cur.rowcount != 1:
        raise LeaseLost(f"job {claim.job_id} attempt {claim.attempt}")


def complete(conn: psycopg.Connection, claim: Claim, result: dict) -> None:
    """Mark the job and its run done. Raises LeaseLost if the attempt is stale."""
    with conn.transaction():
        cur = conn.execute(
            "UPDATE jobs SET state = 'done', lease_owner = NULL, lease_until = NULL "
            "WHERE id = %s AND attempt = %s AND state = 'leased'",
            (claim.job_id, claim.attempt),
        )
        if cur.rowcount != 1:
            raise LeaseLost(f"job {claim.job_id} attempt {claim.attempt}")
        conn.execute(
            "UPDATE runs SET status = 'done', result = %s, updated_at = now() WHERE id = %s",
            (Jsonb(result), claim.run_id),
        )


def fail(conn: psycopg.Connection, claim: Claim, error: str, retry_in: float = 0) -> None:
    """Give the job back so it can be tried again after retry_in seconds.

    When attempts run out, bury_exhausted() moves it to dead_letters.
    """
    cur = conn.execute(
        "UPDATE jobs SET state = 'queued', lease_owner = NULL, lease_until = NULL, last_error = %s, "
        "available_at = now() + make_interval(secs => %s) "
        "WHERE id = %s AND attempt = %s AND state = 'leased'",
        (error, retry_in, claim.job_id, claim.attempt),
    )
    if cur.rowcount != 1:
        raise LeaseLost(f"job {claim.job_id} attempt {claim.attempt}")


def record_step(conn: psycopg.Connection, claim: Claim, seq: int, kind: str,
                input: dict | None, output: dict | None) -> bool:
    """Append a finished step to the checkpoint log.

    Returns False if that step was already recorded (a replay). Raises
    LeaseLost if this attempt is no longer the current one.
    """
    with conn.transaction():
        # FOR SHARE keeps the job row from being re-claimed until we commit,
        # so the check and the insert can't be separated by a takeover.
        holder = conn.execute(
            "SELECT 1 FROM jobs WHERE id = %s AND attempt = %s AND state = 'leased' FOR SHARE",
            (claim.job_id, claim.attempt),
        ).fetchone()
        if holder is None:
            raise LeaseLost(f"job {claim.job_id} attempt {claim.attempt}")
        cur = conn.execute(
            "INSERT INTO steps (run_id, seq, kind, input, output, attempt) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (run_id, seq) DO NOTHING",
            (claim.run_id, seq, kind, Jsonb(input), Jsonb(output), claim.attempt),
        )
        return cur.rowcount == 1


def load_steps(conn: psycopg.Connection, run_id: UUID) -> list[dict]:
    """Everything this run has already finished, in order. Used to resume."""
    return conn.execute("SELECT * FROM steps WHERE run_id = %s ORDER BY seq", (run_id,)).fetchall()


def bury_exhausted(conn: psycopg.Connection) -> int:
    """Move jobs that used up their attempts to dead_letters and fail their runs."""
    cur = conn.execute(
        """
        WITH dead AS (
            UPDATE jobs SET state = 'dead', lease_owner = NULL, lease_until = NULL
            WHERE attempt >= max_attempts
              AND (state = 'queued' OR (state = 'leased' AND lease_until < now()))
            RETURNING run_id, attempt, last_error
        ), failed AS (
            UPDATE runs SET status = 'failed', updated_at = now()
            WHERE id IN (SELECT run_id FROM dead)
        )
        INSERT INTO dead_letters (run_id, attempts, reason)
        SELECT run_id, attempt, COALESCE(last_error, 'lease expired too many times') FROM dead
        ON CONFLICT (run_id) DO NOTHING
        """
    )
    return cur.rowcount


def backoff(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Seconds to wait before retrying: exponential, with full jitter.

    The jitter matters. Without it, a hundred jobs that failed together all
    retry together and knock the thing over again.
    """
    return random.uniform(0, min(cap, base * 2 ** attempt))
