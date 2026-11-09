"""Tools: the side effects an agent can cause.

The hard part is a crash between "the effect happened" and "we wrote it down".
On retry the worker can't tell whether the email went out. The answer is to
send the outside system an idempotency key, so a repeat is ignored on its end.
That is why the guarantee is only as strong as the outside system's support
for idempotency keys.
"""
import time
from dataclasses import dataclass

from psycopg.types.json import Jsonb

from ratchet import queue
from ratchet.queue import Claim


def external_send(conn, run_id, seq: int, worker: str, dedupe_key: str | None = None) -> bool:
    """Do the side effect on the simulated outside system. True if it was a new effect.

    With a dedupe_key, a repeat is ignored. Without one, every call counts.
    """
    cur = conn.execute(
        "INSERT INTO external_effects (dedupe_key, run_id, seq, worker) VALUES (%s, %s, %s, %s) "
        "ON CONFLICT (dedupe_key) DO NOTHING",
        (dedupe_key, run_id, seq, worker),
    )
    return cur.rowcount == 1


@dataclass
class ToolExecutor:
    # With use_keys=False the executor behaves like a naive agent runtime. It exists
    # so the chaos test can show the duplicates that idempotency keys prevent.
    use_keys: bool = True
    # Pause after the effect and before recording it. It widens the crash window
    # on purpose so the chaos harness has something to hit.
    effect_delay: float = 0.0

    def send_email(self, conn, claim: Claim, seq: int, args: dict, worker: str = "") -> dict:
        key = f"{claim.run_id}:{seq}"

        if self.use_keys:
            seen = conn.execute("SELECT result FROM idempotency_keys WHERE key = %s", (key,)).fetchone()
            if seen:
                return seen["result"]

        # Don't start a side effect if we already know we are a stale worker.
        queue.assert_lease(conn, claim)

        external_send(conn, claim.run_id, seq, worker, dedupe_key=key if self.use_keys else None)
        if self.effect_delay:
            time.sleep(self.effect_delay)

        result = {"to": args["to"], "delivered": True}
        if self.use_keys:
            conn.execute(
                "INSERT INTO idempotency_keys (key, run_id, result) VALUES (%s, %s, %s) ON CONFLICT (key) DO NOTHING",
                (key, claim.run_id, Jsonb(result)),
            )
        return result
