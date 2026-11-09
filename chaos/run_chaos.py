"""Chaos test: run real worker processes, kill them at random, then check nothing was lost or repeated.

    python chaos/run_chaos.py --runs 60 --workers 4 --kill-every 0.8 --compare

Each worker is a separate OS process. Killing one uses Popen.kill(), which is
SIGKILL on Linux/macOS and TerminateProcess on Windows: the process gets no
chance to clean up, which is the point.

After the queue drains it checks, against the simulated outside system:
  lost      runs that never reached 'done'
  duplicate side effects that happened more than once
  missing   side effects that never happened
"""
import argparse
import json
import random
import subprocess
import sys
import time
from pathlib import Path

from ratchet import config, db, queue

RESULTS = Path(__file__).resolve().parent / "results"


def spawn(dsn: str, name: str, lease: float, use_keys: bool, llm_latency: float, effect_delay: float):
    cmd = [sys.executable, "-m", "ratchet.cli", "--dsn", dsn, "--id", name, "--lease", str(lease),
           "--poll", "0.02", "--llm-latency", str(llm_latency), "--effect-delay", str(effect_delay)]
    if not use_keys:
        cmd.append("--no-keys")
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def run_chaos(dsn: str, runs: int = 60, rounds: int = 4, workers: int = 4, kill_every: float = 0.8,
              lease: float = 1.0, use_keys: bool = True, seed: int = 1, timeout: float = 120.0,
              llm_latency: float = 0.02, effect_delay: float = 0.02) -> dict:
    rng = random.Random(seed)
    conn = db.connect(dsn)
    db.migrate(conn)
    conn.execute("TRUNCATE external_effects, dead_letters, idempotency_keys, steps, jobs, runs RESTART IDENTITY CASCADE")
    for n in range(runs):
        # max_attempts is generous: every kill costs an attempt and we are testing recovery, not retry limits.
        queue.enqueue(conn, {"rounds": rounds, "task": f"task-{n}"}, request_key=f"chaos-{seed}-{n}", max_attempts=200)

    live = {i: spawn(dsn, f"w{i}", lease, use_keys, llm_latency, effect_delay) for i in range(workers)}
    next_id, kills = workers, 0
    started = time.time()
    next_kill = started + rng.expovariate(1 / kill_every)

    while True:
        time.sleep(0.05)
        now = time.time()
        if now >= next_kill and live:
            victim = rng.choice(list(live))
            live[victim].kill()
            live[victim].wait()
            del live[victim]
            kills += 1
            live[next_id] = spawn(dsn, f"w{next_id}", lease, use_keys, llm_latency, effect_delay)
            next_id += 1
            next_kill = now + rng.expovariate(1 / kill_every)
        left = conn.execute("SELECT count(*) AS n FROM jobs WHERE state IN ('queued', 'leased')").fetchone()["n"]
        if left == 0 or now - started > timeout:
            break

    elapsed = time.time() - started
    for proc in live.values():
        proc.kill()
        proc.wait()

    one = lambda sql: conn.execute(sql).fetchone()["n"]
    expected = runs * rounds
    effects_total = one("SELECT count(*) AS n FROM external_effects")
    effects_distinct = one("SELECT count(*) AS n FROM (SELECT DISTINCT run_id, seq FROM external_effects) d")
    result = {
        "idempotency_keys": use_keys,
        "seed": seed,
        "runs": runs,
        "rounds_per_run": rounds,
        "workers": workers,
        "worker_kills": kills,
        "seconds": round(elapsed, 2),
        "runs_done": one("SELECT count(*) AS n FROM runs WHERE status = 'done'"),
        "lost_runs": runs - one("SELECT count(*) AS n FROM runs WHERE status = 'done'"),
        "effects_expected": expected,
        "effects_total": effects_total,
        "duplicate_effects": effects_total - effects_distinct,
        "missing_effects": expected - effects_distinct,
        "retries": one("SELECT coalesce(sum(attempt), 0)::int AS n FROM jobs") - runs,
    }
    conn.close()
    return result


def show(results: list[dict]) -> None:
    rows = [("idempotency keys", "idempotency_keys"), ("worker kills", "worker_kills"), ("runs done", "runs_done"),
            ("lost runs", "lost_runs"), ("effects expected", "effects_expected"), ("effects total", "effects_total"),
            ("DUPLICATE effects", "duplicate_effects"), ("missing effects", "missing_effects"),
            ("retries (extra attempts)", "retries"), ("seconds", "seconds")]
    width = max(len(label) for label, _ in rows)
    print()
    print(f"{'':{width}}  " + "  ".join(f"{'keys ON' if r['idempotency_keys'] else 'keys OFF':>9}" for r in results))
    for label, key in rows:
        print(f"{label:{width}}  " + "  ".join(f"{str(r[key]):>9}" for r in results))
    print()


def main() -> None:
    p = argparse.ArgumentParser(description="Kill workers at random and check the guarantees")
    p.add_argument("--dsn", default=config.test_dsn(), help="uses the TEST database by default; it gets truncated")
    p.add_argument("--runs", type=int, default=60)
    p.add_argument("--rounds", type=int, default=4)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--kill-every", type=float, default=0.8, help="mean seconds between kills")
    p.add_argument("--lease", type=float, default=1.0)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--no-keys", action="store_true", help="run once without idempotency keys")
    p.add_argument("--compare", action="store_true", help="run with keys on, then off")
    args = p.parse_args()

    modes = [True, False] if args.compare else [not args.no_keys]
    results = [run_chaos(args.dsn, args.runs, args.rounds, args.workers, args.kill_every, args.lease, k, args.seed)
               for k in modes]
    show(results)
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / f"chaos-seed{args.seed}.json"
    out.write_text(json.dumps(results, indent=2))
    print(f"saved {out}")


if __name__ == "__main__":
    main()
