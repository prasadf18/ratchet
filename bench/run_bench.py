"""Benchmarks. Every number in the README comes from running this on one laptop with a local Postgres.

    python bench/run_bench.py throughput    # latency and throughput vs number of workers
    python bench/run_bench.py claim         # SKIP LOCKED vs a naive select-then-update claim
    python bench/run_bench.py recovery      # time for a killed worker's job to be taken over, vs lease length

They use the TEST database and truncate it. Results are written to bench/results/*.json.
"""
import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

from ratchet import config, db, queue

RESULTS = Path(__file__).resolve().parent / "results"
TRUNCATE = "TRUNCATE external_effects, dead_letters, idempotency_keys, steps, jobs, runs RESTART IDENTITY CASCADE"


def percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return ordered[index]


def spawn(dsn: str, name: str, lease: float, llm_latency: float = 0.02):
    cmd = [sys.executable, "-m", "ratchet.cli", "--dsn", dsn, "--id", name, "--lease", str(lease),
           "--poll", "0.01", "--llm-latency", str(llm_latency)]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def save(name: str, data) -> None:
    RESULTS.mkdir(exist_ok=True)
    (RESULTS / f"{name}.json").write_text(json.dumps(data, indent=2))
    print(f"saved bench/results/{name}.json")


# ------------------------------------------------------------------ latency and throughput vs workers
def bench_throughput(dsn: str, rate: float = 40.0, seconds: float = 6.0, rounds: int = 3,
                     worker_counts: tuple[int, ...] = (1, 2, 4, 8)) -> list[dict]:
    """Arrivals at a fixed rate (an open loop). More workers should cut the queueing delay."""
    out = []
    for workers in worker_counts:
        conn = db.connect(dsn)
        conn.execute(TRUNCATE)
        procs = [spawn(dsn, f"w{i}", lease=5) for i in range(workers)]
        time.sleep(1.5)                                   # let the processes import and connect

        total = int(rate * seconds)
        begin = time.monotonic()
        for n in range(total):
            queue.enqueue(conn, {"rounds": rounds, "task": f"t{n}"})
            wait = begin + (n + 1) / rate - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        while conn.execute("SELECT count(*) AS n FROM jobs WHERE state IN ('queued','leased')").fetchone()["n"]:
            time.sleep(0.1)
            if time.monotonic() - begin > 120:
                break
        for proc in procs:
            proc.kill()
            proc.wait()

        lat = [float(r["s"]) for r in conn.execute(
            "SELECT extract(epoch FROM updated_at - created_at) AS s FROM runs WHERE status = 'done'")]
        span = float(conn.execute(
            "SELECT extract(epoch FROM max(updated_at) - min(created_at)) AS s FROM runs WHERE status = 'done'"
        ).fetchone()["s"])
        row = {"workers": workers, "offered_runs_per_sec": rate, "runs": total, "done": len(lat),
               "throughput_runs_per_sec": round(len(lat) / span, 1),
               "latency_p50_ms": round(percentile(lat, 50) * 1000), "latency_p99_ms": round(percentile(lat, 99) * 1000)}
        out.append(row)
        print(row)
        conn.close()
    save(f"throughput-rate{int(rate)}", out)
    return out


# ------------------------------------------------------------------ SKIP LOCKED vs naive claim
NAIVE_PICK = "SELECT id FROM jobs WHERE state = 'queued' AND available_at <= now() ORDER BY available_at, id LIMIT 1"
NAIVE_TAKE = ("UPDATE jobs SET state = 'leased', lease_owner = %s, lease_until = now() + interval '60 seconds', "
              "attempt = attempt + 1 WHERE id = %s AND state = 'queued'")


def _claim_run(dsn: str, mode: str, jobs: int, threads: int) -> dict:
    conn = db.connect(dsn)
    conn.execute(TRUNCATE)
    conn.execute("INSERT INTO runs (input) SELECT '{}'::jsonb FROM generate_series(1, %s)", (jobs,))
    conn.execute("INSERT INTO jobs (run_id) SELECT id FROM runs")
    counts = {"claims": 0, "conflicts": 0}
    lock = threading.Lock()

    def worker(name: str) -> None:
        own = db.connect(dsn)
        claims = conflicts = 0
        while True:
            if mode == "skip_locked":
                row = own.execute(queue._CLAIM, {"worker": name, "lease": 60}).fetchone()
                if row is None:
                    break
                claims += 1
            else:
                picked = own.execute(NAIVE_PICK).fetchone()
                if picked is None:
                    break
                if own.execute(NAIVE_TAKE, (name, picked["id"])).rowcount == 1:
                    claims += 1
                else:
                    conflicts += 1          # someone else got there between our SELECT and UPDATE
        own.close()
        with lock:
            counts["claims"] += claims
            counts["conflicts"] += conflicts

    pool = [threading.Thread(target=worker, args=(f"t{i}",)) for i in range(threads)]
    start = time.perf_counter()
    for t in pool:
        t.start()
    for t in pool:
        t.join()
    wall = time.perf_counter() - start
    conn.close()
    return {"mode": mode, "threads": threads, "jobs": jobs, "claims": counts["claims"],
            "conflicts": counts["conflicts"], "seconds": round(wall, 2), "claims_per_sec": round(counts["claims"] / wall)}


def bench_claim(dsn: str, jobs: int = 3000, threads: int = 8, repeats: int = 3) -> list[dict]:
    out = []
    for mode in ("skip_locked", "naive"):
        runs = [_claim_run(dsn, mode, jobs, threads) for _ in range(repeats)]
        median = sorted(runs, key=lambda r: r["claims_per_sec"])[len(runs) // 2]
        median["repeats"] = repeats
        out.append(median)
        print(median)
    save("claim", out)
    return out


# ------------------------------------------------------------------ recovery time vs lease length
def _recovery_trial(dsn: str, lease: float) -> dict:
    conn = db.connect(dsn)
    conn.execute(TRUNCATE)
    queue.enqueue(conn, {"rounds": 40, "task": "long"})          # long enough to kill it mid-run
    victim = spawn(dsn, "victim", lease)
    while not conn.execute("SELECT 1 FROM jobs WHERE lease_owner = 'victim'").fetchone():
        time.sleep(0.02)
    survivor = spawn(dsn, "survivor", lease)                      # idle: the lease is held, so it can't claim
    while conn.execute("SELECT count(*) AS n FROM steps").fetchone()["n"] < 6:
        time.sleep(0.02)

    victim.kill()
    victim.wait()
    killed_at = time.monotonic()
    while not conn.execute("SELECT 1 FROM jobs WHERE attempt >= 2").fetchone():
        time.sleep(0.01)
    taken_over = time.monotonic() - killed_at
    while conn.execute("SELECT status FROM runs").fetchone()["status"] != "done":
        time.sleep(0.05)
    finished = time.monotonic() - killed_at
    survivor.kill()
    survivor.wait()
    steps = conn.execute("SELECT count(*) AS n FROM steps").fetchone()["n"]
    conn.close()
    return {"takeover_s": round(taken_over, 2), "finish_s": round(finished, 2), "steps": steps}


def bench_recovery(dsn: str, trials: int = 4) -> list[dict]:
    out = []
    for lease in (0.5, 1.0, 2.0, 4.0):
        results = [_recovery_trial(dsn, lease) for _ in range(trials)]
        takeovers = [r["takeover_s"] for r in results]
        row = {"lease_s": lease, "trials": trials, "takeover_min_s": min(takeovers),
               "takeover_median_s": statistics.median(takeovers), "takeover_max_s": max(takeovers),
               "all_runs_complete_with_80_steps": all(r["steps"] == 80 for r in results)}
        out.append(row)
        print(row)
    save("recovery", out)
    return out


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("which", choices=["throughput", "claim", "recovery"])
    p.add_argument("--dsn", default=config.test_dsn())
    p.add_argument("--rate", type=float, default=40.0, help="throughput: offered runs per second")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8], help="throughput: worker counts to try")
    args = p.parse_args()
    if args.which == "throughput":
        bench_throughput(args.dsn, rate=args.rate, worker_counts=tuple(args.workers))
    elif args.which == "claim":
        bench_claim(args.dsn)
    else:
        bench_recovery(args.dsn)
