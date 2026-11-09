# Ratchet

A durable runtime for AI agents. If a worker dies halfway through a run, the run picks up from the last finished step and nothing that already happened gets done twice.

I built this to learn how systems like this really work: leases, fencing tokens, idempotency, checkpointing, and how to prove the guarantees with failure tests instead of just claiming them. It was built step by step as a pair-programming exercise with Claude, and every commit says so in its co-author line.

![architecture](docs/architecture.png)

## The idea

An agent run is a few steps: ask the model, call a tool, ask again. Workers crash. Without care you either lose the run or repeat a side effect, like sending an email twice.

Ratchet keeps everything that matters in Postgres. Workers take short **leases** on jobs. After every step the result is **checkpointed**. Before every side effect an **idempotency key** goes to the outside system. A **fencing token** stops a slow worker from overwriting the worker that replaced it.

The honest version of the guarantee: **at-least-once delivery + idempotency + fencing gives effectively-once side effects.** True exactly-once over a network isn't possible, and effects on outside systems are only as safe as their support for idempotency keys.

## Does it work? The numbers

Everything below was measured on one laptop (12 logical CPUs) with a local Postgres 17, a fake model, and workers as separate OS processes. These are not production numbers. The raw JSON is in [`chaos/results`](chaos/results) and [`bench/results`](bench/results), and the commands to reproduce them are in [Running the tests, chaos and benchmarks](#running-the-tests-chaos-and-benchmarks).

### Kill workers at random, then check the books

The chaos harness starts 4 real worker processes and kills one at random about every 0.4 seconds (`TerminateProcess` on Windows, the equivalent of `kill -9`), replaces it, and when the queue drains compares what happened against what should have happened. 5 seeds, 100 runs each, 4 rounds per run, so 2,000 emails were expected per mode.

| | idempotency keys ON | idempotency keys OFF |
| --- | --- | --- |
| worker kills | 117 | 181 |
| lost runs | **0** | 0 |
| emails expected | 2,000 | 2,000 |
| **duplicate emails** | **0** | **39** |
| missing emails | 0 | 0 |
| retries | 69 | 114 |

The number of kills differs between modes because kills are timed, not counted. The point is the duplicate row. Without keys, a worker killed after sending an email but before recording it sends it again on retry. With keys, the outside system ignores the repeat.

Zero duplicates in 117 kills is evidence, not a proof. A bigger run and a model checker such as TLA+ would be the next step.

### Claiming jobs: `SKIP LOCKED` against select-then-update

3,000 jobs, 8 threads, median of 3 runs.

| claim method | claims per second | collisions |
| --- | --- | --- |
| `FOR UPDATE SKIP LOCKED` | **4,380** | 0 |
| naive `SELECT` then `UPDATE ... WHERE state = 'queued'` | 301 | 19,458 |

The naive version is the obvious first attempt and it is a worst case: every worker looks at the same first row, one wins, and the rest retry. It is a real effect, but a smarter naive version would narrow the gap.

### Latency and throughput against worker count

Runs arrive at a fixed rate. Each run is 3 rounds with a 20 ms fake model call. One worker handles about 12 runs per second.

Offered load 40 runs per second:

| workers | throughput (runs/s) | p50 latency | p99 latency |
| --- | --- | --- | --- |
| 1 | 11.9 | 7,026 ms | 14,052 ms |
| 2 | 22.8 | 2,467 ms | 4,470 ms |
| 4 | 39.6 | 96 ms | 164 ms |
| 8 | 39.7 | 79 ms | 135 ms |

Throughput scales roughly linearly until it catches the offered load, and then latency collapses from seconds to about 100 ms. To find the ceiling I offered 150 runs per second:

| workers | throughput (runs/s) | p50 latency | p99 latency |
| --- | --- | --- | --- |
| 4 | 43.3 | 7,680 ms | 14,614 ms |
| 8 | 62.9 | 4,422 ms | 8,255 ms |
| 12 | 65.7 | 4,142 ms | 7,617 ms |
| 16 | 32.5 | 8,595 ms | 21,153 ms |

It flattens at about 65 runs per second, and 16 workers is *worse*. I didn't isolate why. The likely cause is that 16 workers, their heartbeat connections, Postgres, and the load generator all share 12 logical CPUs on one machine. I haven't tested that, so treat it as a guess.

### Recovery time against lease length

A worker is killed mid-run and a second worker waits. 4 trials per lease length.

| lease | takeover time (min / median / max) | run finished correctly |
| --- | --- | --- |
| 0.5 s | 0.38 / 0.40 / 0.41 s | yes |
| 1 s | 0.90 / 0.90 / 0.90 s | yes |
| 2 s | 1.85 / 1.90 / 1.90 s | yes |
| 4 s | 3.89 / 3.90 / 3.91 s | yes |

A dead worker's job waits about one lease before someone takes it over. In every trial the resumed run finished all 80 of its steps with none repeated. All four takeovers landed close to the full lease length. My guess is that the kill happens early in the run, before the lease has been renewed. With a heartbeat every third of a lease, a kill later in a run should give a takeover somewhere between two thirds of the lease and the full lease. I haven't measured that case.

The trade-off: a short lease recovers fast but means more heartbeat traffic and a higher chance of expiring a healthy worker during a long pause. A long lease is quiet but slow to recover.

## How a run works

1. A client sends `POST /runs` with an `Idempotency-Key`. The API writes the run and its job in **one transaction**. The same key again returns the original run.
2. A worker **claims** the job with `FOR UPDATE SKIP LOCKED`, which gives it a lease and a new **attempt number**.
3. While the agent runs, a heartbeat thread keeps extending the lease.
4. The agent asks the model (through a **gateway** that rate-limits, retries with jitter, and trips a circuit breaker), then calls a tool.
5. After every step the result is written to the **checkpoint log**, only if the worker's attempt number is still current.
6. If the worker dies, the lease runs out and another worker claims the job with a higher attempt number. It reads the log and **replays** finished steps instead of redoing them.
7. If the old worker wakes up, every write it tries is rejected because its attempt number is stale. That is the **fencing token**.

## Design decisions

**Why Postgres and not a message broker?** One system holds the queue, the run state and the checkpoints, so claiming a job and recording progress can be transactional. The cost is that Postgres write throughput becomes the ceiling. At around 10x I'd add PgBouncer, batch claims and a read replica for status reads. At around 100x I'd partition by tenant, or move the queue to Kafka or SQS and keep Postgres for run state.

**Why `SKIP LOCKED`?** It lets many workers pull from one table without blocking or colliding. The benchmark above shows what happens without it.

**Why a lease and not a lock?** A lock held by a dead worker is held forever. A lease expires by itself, so recovery needs no failure detector and no cleanup process. The claim query treats an expired lease as available.

**Why a fencing token?** A lease alone isn't enough. A worker can be slow, not dead: its lease expires, another worker takes over, and then it wakes up and keeps going. The attempt number makes the stale worker's writes fail instead of corrupting the run. See Kleppmann's article below.

**Why store the model's answers?** Models aren't deterministic. If a resumed run asked again it might get a different answer and wander down a different path. Replaying the stored answer keeps the run consistent, the same idea as event-history replay in durable execution systems.

**Why idempotency keys, and what is the limit?** The worker can die between doing a side effect and writing it down, and then it can't tell on retry whether the effect happened. Sending the outside system a key makes the repeat harmless, but only if that system honours keys. For one that doesn't, there is a small window that can't be closed from our side.

**Why backoff with jitter?** Without jitter, jobs that failed together retry together and knock the thing over again.

### What I rejected or left out

- **Exactly-once delivery.** Not achievable over an unreliable network. The design aims for effectively-once effects instead.
- **A separate lease reaper process.** The claim query reclaims expired leases, so one isn't needed.
- **An outbox table.** I drew one in the first diagram, then cut it: with no relay process reading it, it did nothing.
- **Exactly-once for the model call itself.** A crash mid-call can cost a repeated (billed) model call. Only effects on the outside world are protected.

## Quick start

You need Python 3.11+ and a local Postgres install.

```bash
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"      # macOS/Linux: .venv/bin/pip
python scripts/dev_db.py start             # a throwaway Postgres on port 55432

.venv/Scripts/python -m uvicorn ratchet.api:app --port 8000 &
.venv/Scripts/python -m ratchet.cli --id w1 &
```

Submit a run, twice with the same key:

```bash
curl -i -X POST localhost:8000/runs -H "Idempotency-Key: order-42" \
     -H "Content-Type: application/json" -d '{"input": {"rounds": 3, "task": "demo"}}'
# HTTP/1.1 202 Accepted   {"run_id":"...","created":true}

curl -i -X POST localhost:8000/runs -H "Idempotency-Key: order-42" \
     -H "Content-Type: application/json" -d '{"input": {"rounds": 3, "task": "demo"}}'
# HTTP/1.1 200 OK         {"run_id":"<same id>","created":false}

curl localhost:8000/runs/<run_id>
# {"status":"done","steps_finished":6,"attempts":1, ...}
```

`dev_db.py` creates a cluster in `.pgdata/`, so it never touches another database you have.

## Running the tests, chaos and benchmarks

```bash
.venv/Scripts/python -m pytest -q                                   # 39 tests
.venv/Scripts/python chaos/run_chaos.py --compare --seed 11 --runs 100 --kill-every 0.4
.venv/Scripts/python bench/run_bench.py claim
.venv/Scripts/python bench/run_bench.py throughput                  # --rate 150 --workers 4 8 12 16
.venv/Scripts/python bench/run_bench.py recovery
```

The chaos and benchmark scripts use the `ratchet_test` database and truncate it. The tests include one for each guarantee: no double claim under 8 threads, a stale worker is fenced off, a replayed run reuses the model's first answer, and a real kill of real worker processes loses and repeats nothing. I also checked the important tests can fail by breaking the code they cover (the fencing token, the heartbeat, the checkpoint replay) and confirming they went red.

## Layout

```text
src/ratchet/
  queue.py      enqueue, claim with a lease, heartbeat, complete, fail, fenced step writes
  worker.py     the worker loop and the heartbeat thread
  agent.py      the agent loop: ask, act, checkpoint, replay
  tools.py      tool calls with idempotency keys, and a simulated outside system
  llm.py        fake model, token bucket, retries with jitter, circuit breaker
  api.py        FastAPI service
  db.py, config.py, cli.py
sql/            numbered migrations
tests/          39 tests
chaos/          the kill-workers harness and its raw results
bench/          the benchmarks and their raw results
scripts/        dev_db.py, a throwaway local Postgres
docs/           the architecture diagram (PNG and editable Excalidraw)
```

## Limitations

- One machine, a fake model and a simulated outside system. Real model latency and failure behaviour will differ.
- The outside system is a table in the same Postgres. In real life an outside system adds network failures between the effect and the acknowledgement.
- Zero duplicates in 117 kills is evidence, not a formal proof.
- No authentication, no multi-tenancy, no metrics endpoint yet.
- The benchmarks measure this design on this machine. They don't compare Ratchet with other runtimes.

## References

- Martin Kleppmann, [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html): why a lock needs a fencing token.
- PostgreSQL documentation, [SELECT, the locking clause](https://www.postgresql.org/docs/current/sql-select.html) (`FOR UPDATE SKIP LOCKED`).
- Marc Brooker, [Exponential Backoff And Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/), AWS Architecture Blog.
- Stripe, [Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency).
- Tyler Treat, [You Cannot Have Exactly-Once Delivery](https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/).
- Wikipedia, [Two Generals' Problem](https://en.wikipedia.org/wiki/Two_Generals%27_Problem).
- Temporal documentation, [Workflow Execution](https://docs.temporal.io/workflow-execution): durable execution by replaying an event history.
- Martin Fowler, [Circuit Breaker](https://martinfowler.com/bliki/CircuitBreaker.html).
- Martin Kleppmann, [Designing Data-Intensive Applications](https://dataintensive.net/), O'Reilly, 2017.

## License

[MIT](LICENSE). You can use, copy, modify and share this code, including commercially, as long as the license notice stays with it.
