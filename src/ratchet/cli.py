"""Run a worker process.

    python -m ratchet.cli --id w1 --lease 2 --llm-latency 0.02

This is what the chaos harness starts, and then kills, so it has to be a real
separate process.
"""
import argparse
import logging
import os

from ratchet import config, worker
from ratchet.agent import Agent
from ratchet.llm import FakeLLM, LLMGateway
from ratchet.tools import ToolExecutor


def main() -> None:
    p = argparse.ArgumentParser(description="Run a Ratchet worker")
    p.add_argument("--dsn", default=config.dsn())
    p.add_argument("--id", default=f"worker-{os.getpid()}")
    p.add_argument("--lease", type=float, default=10.0, help="lease length in seconds")
    p.add_argument("--poll", type=float, default=0.05)
    p.add_argument("--llm-latency", type=float, default=0.0)
    p.add_argument("--llm-failure-rate", type=float, default=0.0)
    p.add_argument("--effect-delay", type=float, default=0.0, help="pause between a tool's effect and recording it")
    p.add_argument("--no-keys", action="store_true", help="run tools without idempotency keys (a naive runtime)")
    p.add_argument("--exit-when-idle", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(message)s")
    gateway = LLMGateway(FakeLLM(latency=args.llm_latency, failure_rate=args.llm_failure_rate))
    tools = ToolExecutor(use_keys=not args.no_keys, effect_delay=args.effect_delay)
    worker.run(args.dsn, args.id, Agent(gateway, tools, worker_id=args.id), lease=args.lease,
               poll=args.poll, exit_when_idle=args.exit_when_idle)


if __name__ == "__main__":
    main()
