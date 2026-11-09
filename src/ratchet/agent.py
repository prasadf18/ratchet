"""The agent loop: ask the model, call a tool, repeat.

Every finished step is checkpointed in the steps table. When a run is retried,
or picked up by a different worker after a crash, steps that already finished
are replayed from the log instead of being done again. That includes the
model's answers: a model isn't deterministic, so asking again could give a
different answer and send the run down a different path.

Step numbering: round i uses step 2i for the model call and step 2i+1 for the
tool call, so a step's position says what it is.
"""
import threading

from ratchet import queue
from ratchet.llm import LLMGateway
from ratchet.queue import Claim, LeaseLost
from ratchet.tools import ToolExecutor


class Agent:
    def __init__(self, gateway: LLMGateway, tools: ToolExecutor, worker_id: str = ""):
        self.gateway, self.tools, self.worker_id = gateway, tools, worker_id

    def __call__(self, conn, claim: Claim, lost: threading.Event) -> dict:
        spec = conn.execute("SELECT input FROM runs WHERE id = %s", (claim.run_id,)).fetchone()["input"]
        rounds, task = spec.get("rounds", 3), spec.get("task", "")
        finished = {row["seq"]: row for row in queue.load_steps(conn, claim.run_id)}
        text = ""

        for i in range(rounds):
            if lost.is_set():
                raise LeaseLost(f"job {claim.job_id} attempt {claim.attempt}")
            ask_seq, act_seq = 2 * i, 2 * i + 1

            if ask_seq in finished:
                text = finished[ask_seq]["output"]["text"]           # replay: don't ask the model again
            else:
                prompt = f"round {i}: {task}"
                text = self.gateway.complete(prompt)
                queue.record_step(conn, claim, ask_seq, "llm", {"prompt": prompt}, {"text": text})

            if act_seq not in finished:
                args = {"to": text.split()[-1]}
                outcome = self.tools.send_email(conn, claim, act_seq, args, worker=self.worker_id)
                queue.record_step(conn, claim, act_seq, "tool", args, outcome)

        return {"rounds": rounds, "last": text}
