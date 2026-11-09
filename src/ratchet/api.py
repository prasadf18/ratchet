"""The HTTP front door. It is stateless: everything lives in Postgres, so you can run as many as you like.

    uvicorn ratchet.api:app --port 8000

    curl -X POST localhost:8000/runs -H "Idempotency-Key: order-42" \\
         -H "Content-Type: application/json" -d '{"input": {"rounds": 3, "task": "demo"}}'
    curl localhost:8000/runs/<run_id>
"""
from typing import Iterator
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from ratchet import config, db, queue


class RunRequest(BaseModel):
    input: dict
    max_attempts: int = Field(default=5, ge=1, le=100)


def create_app(dsn: str | None = None) -> FastAPI:
    app = FastAPI(title="Ratchet", description="A durable runtime for AI agents")

    def connection() -> Iterator:
        conn = db.connect(dsn or config.dsn())
        try:
            yield conn
        finally:
            conn.close()

    @app.get("/health")
    def health(conn=Depends(connection)) -> dict:
        conn.execute("SELECT 1")
        return {"ok": True}

    @app.post("/runs", status_code=202)
    def submit(body: RunRequest, response: Response, idempotency_key: str | None = Header(default=None),
               conn=Depends(connection)) -> dict:
        """Queue a run. Sending the same Idempotency-Key again returns the first run, not a second one."""
        run_id, created = queue.enqueue(conn, body.input, request_key=idempotency_key, max_attempts=body.max_attempts)
        if not created:
            response.status_code = 200
        return {"run_id": str(run_id), "created": created}

    @app.get("/runs/{run_id}")
    def get_run(run_id: UUID, conn=Depends(connection)) -> dict:
        run = conn.execute("SELECT id, status, result, created_at, updated_at FROM runs WHERE id = %s",
                           (run_id,)).fetchone()
        if run is None:
            raise HTTPException(status_code=404, detail="no such run")
        job = conn.execute("SELECT attempt, last_error FROM jobs WHERE run_id = %s", (run_id,)).fetchone()
        steps = conn.execute("SELECT count(*) AS n FROM steps WHERE run_id = %s", (run_id,)).fetchone()["n"]
        return {
            "id": str(run["id"]),
            "status": run["status"],
            "result": run["result"],
            "steps_finished": steps,
            "attempts": job["attempt"] if job else 0,
            "last_error": job["last_error"] if job else None,
        }

    return app


app = create_app()
