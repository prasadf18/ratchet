-- Ratchet schema, version 1.
--
-- Postgres is the only place state lives. Workers are disposable: kill one at
-- any moment and another can pick the run up from these tables.

-- A run is one agent task from start to finish.
CREATE TABLE runs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Set by the caller (Idempotency-Key header). Submitting the same key twice
    -- returns the first run instead of creating a second one.
    request_key     text UNIQUE,
    input           jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'done', 'failed')),
    result          jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- The work queue. One job per run.
--
-- A worker claims a job by taking a lease: lease_until in the future and a new
-- attempt number. If the worker dies, lease_until passes and the job can be
-- claimed again. `attempt` doubles as the fencing token: every write a worker
-- makes is checked against it, so a slow worker whose lease expired can't
-- overwrite what the new worker did.
CREATE TABLE jobs (
    id              bigserial PRIMARY KEY,
    run_id          uuid NOT NULL UNIQUE REFERENCES runs (id),
    state           text NOT NULL DEFAULT 'queued'
                    CHECK (state IN ('queued', 'leased', 'done', 'dead')),
    available_at    timestamptz NOT NULL DEFAULT now(),
    lease_owner     text,
    lease_until     timestamptz,
    attempt         integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL DEFAULT 5,
    last_error      text
);

-- Workers only ever look at jobs that could be claimed, so index just those.
CREATE INDEX jobs_claimable ON jobs (available_at)
    WHERE state IN ('queued', 'leased');

-- The checkpoint log. One row per finished step, append-only.
--
-- The output of model calls is stored here, because models aren't
-- deterministic. When a run resumes, finished steps are replayed from this
-- table instead of being asked again.
CREATE TABLE steps (
    run_id          uuid NOT NULL REFERENCES runs (id),
    seq             integer NOT NULL,
    kind            text NOT NULL CHECK (kind IN ('llm', 'tool')),
    input           jsonb,
    output          jsonb,
    attempt         integer NOT NULL,   -- which attempt wrote this step
    created_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);

-- One row per side effect that has happened. Before doing a tool call the
-- worker inserts its key; if the key already exists, the effect already
-- happened and the stored result is returned instead.
CREATE TABLE idempotency_keys (
    key             text PRIMARY KEY,
    run_id          uuid NOT NULL REFERENCES runs (id),
    result          jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Runs that ran out of attempts. Kept so someone can look at why.
CREATE TABLE dead_letters (
    run_id          uuid PRIMARY KEY REFERENCES runs (id),
    attempts        integer NOT NULL,
    reason          text,
    created_at      timestamptz NOT NULL DEFAULT now()
);
