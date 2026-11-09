-- A stand-in for the outside world: where tool side effects land.
--
-- In real life this is an email provider, a payments API and so on. It has no
-- foreign keys into our tables on purpose, because we don't control it.
--
-- dedupe_key models an API that accepts idempotency keys (Stripe-style). When
-- a caller sends one, a repeat is ignored. When it doesn't, every call is a new
-- row, which is how duplicates show up in the chaos tests.
CREATE TABLE external_effects (
    id          bigserial PRIMARY KEY,
    dedupe_key  text UNIQUE,
    run_id      uuid NOT NULL,
    seq         integer NOT NULL,
    worker      text,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX external_effects_run ON external_effects (run_id, seq);
