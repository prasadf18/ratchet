import pytest

from ratchet import config, db


@pytest.fixture(scope="session")
def dsn():
    """The test database, with the schema applied. Start it with scripts/dev_db.py."""
    url = config.test_dsn()
    conn = db.connect(url)
    db.migrate(conn)
    conn.close()
    return url


@pytest.fixture
def conn(dsn):
    """A connection to an empty database."""
    c = db.connect(dsn)
    c.execute("TRUNCATE external_effects, dead_letters, idempotency_keys, steps, jobs, runs RESTART IDENTITY CASCADE")
    yield c
    c.close()
