import os

# Dev database started by scripts/dev_db.py. Override with RATCHET_DSN.
DEFAULT_DSN = "postgresql://ratchet@localhost:55432/ratchet"
DEFAULT_TEST_DSN = "postgresql://ratchet@localhost:55432/ratchet_test"


def dsn() -> str:
    return os.environ.get("RATCHET_DSN", DEFAULT_DSN)


def test_dsn() -> str:
    return os.environ.get("RATCHET_TEST_DSN", DEFAULT_TEST_DSN)
