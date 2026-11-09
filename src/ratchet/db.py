from pathlib import Path

import psycopg
from psycopg.rows import dict_row

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"


def connect(dsn: str) -> psycopg.Connection:
    """Open a connection that returns rows as dicts."""
    return psycopg.connect(dsn, row_factory=dict_row)


def migrate(conn: psycopg.Connection) -> list[str]:
    """Apply any sql/NNN_*.sql files that haven't been applied yet.

    Returns the names of the files it applied. Kept deliberately simple: files
    run in name order and are recorded in a small table.
    """
    with conn.transaction():
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
        )
        done = {row["name"] for row in conn.execute("SELECT name FROM schema_migrations")}
        applied = []
        for path in sorted(SQL_DIR.glob("*.sql")):
            if path.name in done:
                continue
            conn.execute(path.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations (name) VALUES (%s)", (path.name,))
            applied.append(path.name)
    return applied
