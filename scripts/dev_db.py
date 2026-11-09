"""Start and stop a throwaway local Postgres for development.

It uses the Postgres binaries you already have installed and keeps all data in
.pgdata/ inside this repo, so it never touches another database on the machine.

    python scripts/dev_db.py start
    python scripts/dev_db.py status
    python scripts/dev_db.py stop
"""
import os
import subprocess
import sys
from pathlib import Path

PORT = "55432"
USER = "ratchet"
DATABASES = ["ratchet", "ratchet_test"]
ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / ".pgdata"


def pg_bin() -> Path:
    """Find the Postgres bin directory. Set PG_BIN to override."""
    if os.environ.get("PG_BIN"):
        return Path(os.environ["PG_BIN"])
    for candidate in sorted(Path("C:/Program Files/PostgreSQL").glob("*/bin"), reverse=True):
        return candidate
    for candidate in ("/usr/lib/postgresql/17/bin", "/usr/local/bin", "/opt/homebrew/bin"):
        if (Path(candidate) / "initdb").exists():
            return Path(candidate)
    sys.exit("Could not find Postgres. Set PG_BIN to its bin directory.")


def run(tool: str, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    exe = str(pg_bin() / tool)
    return subprocess.run([exe, *args], check=check, capture_output=True, text=True)


def is_running() -> bool:
    return run("pg_ctl", "-D", str(DATA), "status", check=False).returncode == 0


def start() -> None:
    if not DATA.exists():
        print("creating a new cluster in .pgdata/")
        run("initdb", "-D", str(DATA), "-U", USER, "-A", "trust", "-E", "UTF8")
    if is_running():
        print("already running")
    else:
        run("pg_ctl", "-D", str(DATA), "-o", f"-p {PORT}", "-l", str(DATA / "server.log"), "-w", "start")
        print(f"started on port {PORT}")
    existing = run("psql", "-h", "localhost", "-p", PORT, "-U", USER, "-d", "postgres", "-tAc",
                   "SELECT datname FROM pg_database").stdout.split()
    for name in DATABASES:
        if name not in existing:
            run("createdb", "-h", "localhost", "-p", PORT, "-U", USER, name)
            print(f"created database {name}")


def stop() -> None:
    if is_running():
        run("pg_ctl", "-D", str(DATA), "-m", "fast", "-w", "stop")
        print("stopped")
    else:
        print("not running")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "status"
    if command == "start":
        start()
    elif command == "stop":
        stop()
    elif command == "status":
        print("running" if is_running() else "stopped")
    else:
        sys.exit(__doc__)
