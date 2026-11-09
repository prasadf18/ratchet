import sys
from pathlib import Path

# chaos/ is a scripts folder, not a package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "chaos"))

from run_chaos import run_chaos  # noqa: E402


def test_killing_workers_loses_nothing_and_repeats_nothing(dsn):
    """A short version of the full chaos run. Real processes, real kills."""
    result = run_chaos(dsn, runs=20, rounds=3, workers=3, kill_every=0.4, lease=1.0, use_keys=True, seed=7, timeout=60)

    assert result["worker_kills"] >= 1, "the test is meaningless if nothing was killed"
    assert result["lost_runs"] == 0
    assert result["duplicate_effects"] == 0
    assert result["missing_effects"] == 0
