"""rrp-gov (rebuild b252, 2026-10-29): one Cloud Run job for the agency data, so dataset gov has one writer.

Runs the feeds ingest and then the market ingest, each in its own process and folder with RRP_SUBJECT=gov, so each keeps
its own sources, timing rules and run log (ops.feeds_runs / ops.market_runs). Extra arguments pass through to both
(e.g. --equities forces the Treasury / EIA / NOAA daily pass). A failure in one part never stops the other; the job
exits 0, like the two ingests it wraps, and failures show in the run logs and on the health board."""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    env = dict(os.environ, RRP_SUBJECT="gov")
    for part in ("feeds", "market"):
        print(f"--- rrp-gov: {part}", flush=True)
        r = subprocess.run([sys.executable, "main.py", *sys.argv[1:]], cwd=os.path.join(HERE, part), env=env)
        if r.returncode:
            print(f"rrp-gov: {part} exited {r.returncode}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
