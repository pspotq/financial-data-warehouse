"""Hard cost cap on every BigQuery query this process runs (b247, added 2026-10-28).

Importing this module patches google.cloud.bigquery.Client so each client's default query config carries
maximum_bytes_billed: a query that would bill more fails instead of running. 5 GB for normal runs (the jobs' largest
query in the week before was 3.7 GB, the insider_filings_snap rebuild); 50 GB when the command line asks for a
backfill or history load. RRP_MAX_GB overrides both. Load jobs (appends) are not queries and are not capped.
Kept identical in cloud/feeds and cloud/market (each job's image only holds its own folder).
Every query is also labelled rrp_job=<Cloud Run job name> (2026-10-28), so spend can be split per job in
INFORMATION_SCHEMA.JOBS (the cockpit's cost-per-pipe view).
"""
import os, sys

NORMAL_GB, BACKFILL_GB = 5, 50
BACKFILL_HINTS = ("history", "backfill", "-range", "--deep", "--days", "--since", "--edgar-index-days")


def cap_gb(argv=None):
    if os.environ.get("RRP_MAX_GB"): return float(os.environ["RRP_MAX_GB"])
    args = " ".join((argv if argv is not None else sys.argv)[1:] + os.environ.get("RRP_EXTRA_ARGS", "").split())
    return BACKFILL_GB if any(h in args for h in BACKFILL_HINTS) else NORMAL_GB


def job_label():
    import re
    name = os.environ.get("CLOUD_RUN_JOB") or "local"
    return re.sub(r"[^a-z0-9_-]", "_", name.lower())[:63]


def install():
    from google.cloud import bigquery
    if getattr(bigquery.Client, "_rrp_capped", False): return
    original = bigquery.Client.__init__
    limit = int(cap_gb() * 1e9)

    def __init__(self, *args, **kw):
        cfg = kw.get("default_query_job_config") or bigquery.QueryJobConfig()
        if cfg.maximum_bytes_billed is None: cfg.maximum_bytes_billed = limit
        if not cfg.labels: cfg.labels = {"rrp_job": job_label()}
        kw["default_query_job_config"] = cfg
        original(self, *args, **kw)

    bigquery.Client.__init__ = __init__
    bigquery.Client._rrp_capped = True


install()
