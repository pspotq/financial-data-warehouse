"""bq_cap: every BigQuery client gets a maximum_bytes_billed default (b247)."""
from conftest import load


def test_cap_values(monkeypatch):
    m = load("cloud/feeds/bq_cap.py", "bq_cap_feeds")
    monkeypatch.delenv("RRP_MAX_GB", raising=False); monkeypatch.delenv("RRP_EXTRA_ARGS", raising=False)
    assert m.cap_gb(["main.py"]) == 5
    assert m.cap_gb(["main.py", "--docs-days", "3"]) == 5
    assert m.cap_gb(["main.py", "--only", "crypto_hourly_history", "--days", "500"]) == 50
    assert m.cap_gb(["main.py", "--redemption-terms-range", "2015-01-01", "2026-06-30"]) == 50
    monkeypatch.setenv("RRP_MAX_GB", "12")
    assert m.cap_gb(["main.py"]) == 12


def test_client_gets_default_cap():
    load("cloud/market/bq_cap.py", "bq_cap_market")
    from google.cloud import bigquery
    from google.auth.credentials import AnonymousCredentials
    c = bigquery.Client(project="p", credentials=AnonymousCredentials())
    assert c.default_query_job_config.maximum_bytes_billed == 5_000_000_000
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=7)
    assert bigquery.Client(project="p", credentials=AnonymousCredentials(), default_query_job_config=cfg).default_query_job_config.maximum_bytes_billed == 7
    assert c.default_query_job_config.labels == {"rrp_job": "local"}
    assert cmp_files("cloud/feeds/bq_cap.py", "cloud/market/bq_cap.py")


def cmp_files(a, b):
    from conftest import ROOT
    import os
    return open(os.path.join(ROOT, a)).read() == open(os.path.join(ROOT, b)).read()
