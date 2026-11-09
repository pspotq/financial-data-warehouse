"""CUSIP -> ticker lookups from OpenFIGI for 13F holdings the 13G-based map can't place -> BigQuery `sec.cusip_figi`
(b190, added 2026-11-07).

rrp.cusip_map ties a CUSIP to a company from 13G filings, which name both; that covers issuers with a 13G filer over 5%
(about two thirds of listed companies, most of the market by value). For every other CUSIP held in the last two 13F
quarters, this asks OpenFIGI (Bloomberg's free identifier service) for the security behind it: ticker, exchange,
security type, name, FIGI. rrp.cusip_map then adds the CUSIPs whose US ticker matches a current SEC ticker
(sec.cik_tickers), which gives the company's CIK.

One row per CUSIP looked up, found or not (append-only; the newest row per cusip8 wins). Not-found CUSIPs are asked
again after 90 days. Largest positions first, so a capped run does the CUSIPs that matter most.
Without an API key OpenFIGI allows 25 requests a minute of 10 CUSIPs each (about 250 a minute); an OPENFIGI_KEY env
var raises that to 100 per request and 25 requests per 6 seconds.

  daily: the rrp-feeds 08:05 UTC run, up to 1,500 CUSIPs (about 6 minutes without a key)
  backfill: python main.py --cusip-figi --cusip-max 20000   (or the deploy workflow's run_job with those args)
"""
import datetime as dt, json, os, time

TABLE = "sec.cusip_figi"
URL = "https://api.openfigi.com/v3/mapping"
SCHEMA = [("cusip8", "STRING"), ("cusip", "STRING"), ("found", "BOOL"), ("ticker", "STRING"), ("exch_code", "STRING"),
          ("name", "STRING"), ("security_type", "STRING"), ("security_type2", "STRING"), ("market_sector", "STRING"),
          ("figi", "STRING"), ("composite_figi", "STRING"), ("share_class_figi", "STRING"), ("note", "STRING"),
          ("looked_up_at", "TIMESTAMP")]
US_EXCH = ("US", "UN", "UQ", "UW", "UA", "UR", "UP", "UF", "UV", "PQ")   # US composite first, then US venues / OTC


def _fields():
    from google.cloud import bigquery
    return [bigquery.SchemaField(c, t) for c, t in SCHEMA]


def candidates(bq, P, limit):
    """CUSIPs held (common long positions) in the last two 13F quarters that rrp.cusip_map can't place and OpenFIGI hasn't
    been asked about (or said not found more than 90 days ago), largest total value first."""
    q = f"""
    WITH h AS (
      SELECT SUBSTR(cusip, 1, 8) AS cusip8, ANY_VALUE(cusip) AS cusip, SUM(value_usd) AS v
      FROM `{P}.rrp.form13f_holdings`
      WHERE period IN (DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 DAY),
                       DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 1 QUARTER), INTERVAL 1 DAY),
                       DATE_SUB(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 2 QUARTER), INTERVAL 1 DAY))
        AND put_call IS NULL AND shares_type = 'SH' AND LENGTH(cusip) = 9
      GROUP BY 1),
    asked AS (SELECT cusip8, found, looked_up_at FROM `{P}.{TABLE}`
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cusip8 ORDER BY looked_up_at DESC) = 1)
    SELECT h.cusip8, h.cusip FROM h
    LEFT JOIN (SELECT cusip8 FROM `{P}.rrp.cusip_map` WHERE source = '13g') m USING (cusip8)
    LEFT JOIN asked a USING (cusip8)
    WHERE m.cusip8 IS NULL
      AND (a.cusip8 IS NULL OR (NOT a.found AND a.looked_up_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)))
    ORDER BY h.v DESC LIMIT {int(limit)}"""
    return [(r.cusip8, r.cusip) for r in bq.query(q).result()]


def pick(data):
    """The US listing out of OpenFIGI's answer for one CUSIP: the US composite if there is one, else the first US venue,
    else the first row (kept for the record; rrp.cusip_map only uses rows with a US exchange code)."""
    for code in US_EXCH:
        for d in data:
            if d.get("exchCode") == code:
                return d
    return data[0]


def lookup(cusips, post, key=None, log=print, flush=None, every=500):
    """[(cusip8, cusip)] -> rows. post(url, json, headers) -> (status, parsed json). Respects the rate limit.
    flush(rows) is called every `every` rows (and at the end) so a long run keeps what it has if it is stopped."""
    per, pause = (100, 0.25) if key else (10, 2.5)
    headers = {"Content-Type": "application/json"}
    if key:
        headers["X-OPENFIGI-APIKEY"] = key
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows, flushed = [], [0]
    for i in range(0, len(cusips), per):
        chunk = cusips[i:i + per]
        body = [{"idType": "ID_CUSIP", "idValue": c} for _, c in chunk]
        for attempt in range(4):
            status, out = post(URL, body, headers)
            if status == 429:   # over the limit: wait out the minute
                time.sleep(15 * (attempt + 1)); continue
            break
        if status != 200 or not isinstance(out, list):
            log(f"  openfigi HTTP {status} on chunk {i // per}; stopping this run")
            break
        for (c8, c), res in zip(chunk, out):
            data = res.get("data") or []
            if data:
                d = pick(data)
                rows.append(dict(cusip8=c8, cusip=c, found=True, ticker=d.get("ticker"), exch_code=d.get("exchCode"),
                                 name=d.get("name"), security_type=d.get("securityType"),
                                 security_type2=d.get("securityType2"), market_sector=d.get("marketSector"),
                                 figi=d.get("figi"), composite_figi=d.get("compositeFIGI"),
                                 share_class_figi=d.get("shareClassFIGI"), note=None, looked_up_at=stamp))
            else:
                rows.append(dict(cusip8=c8, cusip=c, found=False, note=(res.get("warning") or res.get("error") or "")[:200],
                                 looked_up_at=stamp))
        if flush and len(rows) - flushed[0] >= every:
            flush(rows[flushed[0]:]); flushed[0] = len(rows)
        if (i // per) % 50 == 0:
            log(f"  openfigi: {len(rows)} of {len(cusips)} looked up")
        time.sleep(pause)
    if flush and len(rows) > flushed[0]:
        flush(rows[flushed[0]:])
    return rows


def run(bq, P, max_lookups=1500, log=lambda m: print(m, flush=True)):
    import requests
    from google.cloud import bigquery
    key = os.environ.get("OPENFIGI_KEY") or None

    def post(url, body, headers):
        r = requests.post(url, data=json.dumps(body), headers=headers, timeout=60)
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, None

    cfg = bigquery.LoadJobConfig(schema=_fields(), source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                 write_disposition="WRITE_APPEND")
    todo = candidates(bq, P, max_lookups)
    log(f"  openfigi: {len(todo)} CUSIPs to look up")
    rows = lookup(todo, post, key, log,
                  flush=lambda rs: bq.load_table_from_json(rs, f"{P}.{TABLE}", job_config=cfg).result())
    found = sum(r["found"] for r in rows)
    return len(rows), f"{len(todo)} asked, {len(rows)} answered, {found} found"


def setup(bq, P):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    ref = f"{P}.{TABLE}"
    try:
        bq.get_table(ref); print(f"  {TABLE} exists")
    except NotFound:
        t = bigquery.Table(ref, schema=_fields())
        t.clustering_fields = ["cusip8"]
        bq.create_table(t); print(f"  created {TABLE}")
