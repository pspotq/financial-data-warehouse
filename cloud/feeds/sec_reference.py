"""SEC reference data -> BigQuery `sec`: company facts (shares outstanding, cash, debt, revenue ...), the CIK/ticker
map and delisting / deregistration filings. Added 2026-10-12.

Tables
  sec.xbrl_facts   one row per (company, concept, unit, period, filing): every value a 10-K / 10-Q / 20-F ... reported
                   for the CONCEPTS below, with the filing date, so a study can use only what was public at the time.
  sec.cik_tickers  the SEC's current CIK -> ticker / exchange list, one snapshot per day (delisted names drop off,
                   so history keeps them).
  sec.delistings   Form 25 / 25-NSE (removal from listing) and Form 15 / 15F (deregistration) filings, by company CIK.

In the rrp-feeds job (main.py): pull_delistings and pull_tickers once a day (7 AM ET run), pull_xbrl_frames on Mondays.
History, run once from the PC (safe to re-run; MERGE on the natural keys):
  py sec_reference.py --xbrl-bulk                 # companyfacts.zip (~1.3 GB, cached in ./_bulk) -> sec.xbrl_facts
  py sec_reference.py --delistings-since 2006     # quarterly full-index form.idx files -> sec.delistings
  py sec_reference.py --xbrl-bulk --dry           # parse and count only, write nothing
SEC fair access: <= 8 requests/second with a descriptive User-Agent (main.get(sec=True) / the throttle here).
"""
import datetime as dt, io, json, os, re, sys, time, zipfile
import bq_cap  # noqa: F401  hard cap on BigQuery query bytes (b247)

PROJECT = os.environ.get("RRP_PROJECT", "rrp-technologies")
CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_bulk")
UA = {"User-Agent": "RRP Technologies research bot (reference data)", "Accept-Encoding": "gzip, deflate"}

# (taxonomy, concept, unit, period kind for the frames API: I = instant, Q = quarter, Y = year)
CONCEPTS = [
    ("dei", "EntityCommonStockSharesOutstanding", "shares", "I"),
    ("dei", "EntityPublicFloat", "USD", "I"),
    ("us-gaap", "CommonStockSharesOutstanding", "shares", "I"),
    ("us-gaap", "WeightedAverageNumberOfDilutedSharesOutstanding", "shares", "Q"),
    ("us-gaap", "CashAndCashEquivalentsAtCarryingValue", "USD", "I"),
    ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents", "USD", "I"),
    ("us-gaap", "ShortTermInvestments", "USD", "I"),
    ("us-gaap", "MarketableSecuritiesCurrent", "USD", "I"),
    ("us-gaap", "AssetsCurrent", "USD", "I"),
    ("us-gaap", "Assets", "USD", "I"),
    ("us-gaap", "LiabilitiesCurrent", "USD", "I"),
    ("us-gaap", "Liabilities", "USD", "I"),
    ("us-gaap", "LongTermDebt", "USD", "I"),
    ("us-gaap", "LongTermDebtNoncurrent", "USD", "I"),
    ("us-gaap", "LongTermDebtCurrent", "USD", "I"),
    ("us-gaap", "DebtCurrent", "USD", "I"),
    ("us-gaap", "StockholdersEquity", "USD", "I"),
    ("us-gaap", "Revenues", "USD", "Q"),
    ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax", "USD", "Q"),
    ("us-gaap", "OperatingIncomeLoss", "USD", "Q"),
    ("us-gaap", "NetIncomeLoss", "USD", "Q"),
    ("us-gaap", "EarningsPerShareDiluted", "USD/shares", "Q"),
    ("us-gaap", "NetCashProvidedByUsedInOperatingActivities", "USD", "Y"),
    ("us-gaap", "PaymentsToAcquirePropertyPlantAndEquipment", "USD", "Y"),
    ("us-gaap", "PaymentsForRepurchaseOfCommonStock", "USD", "Y"),
]
WANT = {(t, c) for t, c, _, _ in CONCEPTS}
DELIST_FORMS = {"25", "25/A", "25-NSE", "25-NSE/A", "15-12B", "15-12B/A", "15-12G", "15-12G/A", "15-15D", "15-15D/A",
                "15F-12B", "15F-12B/A", "15F-12G", "15F-12G/A", "15F-15D", "15F-15D/A"}
IDX_LINE = re.compile(r"^(\S.*?)\s{2,}(.+?)\s{2,}(\d+)\s{2,}(\d{4}-?\d{2}-?\d{2})\s{2,}(edgar/data/\S+)\s*$")

F, D, St, T = "FLOAT", "DATE", "STRING", "TIMESTAMP"
SCHEMAS = {
    "xbrl_facts": [("cik", St), ("entity", St), ("taxonomy", St), ("concept", St), ("unit", St), ("period_start", D),
                   ("period_end", D), ("val", F), ("accn", St), ("fy", St), ("fp", St), ("form", St), ("filed", D),
                   ("frame", St), ("source", St), ("ingested_at", T)],
    "cik_tickers": [("as_of", D), ("cik", St), ("name", St), ("ticker", St), ("exchange", St), ("ingested_at", T)],
    "delistings": [("form", St), ("company", St), ("cik", St), ("filed", D), ("accession", St), ("path", St),
                   ("ingested_at", T)],
}
TABLES = {"xbrl_facts": "sec.xbrl_facts", "cik_tickers": "sec.cik_tickers", "delistings": "sec.delistings"}
MERGE_KEYS = {"xbrl_facts": ["cik", "taxonomy", "concept", "unit", "period_start", "period_end", "accn"],
              "cik_tickers": ["as_of", "cik", "ticker"], "delistings": ["accession", "cik", "form"]}
PARTITION = {"xbrl_facts": ("period_end", "YEAR"), "cik_tickers": ("as_of", "MONTH"), "delistings": ("filed", "YEAR")}
CLUSTER = {"xbrl_facts": ["cik", "concept"], "cik_tickers": ["ticker"], "delistings": ["cik"]}


def _d(s):
    s = (s or "").strip()
    if re.fullmatch(r"\d{8}", s): s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s or None


# ------------------------------------------------------------------ pulls used by the hourly job (get = main.get)
def pull_tickers(now, get):
    r = get("https://www.sec.gov/files/company_tickers_exchange.json", sec=True)
    js = r.json(); idx = {k: i for i, k in enumerate(js["fields"])}
    as_of = (now - dt.timedelta(hours=4)).date().isoformat()
    rows = [dict(as_of=as_of, cik=str(x[idx["cik"]]), name=x[idx["name"]], ticker=(x[idx["ticker"]] or "").upper(),
                 exchange=x[idx["exchange"]], ingested_at=now.strftime("%Y-%m-%d %H:%M:%S")) for x in js["data"]]
    if len(rows) < 5000: raise RuntimeError(f"ticker list too short ({len(rows)})")
    return {"cik_tickers": rows}, f"{len(rows)} tickers"


def _idx_rows(txt, now):
    out = []
    for line in txt.splitlines():
        m = IDX_LINE.match(line)
        if not m or m.group(1).strip() not in DELIST_FORMS: continue
        path = m.group(5)
        acc = re.search(r"(\d{10}-?\d{2}-?\d{6})", path)
        out.append(dict(form=m.group(1).strip(), company=m.group(2).strip(), cik=m.group(3).lstrip("0"),
                        filed=_d(m.group(4)), accession=(acc.group(1) if acc else path).replace("-", ""), path=path,
                        ingested_at=now.strftime("%Y-%m-%d %H:%M:%S")))
    return out


def pull_delistings(now, get, days=10):
    """Form 25 / 15 filings from the last `days` daily form indexes (one small file per business day)."""
    rows, seen = [], 0
    for i in range(1, days + 1):
        day = (now - dt.timedelta(days=i)).date()
        if day.weekday() >= 5: continue
        try:   # a holiday has no index; sec.gov answers a missing file with 403 as well as 404
            r = get(f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{(day.month - 1) // 3 + 1}/form.{day:%Y%m%d}.idx", sec=True)
        except Exception:
            r = None
        if r is None: continue
        seen += 1; rows += _idx_rows(r.text, now)
    return {"delistings": rows}, f"{seen} daily indexes, {len(rows)} filings"


def _frame_periods(kind, today):
    """The frames to ask for: the last 4 calendar quarters (I/Q) or the last 2 years (Y)."""
    q = (today.month - 1) // 3 + 1; y = today.year
    qs = []
    for _ in range(4):
        q -= 1
        if q == 0: q, y = 4, y - 1
        qs.append((y, q))
    if kind == "I": return [f"CY{y}Q{q}I" for y, q in qs]
    if kind == "Q": return [f"CY{y}Q{q}" for y, q in qs]
    return [f"CY{today.year - 1}", f"CY{today.year - 2}"]


def pull_xbrl_frames(now, get, bq=None):
    """Recent periods for every concept, all companies, from the frames API (one call per concept and period).
    With bq the rows are written in batches of ~150k (memory and MERGE cost); without it (dry run) they are returned."""
    rows, calls, written = [], 0, 0
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    for tax, concept, unit, kind in CONCEPTS:
        for per in _frame_periods(kind, now.date()):
            r = get(f"https://data.sec.gov/api/xbrl/frames/{tax}/{concept}/{unit}/{per}.json", sec=True); calls += 1
            if r is None: continue
            for x in r.json().get("data", []):
                rows.append(dict(cik=str(x.get("cik")), entity=x.get("entityName"), taxonomy=tax, concept=concept, unit=unit,
                                 period_start=_d(x.get("start")), period_end=_d(x.get("end")), val=x.get("val"),
                                 accn=(x.get("accn") or "").replace("-", ""), frame=per, source="frames", ingested_at=stamp))
        if bq is not None and len(rows) >= 150000:
            written += write(bq, "xbrl_facts", rows); rows = []
    if bq is None: return {"xbrl_facts": rows}, f"{calls} frames, {len(rows)} facts"
    written += write(bq, "xbrl_facts", rows)
    return {"_direct_rows": written}, f"{calls} frames, {written} facts"


# ------------------------------------------------------------------ history (PC)
_last = [0.0]


def _get(url, stream=False):
    import requests
    for i in range(5):
        wait = 0.125 - (time.time() - _last[0])
        if wait > 0: time.sleep(wait)
        _last[0] = time.time()
        try:
            r = requests.get(url, headers=UA, timeout=600, stream=stream)
            if r.status_code == 404: return None
            if r.status_code in (429, 503): time.sleep(5 * (i + 1)); continue
            r.raise_for_status(); return r
        except Exception:
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"gave up on {url}")


def companyfacts_rows(path, stamp):
    """Yield fact rows for CONCEPTS from every company in companyfacts.zip."""
    z = zipfile.ZipFile(path)
    for n, name in enumerate(z.namelist(), 1):
        if not name.endswith(".json"): continue
        try: js = json.loads(z.read(name))
        except ValueError: continue
        cik, ent = str(js.get("cik") or "").lstrip("0"), js.get("entityName")
        for tax, concepts in (js.get("facts") or {}).items():
            for concept, body in concepts.items():
                if (tax, concept) not in WANT: continue
                for unit, facts in (body.get("units") or {}).items():
                    for x in facts:
                        yield dict(cik=cik, entity=ent, taxonomy=tax, concept=concept, unit=unit,
                                   period_start=_d(x.get("start")), period_end=_d(x.get("end")), val=x.get("val"),
                                   accn=(x.get("accn") or "").replace("-", ""), fy=str(x.get("fy") or ""),
                                   fp=x.get("fp"), form=x.get("form"), filed=_d(x.get("filed")), frame=x.get("frame"),
                                   source="companyfacts", ingested_at=stamp)
        if n % 2000 == 0: print(f"  {n} companies read", flush=True)


def xbrl_bulk(dry=False):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, "companyfacts.zip")
    if not (os.path.exists(path) and zipfile.is_zipfile(path)):
        print("downloading companyfacts.zip (~1.3 GB) ...", flush=True)
        r = _get("https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip", stream=True)
        with open(path + ".part", "wb") as f:
            for chunk in r.iter_content(1 << 20): f.write(chunk)
        os.replace(path + ".part", path)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    bq = None if dry else _bq()
    empty = dry or _row_count(bq, "xbrl_facts") == 0
    batch, total = [], 0
    for row in companyfacts_rows(path, stamp):
        batch.append(row)
        if len(batch) >= 500000:
            total += len(batch) if dry else write(bq, "xbrl_facts", batch, append=empty); batch = []
            print(f"  {total:,} facts {'counted' if dry else 'loaded'}", flush=True)
    if batch: total += len(batch) if dry else write(bq, "xbrl_facts", batch, append=empty)
    print(f"xbrl bulk done: {total:,} facts")


def delistings_since(year, dry=False):
    now = dt.datetime.now(dt.timezone.utc)
    bq = None if dry else _bq()
    total = 0
    for y in range(year, now.year + 1):
        for q in range(1, 5):
            if dt.date(y, 3 * q - 2, 1) > now.date(): break
            r = _get(f"https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.idx")
            if r is None: print(f"{y}Q{q}: no index"); continue
            rows = _idx_rows(r.text, now)
            total += len(rows) if dry else write(bq, "delistings", rows)
            print(f"{y}Q{q}: {len(rows)} Form 25 / 15 filings", flush=True)
    print(f"delistings done: {total:,} filings")


# ------------------------------------------------------------------ BigQuery
def _bq():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT)


def _row_count(bq, table):
    return list(bq.query(f"SELECT COUNT(*) n FROM `{PROJECT}.{TABLES[table]}`").result())[0].n


def write(bq, table, rows, append=False):
    """MERGE on MERGE_KEYS through a staging table (append=True loads straight in: first load into an empty table)."""
    from google.cloud import bigquery
    if not rows: return 0
    cols = [c for c, _ in SCHEMAS[table]]
    seen, uniq = set(), []
    for r in rows:
        k = tuple(r.get(c) for c in MERGE_KEYS[table])
        if k not in seen: seen.add(k); uniq.append({c: r.get(c) for c in cols})
    schema = [bigquery.SchemaField(c, t) for c, t in SCHEMAS[table]]
    cfg = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    if append:
        cfg.write_disposition = "WRITE_APPEND"
        bq.load_table_from_json(uniq, f"{PROJECT}.{TABLES[table]}", job_config=cfg).result(); return len(uniq)
    stage = f"{PROJECT}.staging.ref_{table}"
    # a fresh table each time: staging tables expire a day after they are CREATED and WRITE_TRUNCATE keeps the old expiry,
    # so a once-a-day load can hit the expiry mid-run (cot, Oct 28 11:05 UTC: "Destination deleted/expired during operation")
    bq.delete_table(stage, not_found_ok=True)
    cfg.write_disposition = "WRITE_TRUNCATE"
    bq.load_table_from_json(uniq, stage, job_config=cfg).result()
    keys = MERGE_KEYS[table]
    # Null-safe equality written as COALESCE(..) = COALESCE(..) so BigQuery can hash-join. IS NOT DISTINCT FROM forces a
    # nested-loop join: the first Monday cloud run (2026-10-19) ran 56 min on 156k x 14.7M rows and never finished.
    types = dict(SCHEMAS[table])
    blank = {"DATE": "DATE '0001-01-01'", "TIMESTAMP": "TIMESTAMP '0001-01-01'", "FLOAT": "-1e308", "INTEGER": "-1"}
    on = " AND ".join(f"COALESCE(t.{k}, {blank.get(types[k], chr(39) * 2)}) = COALESCE(s.{k}, {blank.get(types[k], chr(39) * 2)})"
                      for k in keys)
    col = PARTITION[table][0]   # limit the MERGE to the partitions the batch touches (cost)
    lo = min((r[col] for r in uniq if r.get(col)), default=None)
    if lo: on += f" AND t.{col} >= DATE '{lo}'"
    # A refresh never blanks a field it does not carry (frames rows have no filed / fy / fp / form), and a frames row
    # never replaces a companyfacts row, which carries the filing date the point-in-time views depend on.
    upd = ", ".join(f"{c} = s.{c}" if c == "ingested_at" else f"{c} = COALESCE(s.{c}, t.{c})" for c in cols if c not in keys)
    cond = " AND NOT (s.source = 'frames' AND t.source = 'companyfacts')" if table == "xbrl_facts" else ""
    bq.query(f"""MERGE `{PROJECT}.{TABLES[table]}` t USING `{stage}` s ON {on}
                 WHEN MATCHED{cond} THEN UPDATE SET {upd}
                 WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})""").result()
    return len(uniq)


def setup(bq, project=PROJECT):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    for name, loc in TABLES.items():
        ref = f"{project}.{loc}"
        try: bq.get_table(ref); print(f"  {loc} exists"); continue
        except NotFound: pass
        t = bigquery.Table(ref, schema=[bigquery.SchemaField(c, ty) for c, ty in SCHEMAS[name]])
        col, unit = PARTITION[name]
        t.time_partitioning = bigquery.TimePartitioning(type_=getattr(bigquery.TimePartitioningType, unit), field=col)
        t.clustering_fields = CLUSTER[name]
        bq.create_table(t); print(f"  created {loc}")


if __name__ == "__main__":
    dry = "--dry" in sys.argv
    if "--xbrl-bulk" in sys.argv: xbrl_bulk(dry)
    elif "--delistings-since" in sys.argv: delistings_since(int(sys.argv[sys.argv.index("--delistings-since") + 1]), dry)
    else: print(__doc__)
