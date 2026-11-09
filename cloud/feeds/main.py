"""RRP filings and government data ingest -> BigQuery datasets `sec` and `gov`.

Runs as the Cloud Run job `rrp-feeds` every hour at :05 (Cloud Scheduler `rrp-feeds-hourly`). Each source is
independent: a failure is written to `ops.feeds_runs` and the rest carry on; the job always exits 0.
The rrp.* views over these tables are the dbt models in warehouse/, not here.

  python main.py                 # one run (what the job does)
  python main.py --setup         # create the tables if missing (dbt in warehouse/ builds the views)
  python main.py --backfill      # deep pulls: EFTS 30 days, COT since 2018, DoD awards 2 years
  python main.py --dry           # pull only, print counts, write nothing
  python main.py --dod-range 2025-03-01 2025-07-31   # re-pull one DoD date range in 10-day windows
  python main.py --docs-only --docs-days 30          # filing text only, reaching back 30 days
  python main.py --dry --reference                   # also run the once-a-day / Monday reference pulls now
  python main.py --edgar-index-only --edgar-index-days 400   # EDGAR daily-index counts per form, history (ops.edgar_index_counts)
  python main.py --efts-count 2001-01-01 2026-08-30  # history backfill, step 1: hits per stream per year (no writes)
  python main.py --efts-range 2001-01-01 2026-08-30  # history backfill, step 2: every stream, month by month
       re-running resumes: stream-years already loaded (ops.feeds_runs, source efts_history) are skipped; --redo pulls all
       add --only tender_issuer,activist_13d to limit the streams
  python main.py --tender-terms-range 2015-01-01 2026-10-21   # tender terms for every Schedule TO with text (history)
  python main.py --stake-terms-range 2015-01-01 2026-10-21    # 13D stakes as columns for every 13D with text (history)
  python main.py --redemption-terms-range 2015-01-01 2026-10-22   # redemption/call terms for the redemption streams (history)
  python main.py --docs-range 2015-01-01 2026-09-30  # filing text for the backfilled streams (tenders, 13Ds, redemptions
       first), stream by stream, year by year; lean: main document + 2 exhibits. Re-running resumes (filings with text
       are skipped). --only works here too. One ops.feeds_runs row per stream-year, source docs_history.

Sources -> tables
  efts      EDGAR full-text search, 20 streams (tenders, 13D, 8-K bankruptcy / redemption / merger / rights /
            crypto / minerals, Form D digital assets), last 3 days -> sec.filing_hits
  form4_today  EDGAR live "latest filings" feed, today's Form 4s, every P/S line, each run (form4_scan.py) -> sec.form4_today,
            and the ownership XML of each filing with a P/S line -> sec.filing_docs (fetched once, 2026-10-26)
  form4     EDGAR daily form index, every Form 4 P/S line, yesterday and earlier (form4_scan.py) -> sec.form4; runs
            04:05-13:05 UTC (the index posts overnight). Filings the live feed read are copied, the rest fetched (their XML
            -> sec.filing_docs). After a new day loads, rebuilds sec.insider_filings_snap (--snap forces it)
  redemption_terms (once a day at 08:05 UTC, after docs) calls of notes/preferred/warrants from the redemption 8-Ks, last 14 days -> sec.redemption_terms
  stake_terms   (once a day at 08:05 UTC, after docs) Schedule 13D stakes as columns, last 14 days (stake_terms.py) -> sec.stake_terms
  tender_terms  (once a day at 08:05 UTC, after docs) Schedule TO terms as columns, last 14 days (tender_terms.py) -> sec.tender_terms
  docs      text of every filing_hits filing, and the XML of any Form 4 with a P/S line not already saved by the two
            Form 4 steps (filing_docs.py) -> sec.filing_docs
  cot       CFTC Commitments of Traders, every market: financial futures (TFF) + disaggregated commodities, last 21 days;
            Friday 19:05-23:05 UTC (release ~3:30 PM ET) and the 11:05 UTC run daily
            (history: py main.py --cot-history --only-cot, every report since 2006) -> gov.cot
  dod       USAspending Department of Defense contract transactions on awards >= $100M, last 180 days, 11:05 UTC daily -> gov.dod_awards
  war_rss   war.gov daily contracts RSS (which days have a contracts page), 11:05 and 23:05 UTC -> gov.war_contract_days
  tickers   SEC CIK -> ticker / exchange list, one snapshot a day, 08:05 UTC (sec_reference.py) -> sec.cik_tickers
  delist    Form 25 / 25-NSE / 15 / 15F filings from the last 10 daily indexes, 08:05 UTC daily -> sec.delistings
  xbrl      company facts (shares outstanding, cash, debt, revenue ...) for recent periods, Mondays 11:05 UTC -> sec.xbrl_facts
            (history: py sec_reference.py --xbrl-bulk / --delistings-since 2006, from the PC)
  edgar_index  every form's filing count in EDGAR's daily form indexes, last 14 days, 08:05 and 11:05 UTC -> ops.edgar_index_counts
            (what EDGAR published; rrp.edgar_completeness compares it with what the sec.* tables loaded)
  cusip_figi (08:05 UTC, after funds) OpenFIGI ticker for 13F CUSIPs the 13G map can't place, up to 1,500 a day,
            largest positions first (cusip_figi.py) -> sec.cusip_figi; feeds rrp.cusip_map
  funds     Form D, 13F, Form 144 and 13G filings from the last 5 daily indexes (XML parsed), plus the monthly ADV
            snapshot, 08:05 UTC daily (sec_funds.py) -> sec.formd, sec.form13f_filings / _holdings / _totals / _unit_fixes,
            sec.form144, sec.form13g, sec.adv_firms (history: py sec_funds.py --formd-history / --13f-history / ...)
"""
import datetime as dt, json, os, re, sys, time
import requests
import bq_cap  # noqa: F401  hard cap on BigQuery query bytes (b247)
import filing_docs
import tender_terms
import stake_terms
import cusip_figi
import redemption_terms
import sec_reference
import sec_funds

PROJECT = os.environ.get("RRP_PROJECT", "rrp-technologies")
# where each table lives (key = the name used in this file); MERGE scratch tables go to `staging` (expire after a day)
TABLES = {"efts_hits": "sec.filing_hits", "cot": "gov.cot", "dod_awards": "gov.dod_awards",
          "war_contract_days": "gov.war_contract_days", "edgar_index": "ops.edgar_index_counts", "runs": "ops.feeds_runs",
          "filing_index": "sec.filing_index"}
UA = {"User-Agent": "RRP Technologies research bot (filings ingest)", "Accept-Encoding": "gzip, deflate"}
S = requests.Session(); S.headers.update(UA)
_last = [0.0]
# one-off runs of the Cloud Run job pass flags this way:
#   gcloud run jobs execute rrp-feeds --update-env-vars "RRP_EXTRA_ARGS=--docs-range 2015-01-01 2026-09-30"
sys.argv += os.environ.get("RRP_EXTRA_ARGS", "").split()
ARGS = set(sys.argv[1:])
# subject split (rebuild b252, 2026-10-29): one Cloud Run job per subject, so each dataset has one writer. rrp-sec runs this
# file with RRP_SUBJECT=sec (everything SEC), rrp-gov with RRP_SUBJECT=gov (CFTC, DoD contracts, war.gov). Unset = all
# sources (the old rrp-feeds job; one-off runs and backfills).
SUBJECT = os.environ.get("RRP_SUBJECT") or None
GOV_SOURCES = {"cot", "dod", "war_rss"}
DOD_RANGE = None
if "--dod-range" in sys.argv:   # py main.py --dod-range 2025-03-01 2025-07-31
    _i = sys.argv.index("--dod-range")
    DOD_RANGE = (dt.date.fromisoformat(sys.argv[_i + 1]), dt.date.fromisoformat(sys.argv[_i + 2]))
EFTS_RANGE = None   # --efts-range / --efts-count START END
for _flag in ("--efts-range", "--efts-count"):
    if _flag in sys.argv:
        _i = sys.argv.index(_flag)
        EFTS_RANGE = (dt.date.fromisoformat(sys.argv[_i + 1]), dt.date.fromisoformat(sys.argv[_i + 2]))
ONLY = set(sys.argv[sys.argv.index("--only") + 1].split(",")) if "--only" in sys.argv else None
DOCS_RANGE = None   # --docs-range START END
if "--docs-range" in sys.argv:
    _i = sys.argv.index("--docs-range")
    DOCS_RANGE = (dt.date.fromisoformat(sys.argv[_i + 1]), dt.date.fromisoformat(sys.argv[_i + 2]))
TENDER_RANGE = None   # --tender-terms-range START END
if "--tender-terms-range" in sys.argv:
    _i = sys.argv.index("--tender-terms-range")
    TENDER_RANGE = (dt.date.fromisoformat(sys.argv[_i + 1]), dt.date.fromisoformat(sys.argv[_i + 2]))
REDEMPTION_RANGE = None   # --redemption-terms-range START END
if "--redemption-terms-range" in sys.argv:
    _i = sys.argv.index("--redemption-terms-range")
    REDEMPTION_RANGE = (dt.date.fromisoformat(sys.argv[_i + 1]), dt.date.fromisoformat(sys.argv[_i + 2]))
CUSIP_MAX = int(sys.argv[sys.argv.index("--cusip-max") + 1]) if "--cusip-max" in sys.argv else 1500
STAKE_RANGE = None   # --stake-terms-range START END
if "--stake-terms-range" in sys.argv:
    _i = sys.argv.index("--stake-terms-range")
    STAKE_RANGE = (dt.date.fromisoformat(sys.argv[_i + 1]), dt.date.fromisoformat(sys.argv[_i + 2]))
FILING_INDEX_RANGE = None   # --filing-index-range 1994Q1 2026Q4 (b192: EDGAR's quarterly full form index)
if "--filing-index-range" in sys.argv:
    _i = sys.argv.index("--filing-index-range")
    FILING_INDEX_RANGE = (sys.argv[_i + 1].upper(), sys.argv[_i + 2].upper())
DOCS_DAYS = int(sys.argv[sys.argv.index("--docs-days") + 1]) if "--docs-days" in sys.argv else 3


def get(url, params=None, tries=4, sec=False, **kw):
    for i in range(tries):
        if sec:   # SEC fair access: <= 8 requests/second
            wait = 0.125 - (time.time() - _last[0])
            if wait > 0: time.sleep(wait)
            _last[0] = time.time()
        try:
            r = S.get(url, params=params, timeout=30, **kw)
            if r.status_code == 404: return None
            if r.status_code in (429, 500, 502, 503, 504): time.sleep(4 * (i + 1)); continue
            r.raise_for_status(); return r
        except requests.RequestException:
            if i == tries - 1: raise
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"gave up on {url}")


def now_utc():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def fnum(x):
    try: return float(x)
    except (TypeError, ValueError): return None


# ------------------------------------------------------------------ EDGAR full-text search
EFTS = [  # (query_id, q phrase or None, forms)
    # form4_open_market ("open market", forms 4) retired 2026-10-27 (b194): the full Form 4 feed replaced it; its rows stay as history
    ("tender_issuer", None, "SC TO-I"),
    ("tender_third_party", None, "SC TO-T"),
    ("activist_13d", None, "SCHEDULE 13D"),
    ("activist_13d_old_code", None, "SC 13D"),
    ("bankruptcy_ch11", "chapter 11", "8-K"),
    ("bankruptcy_receivership", "receivership", "8-K"),
    ("redemption_notice", "notice of redemption", "8-K"),
    ("redemption_partial", "partial redemption", "8-K"),
    ("merger_agreement", "agreement and plan of merger", "8-K"),
    ("rights_offering", "rights offering", "8-K"),
    ("crypto_bitcoin", "bitcoin", "8-K"),
    ("crypto_ethereum", "ethereum", "8-K"),
    ("crypto_solana", "solana", "8-K"),
    ("crypto_digital_asset", "digital asset", "8-K"),
    ("minerals_dpa", "Defense Production Act", "8-K"),
    ("minerals_critical", "critical mineral", "8-K"),
    ("minerals_lpo", "Loan Programs Office", "8-K"),
    ("minerals_osc", "Office of Strategic Capital", "8-K"),
    ("minerals_trs", "technical report summary", "8-K"),
    ("formd_digital_assets", "digital assets", "D"),
]


def efts_get(qid, p, tries=6):
    """One EFTS request, checked: the response must carry hits and a query object that matches what was asked
    (forms and start date). Under sustained load EFTS sometimes answers with an error body or another query's
    response; those are retried with a growing pause (5 s up to 2 min) before the run gives up on that window."""
    for i in range(tries):
        try:
            r = get("https://efts.sec.gov/LATEST/search-index", params=p, sec=True)
            d = r.json() if r is not None else {}
            filt = json.dumps(d.get("query", {}))
            if "hits" in d and p["forms"] in filt and p["startdt"] in filt:
                return d
            why = "no hits in response" if "hits" not in d else "query object does not match the request"
        except ValueError as e:
            why = f"bad JSON ({e})"
        if i < tries - 1: time.sleep(min(120, 5 * 2 ** i))
    raise RuntimeError(f"{qid}: {why} after {tries} tries ({p['startdt']}..{p['enddt']})")


def efts_query(qid, phrase, forms, start, end, cap=2000):
    rows, frm = [], 0
    while True:
        p = {"forms": forms, "dateRange": "custom", "startdt": start, "enddt": end, "from": frm}
        if phrase: p["q"] = f'"{phrase}"'
        d = efts_get(qid, p)
        hits = d["hits"]["hits"]
        for h in hits:
            s = h["_source"]
            acc = s.get("adsh") or h["_id"].split(":")[0]
            fname = h["_id"].split(":", 1)[1] if ":" in h["_id"] else ""
            ciks = s.get("ciks") or []
            rows.append(dict(query_id=qid, phrase=phrase, forms=forms, doc_id=h["_id"], accession=acc, filename=fname,
                             file_date=s.get("file_date"), form=s.get("form"), root_forms=",".join(s.get("root_forms") or []),
                             file_type=s.get("file_type"), file_description=s.get("file_description"),
                             ciks=",".join(ciks), display_names=" | ".join(s.get("display_names") or []),
                             biz_locations=" | ".join(s.get("biz_locations") or []), inc_states=",".join(s.get("inc_states") or []),
                             sics=",".join(s.get("sics") or []), items=",".join(s.get("items") or []),
                             period_ending=s.get("period_ending"),
                             index_url=(f"https://www.sec.gov/Archives/edgar/data/{int(ciks[0])}/{acc.replace('-', '')}/" if ciks else None),
                             doc_url=(f"https://www.sec.gov/Archives/edgar/data/{int(ciks[0])}/{acc.replace('-', '')}/{fname}" if ciks and fname else None)))
        frm += len(hits)
        total = d["hits"]["total"]["value"]
        if not hits or frm >= total or frm >= cap: return rows, total


def efts_total(phrase, forms, start, end, qid="count"):
    """One request: how many hits a window has. EFTS reports at most 10,000 (relation 'gte' beyond that)."""
    p = {"forms": forms, "dateRange": "custom", "startdt": start, "enddt": end}
    if phrase: p["q"] = f'"{phrase}"'
    t = efts_get(qid, p)["hits"]["total"]
    return t["value"], t.get("relation", "eq")


def _windows(start, end, days):
    d = start
    while d <= end:
        e = min(d + dt.timedelta(days=days - 1), end); yield d, e; d = e + dt.timedelta(days=1)


def _efts_streams():
    return [x for x in EFTS if ONLY is None or x[0] in ONLY]


def efts_count(start, end):
    """Backfill step 1: hits per stream per calendar year, so the size of the history pull is known first."""
    grand = 0
    for qid, phrase, forms in _efts_streams():
        per = []
        for y in range(start.year, end.year + 1):
            a, b = max(start, dt.date(y, 1, 1)), min(end, dt.date(y, 12, 31))
            n, rel = efts_total(phrase, forms, a.isoformat(), b.isoformat())
            per.append(f"{y}:{n}{'+' if rel == 'gte' else ''}"); grand += n
        print(f"{qid}: {sum(int(x.split(':')[1].rstrip('+')) for x in per):,}  ({' '.join(per)})", flush=True)
    print(f"total hits (10,000+ years counted as 10,000): {grand:,}")


def pull_efts_range(bq, start, end):
    """Backfill step 2: every stream month by month (a month with 10,000+ hits is split into days, the EFTS paging
    limit), one write per stream per year. first_seen = the filing date, so history never shows up as new in
    rrp.sec_filings_new. Safe to re-run: rows merge on (query_id, doc_id). Logs one ops.feeds_runs row per stream-year
    (source efts_history: one-off, left out of rrp.health_feeds)."""
    ts_now = now_utc().strftime("%Y-%m-%d %H:%M:%S")
    done = set()   # stream-years an earlier run already loaded (resume after a stop or a failure); --redo ignores them
    if bq is not None and "--redo" not in ARGS:
        for r in bq.query(f"SELECT note FROM `{PROJECT}.{TABLES['runs']}` WHERE source = 'efts_history' AND ok "
                          f"AND ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)").result():
            m = re.match(r"(\S+) (\d{4}):", r.note or "")
            if m: done.add((m.group(1), int(m.group(2))))
        if done: print(f"resuming: {len(done)} stream-years already loaded are skipped (--redo to pull them again)")
    for qid, phrase, forms in _efts_streams():
        for y in range(start.year, end.year + 1):
            if (qid, y) in done: continue
            t0 = time.time(); err = None; rows = []; note = ""
            try:
                a, b = max(start, dt.date(y, 1, 1)), min(end, dt.date(y, 12, 31))
                for ms, me in _windows(a, b, 31):
                    n, rel = efts_total(phrase, forms, ms.isoformat(), me.isoformat(), qid)
                    if n == 0: continue
                    spans = [(ms, me)] if n < 10000 and rel != "gte" else list(_windows(ms, me, 1))
                    for ws, we in spans:
                        got, _ = efts_query(qid, phrase, forms, ws.isoformat(), we.isoformat(), cap=10000)
                        rows += got
                for r in rows:
                    r["first_seen"] = f"{r['file_date']} 00:00:00" if r.get("file_date") else ts_now
                    r["last_seen"] = ts_now
                n_written = write(bq, "efts_hits", rows) if bq is not None and rows else len(rows)
                note = f"{qid} {y}: {n_written} rows"
            except Exception as e:
                err = f"{type(e).__name__}: {e}"[:500]; n_written = 0
            h = dict(ts=ts_now, source="efts_history", ok=err is None, row_count=n_written,
                     seconds=round(time.time() - t0, 2), note=note or f"{qid} {y}", error=err)
            print(f"{qid} {y}: {'ok' if err is None else 'FAILED ' + err} ({n_written} rows, {h['seconds']}s)", flush=True)
            if bq is not None:
                try: write(bq, "runs", [h])
                except Exception as e: print(f"health write FAILED: {e}")


def pull_efts(now, days=3):
    start = (now.date() - dt.timedelta(days=days)).isoformat(); end = now.date().isoformat()
    out, notes = [], []
    for qid, phrase, forms in EFTS:
        rows, total = efts_query(qid, phrase, forms, start, end, cap=5000 if "--backfill" in ARGS else 2000)
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        for r in rows: r["first_seen"] = ts; r["last_seen"] = ts
        out += rows; notes.append(f"{qid}={total}")
    return {"efts_hits": out}, "; ".join(notes)


# ------------------------------------------------------------------ Form 4 daily index (form4_scan.py)
def pull_form4(now, bq):
    """Days not yet in sec.form4, from the daily index (the checklist). Filings the live feed already read are copied
    from sec.form4_today; only the rest are fetched, and their ownership XML goes to sec.filing_docs (b201)."""
    import form4_scan as f4
    done = f4.loaded_days(bq, PROJECT)
    day = dt.date.fromisoformat(max(done)) + dt.timedelta(days=1) if done else now.date() - dt.timedelta(days=5)
    loaded, notes = 0, []
    t0 = time.time(); stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    while day < now.date() and time.time() - t0 < 2400:     # yesterday and earlier; index for today posts overnight
        if day.weekday() < 5 and day.isoformat() not in done:
            idx = f4.day_index(day)
            if idx is None: notes.append(f"{day}: no index yet")
            else:
                copied, seen = f4.from_today(bq, PROJECT, day)
                todo = [x for x in idx if re.sub(r"\D", "", x[0].rsplit("/", 1)[-1]) not in seen]
                docs = []
                rows = f4.fetch(todo, day.isoformat(), docs=docs) + copied
                if rows:
                    f4.load_rows(bq, PROJECT, rows, stamp)
                    loaded += len(rows)
                    notes.append(f"{day}: {len(rows)} rows ({len(idx) - len(todo)} filings from the live feed, {len(todo)} fetched)")
                if docs:
                    for d in docs: d["fetched_at"] = stamp
                    filing_docs.load(bq, PROJECT, docs)
        day += dt.timedelta(days=1)
    if loaded or "--snap" in ARGS:
        notes.append(snap_insider_filings(bq))
    return {"_direct_rows": loaded}, "; ".join(notes) or "up to date"


def pull_form4_today(now, bq):
    """Today's Form 4s from the live feed, so the evening Filing Sweep sees same-day insider buys (2026-10-22)."""
    import form4_scan as f4
    from zoneinfo import ZoneInfo
    day = now.astimezone(ZoneInfo("America/New_York")).date() if now.tzinfo else \
        now.replace(tzinfo=dt.timezone.utc).astimezone(ZoneInfo("America/New_York")).date()
    if day.weekday() >= 5: return {"_direct_rows": 0}, "weekend"
    seen = f4.seen_today(bq, PROJECT, day)
    docs = []
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    rows = f4.scan_today(day, seen, log=lambda *_: None, docs=docs)
    if rows: f4.load_rows(bq, PROJECT, rows, stamp, table=f4.TABLE_TODAY)
    if docs:   # the ownership XML, saved on first sight so filing_docs.py need not fetch it again (b201)
        for d in docs: d["fetched_at"] = stamp
        filing_docs.load(bq, PROJECT, docs)
    ps = sum(r["code"] in ("P", "S") for r in rows)
    return {"_direct_rows": ps}, f"{day}: {len({r['accession'] for r in rows})} new filings, {ps} P/S lines ({len(seen)} checked earlier)"


def snap_insider_filings(bq):
    """Save rrp.insider_filings_live (the Form 4 screen with buy_type; ~3.2 GB to compute) as a table after each new
    Form 4 day loads, so the tasks read it for megabytes. rrp.insider_filings is a view over this table (2026-10-19)."""
    sql = (f"CREATE OR REPLACE TABLE `{PROJECT}.sec.insider_filings_snap` "
           "PARTITION BY DATE_TRUNC(filed, MONTH) CLUSTER BY ticker, code "
           "OPTIONS (description = 'Nightly copy of rrp.insider_filings_live, rebuilt by rrp-feeds after each Form 4 load') "
           f"AS SELECT *, CURRENT_TIMESTAMP() AS snapped_at FROM `{PROJECT}.rrp.insider_filings_live`")
    t0 = time.time()
    bq.query(sql).result()
    return f"insider_filings_snap rebuilt in {round(time.time() - t0)}s"


# ------------------------------------------------------------------ CFTC COT
# Every market in both CFTC futures-only reports (was 7 contracts until 2026-10-23): financial = Traders in Financial
# Futures (currencies, rates, equity indexes, BTC / ETH), disaggregated = physical commodities (energy incl. natural gas
# 023651 and WTI 067651, metals, grains, softs, livestock). New reports land Friday ~3:30 PM ET for the Tuesday before.
COT = {"financial": "gpe5-46if", "disaggregated": "72hh-3qpy"}
COT_FIELDS = ["open_interest_all", "dealer_positions_long_all", "dealer_positions_short_all", "asset_mgr_positions_long",
              "asset_mgr_positions_short", "lev_money_positions_long", "lev_money_positions_short", "m_money_positions_long_all",
              "m_money_positions_short_all", "prod_merc_positions_long", "prod_merc_positions_short", "swap_positions_long_all",
              "swap__positions_short_all", "nonrept_positions_long_all", "nonrept_positions_short_all"]


def _cot_pages(res, since):
    """Every row of one COT report dataset with report date >= since, 50,000 per page."""
    out, offset = [], 0
    while True:
        r = get(f"https://publicreporting.cftc.gov/resource/{res}.json",
                params={"$where": f"report_date_as_yyyy_mm_dd >= '{since}T00:00:00'", "$order": "report_date_as_yyyy_mm_dd, id",
                        "$limit": 50000, "$offset": offset})
        page = r.json() if r is not None else []
        out += page; offset += len(page)
        if len(page) < 50000: return out


def pull_cot(now):
    """Normal runs: every market's reports from the last 21 days. --cot-history: everything since 2006 (TFF starts 2006,
    disaggregated 2006), one ops.feeds_runs row; --backfill keeps its old meaning (since 2018)."""
    rows = []
    since = "2006-01-01" if "--cot-history" in ARGS else "2018-01-01" if "--backfill" in ARGS else \
        (now - dt.timedelta(days=21)).date().isoformat()
    for report, res in COT.items():
        for x in _cot_pages(res, since):
            row = dict(report=report, id=x.get("id"), report_date=(x.get("report_date_as_yyyy_mm_dd") or "")[:10],
                       market_code=x.get("cftc_contract_market_code"), market_name=x.get("market_and_exchange_names"),
                       raw_json=json.dumps(x, sort_keys=True), ingested_at=now.strftime("%Y-%m-%d %H:%M:%S"))
            for f in COT_FIELDS: row[f.replace("__", "_")] = fnum(x.get(f))
            rows.append(row)
    return {"cot": rows}, f"{len(rows)} rows"


# ------------------------------------------------------------------ USAspending DoD awards
def pull_dod(now):
    """Normal runs: last 180 days (USAspending posts DoD awards ~90 days late; 180 leaves three months to pick up a late
    batch or a revision, was 120 until 2026-10-27, b195). --backfill: 2 years in 30-day windows (the API pages up to 100
    rows and a single 2-year query hits the page cap). The $100M filter is on the AWARD amount: every transaction on such
    an award is kept, including small modifications and negative de-obligations."""
    fields = ["Award ID", "Mod", "Recipient Name", "Action Date", "Transaction Amount", "Awarding Sub Agency",
              "Transaction Description", "generated_internal_id", "Recipient UEI"]
    span = 730 if "--backfill" in ARGS else 180
    step = 30 if "--backfill" in ARGS else 60
    top, floor = now.date(), now.date() - dt.timedelta(days=span)
    if DOD_RANGE:   # gap fill: --dod-range START END in 10-day windows
        floor, top = DOD_RANGE; step = 10
    rows, capped = [], []
    w_end = top
    while w_end >= floor:
        w_start = max(w_end - dt.timedelta(days=step - 1), floor)
        page = 1
        while True:
            body = {"filters": {"agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Defense"}],
                                "time_period": [{"start_date": w_start.isoformat(), "end_date": w_end.isoformat()}],
                                "award_type_codes": ["A", "B", "C", "D"], "award_amounts": [{"lower_bound": 100000000}]},
                    "fields": fields, "limit": 100, "page": page, "sort": "Action Date", "order": "desc"}
            d = None
            for i in range(4):
                try:
                    r = S.post("https://api.usaspending.gov/api/v2/search/spending_by_transaction/", json=body, timeout=90)
                    if r.status_code >= 500: time.sleep(5 * (i + 1)); continue
                    r.raise_for_status(); d = r.json(); break
                except requests.RequestException:
                    time.sleep(5 * (i + 1))
            if d is None:
                capped.append(f"{w_start.isoformat()} p{page} (server error)"); break
            for x in d.get("results", []):
                gid = x.get("generated_internal_id")
                rows.append(dict(award_id=x.get("Award ID"), mod=str(x.get("Mod")) if x.get("Mod") is not None else None,
                                 recipient=x.get("Recipient Name"), recipient_uei=x.get("Recipient UEI"), action_date=x.get("Action Date"),
                                 amount_usd=fnum(x.get("Transaction Amount")), sub_agency=x.get("Awarding Sub Agency"),
                                 description=x.get("Transaction Description"), generated_internal_id=gid,
                                 is_new_award=str(x.get("Mod")) in ("0", "None", ""),
                                 url=f"https://www.usaspending.gov/award/{gid}" if gid else None,
                                 ingested_at=now.strftime("%Y-%m-%d %H:%M:%S")))
            if not (d.get("page_metadata") or {}).get("hasNext"): break
            page += 1; time.sleep(0.3)
            if page > 100: capped.append(w_start.isoformat()); break
        w_end = w_start - dt.timedelta(days=1)
        if DOD_RANGE: time.sleep(2)
    note = f"{len(rows)} transactions {floor} to {top}"
    if capped: note += f"; incomplete windows: {', '.join(capped)}"
    return {"dod_awards": rows}, note


# ------------------------------------------------------------------ war.gov contracts RSS
def pull_war_rss(now):
    r = S.get("https://www.war.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=400&Site=945&max=30",
              headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RRP Technologies research bot"}, timeout=30)
    r.raise_for_status()
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.content)
    rows = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip(); link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        try: pub_ts = dt.datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z").strftime("%Y-%m-%d %H:%M:%S")
        except ValueError: pub_ts = None
        m = re.search(r"Contracts for (\w+)\.? (\d{1,2}), (\d{4})", title)
        cdate = None
        if m:
            mon = m.group(1)[:3].title()
            try: cdate = dt.datetime.strptime(f"{mon} {m.group(2)} {m.group(3)}", "%b %d %Y").date().isoformat()
            except ValueError: cdate = None
        rows.append(dict(link=link, title=title, contract_date=cdate, published=pub_ts, ingested_at=now.strftime("%Y-%m-%d %H:%M:%S")))
    return {"war_contract_days": rows}, f"{len(rows)} items"


# ------------------------------------------------------------------ EDGAR daily-index counts (completeness check, b191)
def pull_edgar_index(now, days=None):
    """What EDGAR itself published: every form type's filing count in each daily form index of the last `days` calendar
    days (default 14; --edgar-index-days N for history) -> ops.edgar_index_counts (MERGE on day + form, so a re-run or a
    late-posted index just updates). filings = distinct accessions (the index lists a filing once per CIK); lines = index
    lines. rrp.edgar_completeness compares these with what each sec.* table loaded. Added 2026-10-27."""
    if days is None:
        days = int(sys.argv[sys.argv.index("--edgar-index-days") + 1]) if "--edgar-index-days" in sys.argv else 14
    rows, idx, seen = [], [], 0
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    for i in range(1, days + 1):
        day = (now - dt.timedelta(days=i)).date()
        if day.weekday() >= 5: continue
        try:   # a holiday (or an index not posted yet) has no file; sec.gov answers 403 as well as 404
            r = get(f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{(day.month - 1) // 3 + 1}/form.{day:%Y%m%d}.idx", sec=True)
        except Exception:
            r = None
        if r is None: continue
        seen += 1
        accs, lines = {}, {}
        for line in r.text.splitlines():
            m = sec_reference.IDX_LINE.match(line)
            if not m: continue
            form = m.group(1).strip()
            accs.setdefault(form, set()).add(m.group(5).rsplit("/", 1)[-1].replace(".txt", ""))
            lines[form] = lines.get(form, 0) + 1
            row = index_row(m, "daily", stamp)
            if row: idx.append(row)
        rows += [dict(day=day.isoformat(), form=f, filings=len(a), lines=lines[f], fetched_at=stamp) for f, a in accs.items()]
    return ({"edgar_index": rows, "filing_index": idx},
            f"{seen} daily indexes, {sum(r['filings'] for r in rows)} filings, {len(rows)} day-forms, {len(idx)} index lines")


def index_row(m, source, stamp):
    """One line of an EDGAR form index (daily or quarterly) -> a sec.filing_index row: one row per filing and CIK (the
    index lists a filing once under each filer / subject). b192, added 2026-10-29."""
    path = m.group(5)
    accession = path.rsplit("/", 1)[-1].replace(".txt", "")
    acc18 = accession.replace("-", "")
    if len(acc18) != 18 or not acc18.isdigit(): return None
    d = m.group(4)
    if len(d) == 8: d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return dict(accession=accession, acc18=acc18, form=m.group(1).strip(), company=m.group(2).strip(), cik=str(int(m.group(3))),
                date_filed=d, filename=path, source=source, ingested_at=stamp)


def pull_filing_index_range(now, bq, first, last, chunk=100000):
    """b192 backfill: EDGAR's quarterly full form index (every form type, every filer, 1993 on) -> sec.filing_index,
    MERGEd on (acc18, cik) in chunks so memory stays flat and a re-run only updates. One ops.feeds_runs row per quarter
    (source filing_index_history). `first` / `last` like 1994Q1 / 2026Q4; the current quarter's file is rebuilt nightly
    by EDGAR, so it can be re-run safely."""
    def quarters(a, b):
        y, q = int(a[:4]), int(a[-1])
        while (y, q) <= (int(b[:4]), int(b[-1])):
            yield y, q
            y, q = (y + 1, 1) if q == 4 else (y, q + 1)
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    total = 0
    for y, q in quarters(first, last):
        t0 = time.time(); n = 0; err = None
        try:
            wait = 0.125 - (time.time() - _last[0])
            if wait > 0: time.sleep(wait)
            _last[0] = time.time()
            r = S.get(f"https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.idx", stream=True, timeout=120)
            if r.status_code == 404: raise RuntimeError("no index file")
            r.raise_for_status()
            batch = []
            for raw in r.iter_lines(decode_unicode=False):
                m = sec_reference.IDX_LINE.match(raw.decode("latin-1"))
                if not m: continue
                row = index_row(m, "quarterly", stamp)
                if row: batch.append(row)
                if len(batch) >= chunk:
                    n += write(bq, "filing_index", batch) if bq is not None else len(batch); batch = []
            if batch: n += write(bq, "filing_index", batch) if bq is not None else len(batch)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:500]
        secs = round(time.time() - t0, 1); total += n
        print(f"filing index {y}Q{q}: {'ok' if err is None else 'FAILED ' + err} ({n} lines, {secs}s)", flush=True)
        if bq is not None:
            try: write(bq, "runs", [dict(ts=stamp, source="filing_index_history", ok=err is None, row_count=n, seconds=secs,
                                         note=f"{y}Q{q}", error=err)])
            except Exception as e: print(f"health write FAILED: {e}")
    return total


# ------------------------------------------------------------------ BigQuery
F, I, T, D, B, St = "FLOAT", "INTEGER", "TIMESTAMP", "DATE", "BOOLEAN", "STRING"
SCHEMAS = {
    "efts_hits": [("query_id", St), ("phrase", St), ("forms", St), ("doc_id", St), ("accession", St), ("filename", St),
                  ("file_date", D), ("form", St), ("root_forms", St), ("file_type", St), ("file_description", St), ("ciks", St),
                  ("display_names", St), ("biz_locations", St), ("inc_states", St), ("sics", St), ("items", St),
                  ("period_ending", St), ("index_url", St), ("doc_url", St), ("first_seen", T), ("last_seen", T)],
    "cot": [("report", St), ("id", St), ("report_date", D), ("market_code", St), ("market_name", St)] +
           [(f.replace("__", "_"), F) for f in COT_FIELDS] + [("raw_json", St), ("ingested_at", T)],
    "dod_awards": [("award_id", St), ("mod", St), ("recipient", St), ("recipient_uei", St), ("action_date", D), ("amount_usd", F),
                   ("sub_agency", St), ("description", St), ("generated_internal_id", St), ("is_new_award", B), ("url", St),
                   ("ingested_at", T)],
    "war_contract_days": [("link", St), ("title", St), ("contract_date", D), ("published", T), ("ingested_at", T)],
    "edgar_index": [("day", D), ("form", St), ("filings", I), ("lines", I), ("fetched_at", T)],
    "filing_index": [("accession", St), ("acc18", St), ("form", St), ("company", St), ("cik", St), ("date_filed", D),
                     ("filename", St), ("source", St), ("ingested_at", T)],
    "runs": [("ts", T), ("source", St), ("ok", B), ("row_count", I), ("seconds", F), ("note", St), ("error", St)],
}
MERGE_KEYS = {"efts_hits": ["query_id", "doc_id"], "cot": ["id"], "dod_awards": ["generated_internal_id", "mod", "action_date", "amount_usd"],
              "war_contract_days": ["link"], "edgar_index": ["day", "form"], "filing_index": ["acc18", "cik"]}
KEEP_ON_UPDATE = {"efts_hits": ["first_seen"]}   # never overwrite when the row already exists
PARTITION = {"efts_hits": ("file_date", "MONTH"), "cot": ("report_date", "YEAR"), "dod_awards": ("action_date", "MONTH"),
             "edgar_index": ("day", "YEAR"), "runs": ("ts", "DAY"), "filing_index": ("date_filed", "MONTH")}
CLUSTER = {"efts_hits": ["query_id", "form"], "cot": ["market_code"], "runs": ["source"], "filing_index": ["form", "cik"]}


def bq_schema(name):
    from google.cloud import bigquery
    return [bigquery.SchemaField(c, t) for c, t in SCHEMAS[name]]


def write(bq, table, rows):
    from google.cloud import bigquery
    if not rows: return 0
    cols = [c for c, _ in SCHEMAS[table]]
    rows = [{c: r.get(c) for c in cols} for r in rows]
    # dedupe inside the batch on the merge key (EFTS pages can repeat a document)
    if table in MERGE_KEYS:
        seen, uniq = set(), []
        for r in rows:
            k = tuple(r.get(c) for c in MERGE_KEYS[table])
            if k not in seen: seen.add(k); uniq.append(r)
        rows = uniq
    cfg = bigquery.LoadJobConfig(schema=bq_schema(table), source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    if table not in MERGE_KEYS:
        cfg.write_disposition = "WRITE_APPEND"
        bq.load_table_from_json(rows, f"{PROJECT}.{TABLES[table]}", job_config=cfg).result(); return len(rows)
    stage = f"{PROJECT}.staging.feeds_{table}"
    # a fresh table each time: staging tables expire a day after they are CREATED and WRITE_TRUNCATE keeps the old expiry,
    # so a once-a-day load can hit the expiry mid-run (cot, Oct 28 11:05 UTC: "Destination deleted/expired during operation")
    bq.delete_table(stage, not_found_ok=True)
    cfg.write_disposition = "WRITE_TRUNCATE"
    bq.load_table_from_json(rows, stage, job_config=cfg).result()
    keys = MERGE_KEYS[table]
    on = " AND ".join(f"t.{k} IS NOT DISTINCT FROM s.{k}" for k in keys)
    if table == "cot":   # id is never NULL: plain equality hash-joins (IS NOT DISTINCT FROM forces a nested loop), and
        # the target is pruned to the batch's report-date span (every market since 2006 is ~400k rows)
        on = "t.id = s.id"
        ds = [r["report_date"] for r in rows if r.get("report_date")]
        if ds and len(ds) == len(rows): on += f" AND t.report_date BETWEEN DATE '{min(ds)}' AND DATE '{max(ds)}'"
    if table == "filing_index":   # keys never NULL: plain equality, target pruned to the batch's filing-date span
        ds = [r["date_filed"] for r in rows]
        on = f"t.acc18 = s.acc18 AND t.cik = s.cik AND t.date_filed BETWEEN DATE '{min(ds)}' AND DATE '{max(ds)}'"
    if table == "efts_hits":   # a document's file_date never changes: prune the target to the batch's date span
        ds = [r["file_date"] for r in rows if r.get("file_date")]
        if ds and len(ds) == len(rows): on += f" AND t.file_date BETWEEN DATE '{min(ds)}' AND DATE '{max(ds)}'"
    upd = ", ".join(f"{c} = s.{c}" for c in cols if c not in keys and c not in KEEP_ON_UPDATE.get(table, []))
    bq.query(f"""MERGE `{PROJECT}.{TABLES[table]}` t USING `{stage}` s ON {on}
                 WHEN MATCHED THEN UPDATE SET {upd}
                 WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})""").result()
    return len(rows)


def _ref(pulled, bq):
    """Write a sec_reference pull with its own schemas; returns (out, note) in the shape run() expects."""
    out, note = pulled
    if bq is None: return {t: rows for t, rows in out.items()}, note
    return {"_direct_rows": sum(sec_reference.write(bq, t, rows) for t, rows in out.items())}, note


def run():
    now = now_utc(); dry = "--dry" in ARGS
    bq = None
    if not dry:
        from google.cloud import bigquery
        bq = bigquery.Client(project=PROJECT)
    first_of_day = now.hour == 11 or "--backfill" in ARGS or "--all" in ARGS   # 7 AM ET: slow-moving sources
    # tender / 13D / redemption extraction runs at 08:05 UTC (4:05 AM EDT, 3:05 AM EST) so yesterday's filings are in
    # sec.*_terms before the early-morning reads (was the 7 AM run until 2026-10-23)
    extract_run = now.hour == 8 or "--backfill" in ARGS or "--all" in ARGS or "--extract" in ARGS
    # funds (Form D / 13F / 144 / 13G / ADV), tickers and delistings also run at 08:05 UTC (2026-10-26): the daily index
    # posts overnight, so yesterday's filings are loaded before the early-morning reads (was 11:05 UTC)
    early_daily = now.hour == 8 or "--backfill" in ARGS or "--all" in ARGS
    # sources run only when they can have something new (b202, 2026-10-26): the Form 4 daily index posts overnight
    # (runs 04:05-13:05 UTC until the day loads; each run after that is a no-op); CFTC posts COT Fridays ~3:30 PM ET
    # (Friday 19:05-23:05 UTC, plus the 7 AM ET run every day to catch holiday-shifted releases and revisions); war.gov
    # posts one contracts page each business day around 5 PM ET (11:05 and 23:05 UTC).
    forced = "--backfill" in ARGS or "--all" in ARGS
    form4_run = forced or "--snap" in ARGS or 4 <= now.hour <= 13
    cot_run = forced or first_of_day or (now.weekday() == 4 and now.hour >= 19)
    war_run = forced or first_of_day or now.hour == 23
    if FILING_INDEX_RANGE:
        n = pull_filing_index_range(now, None if dry else bq, *FILING_INDEX_RANGE)
        print(f"filing index: {n} lines"); return
    if EFTS_RANGE:
        if "--efts-count" in ARGS: efts_count(*EFTS_RANGE)
        else: pull_efts_range(None if dry else bq, *EFTS_RANGE)
        return
    if REDEMPTION_RANGE:
        if bq is None: print("--redemption-terms-range needs BigQuery (drop --dry)"); return
        n = redemption_terms.run_range(bq, PROJECT, *REDEMPTION_RANGE)
        print(f"redemption terms: {n} filings extracted"); return
    if "--cusip-figi" in ARGS:   # py main.py --cusip-figi --cusip-max 20000  (OpenFIGI backfill)
        if bq is None: print("--cusip-figi needs BigQuery (drop --dry)"); return
        t0 = time.time(); err = None; n = 0; note = None
        try: n, note = cusip_figi.run(bq, PROJECT, CUSIP_MAX)
        except Exception as e: err = f"{type(e).__name__}: {e}"[:500]
        write(bq, "runs", [dict(ts=now.strftime("%Y-%m-%d %H:%M:%S"), source="cusip_figi", ok=err is None, row_count=n,
                                seconds=round(time.time() - t0, 2), note=note or "", error=err)])
        print(f"cusip_figi: {'ok' if err is None else 'FAILED ' + err} ({n} rows) {note or ''}"); return
    if STAKE_RANGE:
        if bq is None: print("--stake-terms-range needs BigQuery (drop --dry)"); return
        n = stake_terms.run_range(bq, PROJECT, *STAKE_RANGE)
        print(f"stake terms: {n} filings extracted"); return
    if TENDER_RANGE:
        if bq is None: print("--tender-terms-range needs BigQuery (drop --dry)"); return
        n = tender_terms.run_range(bq, PROJECT, *TENDER_RANGE)
        print(f"tender terms: {n} filings extracted"); return
    if DOCS_RANGE:
        if bq is None: print("--docs-range needs BigQuery (drop --dry)"); return
        def log(stream, year, ok, n, note, err, secs):
            print(f"{note}: {'ok' if ok else 'FAILED ' + err} ({secs}s)", flush=True)
            try: write(bq, "runs", [dict(ts=now.strftime("%Y-%m-%d %H:%M:%S"), source="docs_history", ok=ok, row_count=n,
                                         seconds=secs, note=note, error=err)])
            except Exception as e: print(f"health write FAILED: {e}")
        filing_docs.pull_docs_range(now, bq, PROJECT, get, *DOCS_RANGE, only=ONLY, log=log)
        return
    if "--docs-only" in ARGS:
        jobs = [("docs", lambda: filing_docs.pull_docs(now, bq, PROJECT, get, days=DOCS_DAYS,
                                                       budget=3000 if DOCS_DAYS > 3 else 900))] if bq is not None else []
    elif DOD_RANGE:
        jobs = [("dod", lambda: pull_dod(now))]
    elif "--only-cot" in ARGS:
        jobs = [("cot", lambda: pull_cot(now))]
    elif "--edgar-index-only" in ARGS:   # py main.py --edgar-index-only --edgar-index-days 400  (history for the check)
        jobs = [("edgar_index", lambda: pull_edgar_index(now))]
    else:
        jobs = [("efts", lambda: pull_efts(now, days=30 if "--backfill" in ARGS else 3))]
    if not DOD_RANGE and "--docs-only" not in ARGS and "--only-cot" not in ARGS and "--edgar-index-only" not in ARGS:
        if bq is not None and form4_run: jobs.append(("form4", lambda: pull_form4(now, bq)))
        if bq is not None: jobs.append(("form4_today", lambda: pull_form4_today(now, bq)))
        if cot_run: jobs.append(("cot", lambda: pull_cot(now)))
        if first_of_day: jobs.append(("dod", lambda: pull_dod(now)))
        if war_run: jobs.append(("war_rss", lambda: pull_war_rss(now)))
        if early_daily or "--reference" in ARGS:
            jobs.append(("tickers", lambda: _ref(sec_reference.pull_tickers(now, get), bq)))
            jobs.append(("delist", lambda: _ref(sec_reference.pull_delistings(now, get), bq)))
        if (first_of_day and now.weekday() == 0) or "--reference" in ARGS:
            jobs.append(("xbrl", lambda: sec_reference.pull_xbrl_frames(now, get, bq)))
        if early_daily or "--funds" in ARGS:
            jobs.append(("funds", lambda: sec_funds.pull_daily(now, get, bq)))
        if (early_daily or "--funds" in ARGS) and bq is not None:   # after funds (new 13F lines), b190
            jobs.append(("cusip_figi", lambda: ({"_direct_rows": cusip_figi.run(bq, PROJECT, CUSIP_MAX)[0]}, "OpenFIGI CUSIP lookups")))
        if early_daily or first_of_day or "--edgar-index" in ARGS:   # what EDGAR published, for rrp.edgar_completeness
            jobs.append(("edgar_index", lambda: pull_edgar_index(now)))
        if bq is not None: jobs.append(("docs", lambda: filing_docs.pull_docs(now, bq, PROJECT, get, days=DOCS_DAYS)))
        if bq is not None and extract_run:   # after docs, so the newest tender text is there
            jobs.append(("tender_terms", lambda: ({"_direct_rows": tender_terms.run_range(
                bq, PROJECT, now.date() - dt.timedelta(days=14), now.date(), log=lambda *_: None)}, "last 14 days")))
            jobs.append(("stake_terms", lambda: ({"_direct_rows": stake_terms.run_range(
                bq, PROJECT, now.date() - dt.timedelta(days=14), now.date(), log=lambda *_: None)}, "last 14 days")))
            jobs.append(("redemption_terms", lambda: ({"_direct_rows": redemption_terms.run_range(
                bq, PROJECT, now.date() - dt.timedelta(days=14), now.date(), log=lambda *_: None)}, "last 14 days")))
    if SUBJECT == "sec": jobs = [j for j in jobs if j[0] not in GOV_SOURCES]
    elif SUBJECT == "gov": jobs = [j for j in jobs if j[0] in GOV_SOURCES]
    health = []
    for name, fn in jobs:
        t0 = time.time(); n = 0; err = None; note = None
        try:
            out, note = fn()
            for table, rows in out.items():
                if table == "_direct_rows": n += rows; continue
                n += len(rows) if dry else write(bq, table, rows)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:500]
        health.append(dict(ts=now.strftime("%Y-%m-%d %H:%M:%S"), source=name, ok=err is None, row_count=n,
                           seconds=round(time.time() - t0, 2), note=(note or "")[:1000], error=err))
        print(f"{name}: {'ok' if err is None else 'FAILED ' + err} ({n} rows, {health[-1]['seconds']}s) {note or ''}", flush=True)
        if not dry:   # one row per source as soon as it finishes, so a run killed later (timeout) still leaves its trail
            try: write(bq, "runs", health[-1:])
            except Exception as e: print(f"health write FAILED: {e}")
    print(f"feeds ingest done: {sum(h['ok'] for h in health)}/{len(health)} sources ok")


def setup():
    """Create any missing table (partitioned and clustered). Datasets come from warehouse/reorg.py."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    bq = bigquery.Client(project=PROJECT)
    for name, loc in TABLES.items():
        ref = f"{PROJECT}.{loc}"
        try: bq.get_table(ref); print(f"  {loc} exists"); continue
        except NotFound: pass
        t = bigquery.Table(ref, schema=bq_schema(name))
        if name in PARTITION:
            col, unit = PARTITION[name]
            t.time_partitioning = bigquery.TimePartitioning(type_=getattr(bigquery.TimePartitioningType, unit), field=col)
        if name in CLUSTER: t.clustering_fields = CLUSTER[name]
        bq.create_table(t); print(f"  created {loc}")
    filing_docs.setup(bq, PROJECT)
    tender_terms.setup(bq, PROJECT)
    stake_terms.setup(bq, PROJECT)
    redemption_terms.setup(bq, PROJECT)
    cusip_figi.setup(bq, PROJECT)
    sec_reference.setup(bq, PROJECT)
    sec_funds.setup(bq, PROJECT)
    import form4_scan as f4
    try: bq.get_table(f"{PROJECT}.{f4.TABLE}"); print(f"  {f4.TABLE} exists")
    except NotFound:
        t = bigquery.Table(f"{PROJECT}.{f4.TABLE}", schema=[bigquery.SchemaField(c, ty) for c, ty in f4.SCHEMA])
        t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field="filed")
        t.clustering_fields = ["ticker", "code"]
        bq.create_table(t); print(f"  created {f4.TABLE}")
    try: bq.get_table(f"{PROJECT}.{f4.TABLE_TODAY}"); print(f"  {f4.TABLE_TODAY} exists")
    except NotFound:   # today's Form 4s from the live feed; partitions expire after 4 days (the daily index takes over)
        t = bigquery.Table(f"{PROJECT}.{f4.TABLE_TODAY}", schema=[bigquery.SchemaField(c, ty) for c, ty in f4.SCHEMA])
        t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="filed",
                                                        expiration_ms=4 * 86400000)
        t.clustering_fields = ["code", "ticker"]
        bq.create_table(t); print(f"  created {f4.TABLE_TODAY}")
    print("setup done")


if __name__ == "__main__":
    try:
        setup() if "--setup" in ARGS else run()
    except Exception as e:
        print(f"feeds FAILED: {e}")
    sys.exit(0)
