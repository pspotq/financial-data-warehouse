"""Form 4 history loader for `sec.form4` (run from the PC; not part of the Cloud Run job).

The SEC's quarterly Insider Transactions Data Sets (<quarter>_form345.zip) say which Form 4 / 4/A filings hold a
code P or S line (about 13% of all Form 4s). Only those filings are fetched from EDGAR and parsed with
form4_scan.parse(), so every row is exactly what the nightly scan stores. The data sets round prices and shares to
2 decimals, so they are used as the list of filings, never for values. A quarter whose data set is not published
yet falls back to the full daily-index scan (form4_scan.scan_day).

  py form4_bulk.py --since 2024-07-01 --until 2026-06-30            # load days not in sec.form4 yet
  py form4_bulk.py --since 2026-07-01 --until 2026-10-02 --refill   # re-pull days already loaded and replace them
  py form4_bulk.py --day 2024-07-01 --csv out.csv                   # one day to a CSV, no BigQuery

Safe to stop and re-run. With --refill a day's old rows are deleted only after its new rows are fetched, in the
same step as the load. Quarter zips are cached in ./_bulk (gitignored).
"""
import argparse, csv, datetime as dt, io, os, zipfile
from collections import defaultdict
import form4_scan as fs

PROJECT = os.environ.get("RRP_PROJECT", "rrp-technologies")
CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_bulk")
BASES = ["https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/",
         "https://www.sec.gov/files/datastandardsinnovation/data/insider-transactions-data-sets/"]
csv.field_size_limit(10**9)


def quarter(d):
    return f"{d.year}q{(d.month - 1) // 3 + 1}"


def quarter_zip(qtr):
    """Path of the cached data set, downloading it if needed; None when the SEC has not published it."""
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, f"{qtr}_form345.zip")
    if os.path.exists(path) and zipfile.is_zipfile(path): return path
    for base in BASES:
        r = fs.S.get(base + f"{qtr}_form345.zip", timeout=300)
        if r.status_code == 200 and r.content[:2] == b"PK":
            open(path, "wb").write(r.content); print(f"{qtr}: downloaded {len(r.content) / 1e6:.1f} MB", flush=True)
            return path
    return None


def _tsv(z, name):
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline=""),
                                  delimiter="\t", quoting=csv.QUOTE_NONE)


def ps_filings(path):
    """{filed date: [(edgar path, form)]} for Form 4 / 4/A filings with a P or S line with shares > 0."""
    z = zipfile.ZipFile(path)
    sub = {r["ACCESSION_NUMBER"]: r for r in _tsv(z, "SUBMISSION.tsv") if r["DOCUMENT_TYPE"] in ("4", "4/A")}
    hit = {tx["ACCESSION_NUMBER"] for tx in _tsv(z, "NONDERIV_TRANS.tsv")
           if tx["ACCESSION_NUMBER"] in sub and (tx["TRANS_CODE"] or "").strip() in ("P", "S") and (fs.num(tx["TRANS_SHARES"]) or 0) > 0}
    days = defaultdict(list)
    for acc in sorted(hit):
        s = sub[acc]
        filed = dt.datetime.strptime(s["FILING_DATE"].strip(), "%d-%b-%Y").date().isoformat()
        days[filed].append((f"edgar/data/{(s['ISSUERCIK'] or '').strip().lstrip('0')}/{acc}.txt", s["DOCUMENT_TYPE"]))
    return days


def day_rows(day, cache):
    """Rows for one day: through the quarter's data set when published, else the full index scan. None = no EDGAR day."""
    q = quarter(day)
    if q not in cache:
        path = quarter_zip(q)
        cache[q] = ps_filings(path) if path else None
        if cache[q] is None: print(f"{q}: data set not published yet - using the full daily index", flush=True)
    if cache[q] is None: return fs.scan_day(day)
    filings = cache[q].get(day.isoformat())
    if not filings: return None
    rows = fs.fetch(filings, day.isoformat())
    print(f"{day}: {len(filings)} P/S filings -> {len(rows)} rows", flush=True)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since"); ap.add_argument("--until"); ap.add_argument("--refill", action="store_true")
    ap.add_argument("--day"); ap.add_argument("--csv")
    a = ap.parse_args()
    cache = {}
    if a.day and a.csv:
        rows = day_rows(dt.date.fromisoformat(a.day), cache) or []
        with open(a.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fs.COLS, extrasaction="ignore"); w.writeheader(); w.writerows(rows)
        print(f"wrote {a.csv}"); return

    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    done = fs.loaded_days(bq, PROJECT)
    day, end = dt.date.fromisoformat(a.since), dt.date.fromisoformat(a.until)
    while day <= end:
        if day.weekday() < 5 and (a.refill or day.isoformat() not in done):
            rows = day_rows(day, cache)
            if rows:
                stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                if a.refill and day.isoformat() in done:
                    old = list(bq.query(f"SELECT COUNT(*) n FROM `{PROJECT}.{fs.TABLE}` WHERE filed = '{day}'").result())[0].n
                    bq.query(f"DELETE FROM `{PROJECT}.{fs.TABLE}` WHERE filed = '{day}'").result()
                    fs.load_rows(bq, PROJECT, rows, stamp)
                    print(f"{day}: replaced {old} rows with {len(rows)}", flush=True)
                else:
                    fs.load_rows(bq, PROJECT, rows, stamp); print(f"{day}: loaded {len(rows)} rows", flush=True)
        day += dt.timedelta(days=1)
    print("form4 bulk done")


if __name__ == "__main__":
    main()
