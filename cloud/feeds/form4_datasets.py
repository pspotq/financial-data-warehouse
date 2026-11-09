"""Fast Form 4 history loader for `sec.form4`, straight from the SEC's quarterly Insider Transactions Data Sets.

form4_bulk.py fetches every P/S filing from EDGAR and parses its XML (exact values, ~22 min per month of filings).
This loader reads the same facts from the quarterly <quarter>_form345.zip files instead (minutes per year): no
per-filing fetches. The data sets round shares and prices to 2 decimals, so rows are tagged source = 'sec_dataset'
(form4_bulk / the nightly scan write 'edgar_daily_index'). Use it for years form4_bulk has not loaded; days already in
sec.form4 are skipped, so it can run beside form4_bulk on a different date range.

  py form4_datasets.py --since 2006-01-01 --until 2020-12-31
  py form4_datasets.py --quarter 2015q2 --dry          # parse one quarter, print counts and 3 rows, write nothing

Columns match form4_scan.SCHEMA: roles from the first reporting owner, insider = every owner joined by "; ",
footnotes = all of the filing's footnotes (first 500 characters), plan_10b5_1 = the 10b5-1 box (data sets from 2023)
or a footnote naming 10b5-1, private_placement / non_usd_suspect by the same footnote tests as form4_scan.parse.
"""
import argparse, csv, datetime as dt, io, re, zipfile
from collections import defaultdict
import form4_scan as fs
from form4_bulk import quarter_zip, PROJECT

csv.field_size_limit(10**9)
PRIVATE = re.compile(r"private placement|subscription agreement|registered direct", re.I)


def _rows(z, name):
    if name not in z.namelist(): return
    with z.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace", newline=""),
                                  delimiter="\t", quoting=csv.QUOTE_NONE)


def _date(s):
    s = (s or "").strip()
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try: return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError: pass
    return None


def _g(r, *names):
    for n in names:
        v = r.get(n)
        if v not in (None, ""): return v.strip()
    return ""


def quarter_rows(path, skip_days=frozenset()):
    """sec.form4 rows for every P/S line (shares > 0) on a Form 4 / 4/A in one quarter's data set."""
    z = zipfile.ZipFile(path)
    sub = {}
    for r in _rows(z, "SUBMISSION.tsv"):
        if r.get("DOCUMENT_TYPE") not in ("4", "4/A"): continue
        filed = _date(r.get("FILING_DATE"))
        if not filed or filed in skip_days: continue
        sub[r["ACCESSION_NUMBER"]] = dict(filed=filed, form=r["DOCUMENT_TYPE"], ticker=_g(r, "ISSUERTRADINGSYMBOL").upper(),
                                          issuer=_g(r, "ISSUERNAME"), issuer_cik=_g(r, "ISSUERCIK").lstrip("0"),
                                          box=_g(r, "AFF10B5ONE").lower() in ("1", "true", "y", "yes"))
    tx = [t for t in _rows(z, "NONDERIV_TRANS.tsv")
          if t.get("ACCESSION_NUMBER") in sub and _g(t, "TRANS_CODE") in ("P", "S") and (fs.num(_g(t, "TRANS_SHARES")) or 0) > 0]
    want = {t["ACCESSION_NUMBER"] for t in tx}
    owners = defaultdict(list)
    for o in _rows(z, "REPORTINGOWNER.tsv"):
        if o.get("ACCESSION_NUMBER") in want: owners[o["ACCESSION_NUMBER"]].append(o)
    notes = defaultdict(list)
    for n in _rows(z, "FOOTNOTES.tsv"):
        if n.get("ACCESSION_NUMBER") in want: notes[n["ACCESSION_NUMBER"]].append(_g(n, "FOOTNOTE_TXT"))
    out = []
    for t in tx:
        acc = t["ACCESSION_NUMBER"]; s = sub[acc]; own = owners.get(acc, [])
        roles = []
        if own:
            rel = _g(own[0], "RPTOWNER_RELATIONSHIP").lower()
            if "director" in rel: roles.append("Director")
            if "officer" in rel: roles.append(_g(own[0], "RPTOWNER_TITLE") or "Officer")
            if "tenpercent" in rel.replace(" ", "").replace("%", "percent"): roles.append("TenPercentOwner")
            if "other" in rel: roles.append(_g(own[0], "RPTOWNER_TXT") or "Other")
        fn = " ".join(x for x in notes.get(acc, []) if x)
        code = _g(t, "TRANS_CODE"); sh = fs.num(_g(t, "TRANS_SHARES")) or 0.0; px = fs.num(_g(t, "TRANS_PRICEPERSHARE"))
        private = bool(PRIVATE.search(fn))
        out.append(dict(filed=s["filed"], tx_date=_date(_g(t, "TRANS_DATE")), ticker=s["ticker"], issuer=s["issuer"],
                        issuer_cik=s["issuer_cik"], insider="; ".join(_g(o, "RPTOWNERNAME") for o in own), role=",".join(roles),
                        code=code, security=_g(t, "SECURITY_TITLE")[:120], shares=sh, price=px,
                        value=round(sh * px * (-1 if code == "S" else 1), 2) if px is not None else None,
                        owned_after=fs.num(_g(t, "SHRS_OWND_FOLWNG_TRANS")),
                        direct_indirect=_g(t, "DIRECT_INDIRECT_OWNERSHIP") or "D",
                        plan_10b5_1=s["box"] or bool(re.search(r"10b5-1", fn)), private_placement=private,
                        non_usd_suspect=bool(fs.NON_USD.search(fn)), accession=acc.replace("-", ""), form=s["form"],
                        footnotes=fn[:500], kind="sale" if code == "S" else ("private_P" if private else "buy"),
                        source="sec_dataset"))
    return out


def quarters(since, until):
    y, q = since.year, (since.month - 1) // 3 + 1
    while (y, q) <= (until.year, (until.month - 1) // 3 + 1):
        yield f"{y}q{q}"
        q += 1
        if q == 5: y, q = y + 1, 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since"); ap.add_argument("--until"); ap.add_argument("--quarter"); ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()
    if a.quarter:
        path = quarter_zip(a.quarter)
        if not path: print(f"{a.quarter}: data set not available"); return
        rows = quarter_rows(path)
        print(f"{a.quarter}: {len(rows)} P/S rows from {len({r['accession'] for r in rows})} filings, "
              f"{len({r['filed'] for r in rows})} days, {sum(r['code'] == 'P' for r in rows)} buys")
        for r in rows[:3]: print(r)
        if a.dry: return
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    done = frozenset(fs.loaded_days(bq, PROJECT))
    since = dt.date.fromisoformat(a.since) if a.since else dt.date(1990, 1, 1)
    until = dt.date.fromisoformat(a.until) if a.until else dt.date(2100, 1, 1)
    total = 0
    for qtr in ([a.quarter] if a.quarter else quarters(since, until)):
        path = quarter_zip(qtr)
        if not path: print(f"{qtr}: data set not available - skipped", flush=True); continue
        rows = [r for r in quarter_rows(path, done) if since.isoformat() <= r["filed"] <= until.isoformat()]
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if rows and not a.dry:
            for i in range(0, len(rows), 50000): fs.load_rows(bq, PROJECT, rows[i:i + 50000], stamp)
        total += len(rows)
        print(f"{qtr}: {'counted' if a.dry else 'loaded'} {len(rows)} rows ({len({r['filed'] for r in rows})} days)", flush=True)
    print(f"form4 data sets done: {total:,} rows")


if __name__ == "__main__":
    main()
