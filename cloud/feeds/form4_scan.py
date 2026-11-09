"""Form 4 open-market scanner -> BigQuery `sec.form4`.

For one EDGAR business day: read the daily form index, fetch every Form 4 / 4/A, and keep each open-market
purchase (code P) and sale (code S) line with shares > 0, as filed: shares, price, value (sales negative),
holdings after, the 10b5-1 / private-placement / non-USD flags, footnotes, issuer CIK and security title.
A jointly filed Form 4 lists a trade once per filer and an amendment usually repeats the original's lines;
both are kept here and handled in the rrp.insider_trades view.

Used by the rrp-feeds job every hour (main.py -> pull_form4: yesterday and earlier, days not yet loaded) and by
form4_bulk.py for history. Test one day without BigQuery:
  py form4_scan.py --day 2026-09-23 --csv out.csv --limit 50

SAME DAY (added 2026-10-22): the daily index only posts overnight, so today's Form 4s come from EDGAR's live
"latest filings" feed instead (current_filings / scan_today), every hour, into sec.form4_today (kept 4 days).
Each filing checked gets one marker row (code '-', kind 'checked') so it is not fetched again. Test without BigQuery:
  py form4_scan.py --today --csv today.csv --limit 50

FETCH ONCE (2026-10-26, b201): each Form 4 used to be downloaded three times (live feed, daily index, filing text).
Now the first fetch also returns the ownership XML as a sec.filing_docs row (xml_doc), the daily-index pass copies the
lines of filings the live feed already read from sec.form4_today (from_today) and fetches only the rest, so
filing_docs.py finds the XML already stored. The daily index stays the checklist: nothing it lists is skipped.

SEC fair access: <= 8 requests/second with a descriptive User-Agent.
"""
import argparse, csv, datetime as dt, io, re, time
import xml.etree.ElementTree as ET
import requests

TABLE = "sec.form4"
TABLE_TODAY = "sec.form4_today"
UA = {"User-Agent": "RRP Technologies research bot (Form 4 open-market scanner)", "Accept-Encoding": "gzip, deflate"}
F, D, B, St, T = "FLOAT", "DATE", "BOOLEAN", "STRING", "TIMESTAMP"
SCHEMA = [("filed", D), ("tx_date", D), ("ticker", St), ("issuer", St), ("issuer_cik", St), ("insider", St), ("role", St),
          ("code", St), ("security", St), ("shares", F), ("price", F), ("value", F), ("owned_after", F),
          ("direct_indirect", St), ("plan_10b5_1", B), ("private_placement", B), ("non_usd_suspect", B),
          ("accession", St), ("form", St), ("footnotes", St), ("kind", St), ("source", St), ("loaded_at", T)]
COLS = [c for c, _ in SCHEMA]
NON_USD = re.compile(r"\b(BRL|reais|NIS|ILS|shekel|CAD|C\$|GBP|pence|EUR|euro|JPY|yen|HKD|INR|rupee|AUD|A\$|SEK|NOK|CHF|KRW|won|RMB|CNY|yuan)\b", re.I)
S = requests.Session(); S.headers.update(UA)
_last = [0.0]


def get(url, tries=5):
    for i in range(tries):
        wait = 0.125 - (time.time() - _last[0])
        if wait > 0: time.sleep(wait)
        _last[0] = time.time()
        try:
            r = S.get(url, timeout=30)
            if r.status_code == 404: return None
            if r.status_code in (429, 503): time.sleep(5 * (i + 1)); continue
            r.raise_for_status(); return r.text
        except requests.RequestException:
            time.sleep(2 * (i + 1))
    return None


def day_index(day):
    """[(edgar path, form)] for every Form 4 / 4/A in the day's form index; None if there is no index (weekend, holiday, not yet posted)."""
    q = (day.month - 1) // 3 + 1
    txt = get(f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{q}/form.{day:%Y%m%d}.idx")
    if not txt: return None
    out, seen = [], set()
    for line in txt.splitlines():
        form = re.split(r"\s{2,}", line.strip(), maxsplit=1)[0]   # the form column is 12 characters wide in 2024, 17 in 2026
        if form not in ("4", "4/A"): continue
        m = re.search(r"(edgar/data/\d+/([\d-]+)\.txt)\s*$", line)
        if not m or m.group(2) in seen: continue                # listed once per party; fetch once
        seen.add(m.group(2)); out.append((m.group(1), form))
    return out


def _t(el, path):
    x = el.find(path)
    return x.text.strip() if x is not None and x.text else ""


def num(s):
    try: return float(s)
    except (TypeError, ValueError): return None


def parse(sub, filed):
    """Rows for every P/S line of one Form 4 submission (full .txt)."""
    m = re.search(r"<XML>\s*(<\?xml.*?</ownershipDocument>)", sub, re.S) or re.search(r"(<ownershipDocument>.*?</ownershipDocument>)", sub, re.S)
    if not m: return []
    try: doc = ET.fromstring(re.sub(r"<\?xml[^>]*\?>", "", m.group(1)).strip())
    except ET.ParseError: return []
    acc = re.search(r"ACCESSION NUMBER:\s*([\d-]+)", sub)
    owners = doc.findall("reportingOwner")
    roles = []
    for o in owners[:1]:
        r = o.find("reportingOwnerRelationship")
        if r is None: continue
        if _t(r, "isDirector") in ("1", "true"): roles.append("Director")
        if _t(r, "isOfficer") in ("1", "true"): roles.append(_t(r, "officerTitle") or "Officer")
        if _t(r, "isTenPercentOwner") in ("1", "true"): roles.append("TenPercentOwner")
        if _t(r, "isOther") in ("1", "true"): roles.append(_t(r, "otherText") or "Other")
    notes = " ".join((f.text or "").strip() for f in doc.findall("footnotes/footnote"))
    private = bool(re.search(r"private placement|subscription agreement|registered direct", notes, re.I))
    base = dict(filed=filed, ticker=_t(doc, "issuer/issuerTradingSymbol").upper(), issuer=_t(doc, "issuer/issuerName"),
                issuer_cik=_t(doc, "issuer/issuerCik").lstrip("0"),
                insider="; ".join(_t(o, "reportingOwnerId/rptOwnerName") for o in owners), role=",".join(roles),
                plan_10b5_1=_t(doc, "aff10b5One") in ("1", "true") or bool(re.search(r"10b5-1", notes)),
                private_placement=private, non_usd_suspect=bool(NON_USD.search(notes)),
                accession=acc.group(1).replace("-", "") if acc else "", footnotes=notes[:500], source="edgar_daily_index")
    rows = []
    for tx in doc.findall("nonDerivativeTable/nonDerivativeTransaction"):
        code = _t(tx, "transactionCoding/transactionCode")
        if code not in ("P", "S"): continue
        sh = num(_t(tx, "transactionAmounts/transactionShares/value")) or 0.0
        if sh <= 0: continue
        px = num(_t(tx, "transactionAmounts/transactionPricePerShare/value"))
        rows.append(dict(base, tx_date=_t(tx, "transactionDate/value")[:10] or None, code=code,
                         security=_t(tx, "securityTitle/value")[:120], shares=sh, price=px,
                         value=round(sh * px * (-1 if code == "S" else 1), 2) if px is not None else None,
                         owned_after=num(_t(tx, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")),
                         direct_indirect=_t(tx, "ownershipNature/directOrIndirectOwnership/value") or "D",
                         kind="sale" if code == "S" else ("private_P" if private else "buy")))
    return rows


def xml_doc(sub, path, filed, rows):
    """The filing's ownership XML as a sec.filing_docs row (same shape as filing_docs.fetch_form4), for a filing with
    P/S lines; None otherwise. fetched_at is set by the caller at load time."""
    if not rows: return None
    m = re.search(r"(<ownershipDocument>.*?</ownershipDocument>)", sub, re.S)
    if not m: return None
    xml = "\n".join(l.rstrip() for l in m.group(1).splitlines() if l.strip())
    acc18 = rows[0]["accession"] or re.sub(r"\D", "", path.rsplit("/", 1)[-1])
    dashed = f"{acc18[:10]}-{acc18[10:12]}-{acc18[12:]}"
    form = (re.search(r"CONFORMED SUBMISSION TYPE:\s*(\S+)", sub) or [None, "4"])[1]
    url = f"https://www.sec.gov/Archives/{path}"
    return dict(accession=dashed, acc18=acc18, form=form, file_date=filed, cik=rows[0].get("issuer_cik") or None, seq=1,
                doc_type="FORM4_XML", description="Form 4 ownership XML", doc_name=path.rsplit("/", 1)[-1], url=url,
                size_bytes=len(sub.encode("utf-8")), chars=len(xml), truncated=False, text=xml, source="insider_form4")


def fetch(filings, filed, log=print, docs=None):
    """Fetch and parse [(edgar path, form)] for one filing day. docs (a list): also collect each filing's ownership XML."""
    rows = []
    for n, (path, form) in enumerate(filings, 1):
        sub = get(f"https://www.sec.gov/Archives/{path}")
        if sub:
            got = parse(sub, filed)
            for r in got:
                r["form"] = form; rows.append(r)
            if docs is not None:
                d = xml_doc(sub, path, filed, got)
                if d: docs.append(d)
        if n % 250 == 0: log(f"  {filed} {n}/{len(filings)} filings, {len(rows)} rows")
    return rows


def scan_day(day, limit=None):
    idx = day_index(day)
    if idx is None: return None
    rows = fetch(idx[:limit] if limit else idx, day.isoformat())
    print(f"{day}: {len(idx)} Form 4s -> {len(rows)} P/S rows", flush=True)
    return rows


CURRENT = ("https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&dateb=&owner=include"
           "&start={start}&count=100&output=atom")


def current_filings(day, seen=(), max_pages=40):
    """[(edgar path, form, accession)] for every Form 4 / 4/A the live feed shows as filed on `day` (newest first,
    each filing once), leaving out accessions in `seen`. The feed lists a filing once per party, 100 entries a page."""
    out, got = [], set(seen)
    for page in range(max_pages):
        xml = get(CURRENT.format(start=page * 100))
        if not xml: break
        entries = re.findall(r"<entry>(.*?)</entry>", xml, re.S)
        if not entries: break
        older = False
        for e in entries:
            term = re.search(r'term="([^"]+)"', e)
            href = re.search(r'href="https://www\.sec\.gov/Archives/(edgar/data/\d+)/\d+/([\d-]+)-index\.html?"', e)
            filed = re.search(r"Filed:(?:</b>|&lt;/b&gt;)?\s*(\d{4}-\d{2}-\d{2})", e)
            if not (term and href): continue
            if filed and filed.group(1) < day.isoformat(): older = True; continue
            if filed and filed.group(1) > day.isoformat(): continue
            if term.group(1) not in ("4", "4/A"): continue
            acc = href.group(2)
            if acc.replace("-", "") in got: continue
            got.add(acc.replace("-", "")); out.append((f"{href.group(1)}/{acc}.txt", term.group(1), acc.replace("-", "")))
        if older: break
    return out


def scan_today(day, seen=(), limit=None, log=print, docs=None):
    """Rows for today's Form 4s not yet checked: every P/S line, plus one marker row per filing checked.
    docs (a list): also collect the ownership XML of each filing with P/S lines (sec.filing_docs rows)."""
    todo = current_filings(day, seen)
    if limit: todo = todo[:limit]
    rows = []
    for path, form, acc in todo:
        sub = get(f"https://www.sec.gov/Archives/{path}")
        got = parse(sub, day.isoformat()) if sub else []
        for r in got: r.update(form=form, source="edgar_current")
        if docs is not None and sub:
            d = xml_doc(sub, path, day.isoformat(), got)
            if d: docs.append(d)
        rows += got or [dict(filed=day.isoformat(), accession=acc, form=form, code="-", kind="checked" if sub else "unread",
                             source="edgar_current")]
    log(f"{day}: {len(todo)} new Form 4s -> {sum(r['code'] in ('P', 'S') for r in rows)} P/S rows")
    return rows


def seen_today(bq, project, day):
    """Accessions already checked (P/S lines or a marker); 'unread' markers are retried."""
    q = (f"SELECT DISTINCT accession FROM `{project}.{TABLE_TODAY}` WHERE filed >= DATE '{day.isoformat()}' "
         "AND IFNULL(kind, '') != 'unread'")
    return {r.accession for r in bq.query(q).result()}


def load_rows(bq, project, rows, stamp, table=TABLE):
    """Append rows to sec.form4 (or sec.form4_today) (typed; empty values load as NULL)."""
    from google.cloud import bigquery
    for r in rows: r["loaded_at"] = stamp
    buf = io.StringIO(); csv.DictWriter(buf, COLS, extrasaction="ignore").writerows(rows)
    cfg = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, write_disposition="WRITE_APPEND",
                                 schema=[bigquery.SchemaField(c, t) for c, t in SCHEMA], allow_quoted_newlines=True)
    bq.load_table_from_file(io.BytesIO(buf.getvalue().encode("utf-8")), f"{project}.{table}", job_config=cfg).result()


def from_today(bq, project, day):
    """What the live feed already read for `day`: (P/S rows to copy into sec.form4, accessions it checked). 'unread'
    markers are not counted, so those filings are fetched from the daily index as before."""
    q = (f"SELECT * EXCEPT (loaded_at) FROM `{project}.{TABLE_TODAY}` WHERE filed = DATE '{day.isoformat()}' "
         "AND IFNULL(kind, '') != 'unread'")
    rows, seen = [], set()
    for r in bq.query(q).result():
        seen.add(r.accession)
        if r.code in ("P", "S"): rows.append(dict(r.items()))
    return rows, seen


def loaded_days(bq, project):
    return {r.d for r in bq.query(f"SELECT DISTINCT CAST(filed AS STRING) AS d FROM `{project}.{TABLE}`").result()}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--day"); ap.add_argument("--today", action="store_true")
    ap.add_argument("--csv", required=True); ap.add_argument("--limit", type=int)
    a = ap.parse_args()
    if a.today:
        from zoneinfo import ZoneInfo
        out = scan_today(dt.datetime.now(ZoneInfo("America/New_York")).date(), limit=a.limit)
    else:
        out = scan_day(dt.date.fromisoformat(a.day), a.limit) or []
    with open(a.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, COLS, extrasaction="ignore"); w.writeheader(); w.writerows(out)
    print(f"wrote {a.csv}")
