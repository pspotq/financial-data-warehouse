"""Schedule 13D stakes as columns -> BigQuery `sec.stake_terms` (b171, added 2026-10-21).

Reads the filing text in sec.filing_docs (filing_docs.py) for every Schedule 13D and amendment (form codes SC 13D and,
from Dec 2024, SCHEDULE 13D) and pulls the cover page and Item 4 out with rules, no model:
  issuer, class, CUSIP, date of event, amendment number; every reporting person's name, source of funds (row 4),
  shares (row 11), percent of class (row 13) and type (row 14); the largest stake on the cover (pct, shares,
  top_filer); Item 4 "Purpose of Transaction" text (first 4,000 characters) and keyword flags on it: board_seat,
  proxy_fight, sale_or_merger, letter_to_board, cooperation_agreement, investment_only.
One row per filing per extractor version (append-only); rrp.stakes keeps the newest version, rrp.stake_changes adds
the previous filing's stake for the same issuer and lead filer.

  python main.py --stake-terms-range 2015-01-01 2026-10-21   # history, month by month (from the PC)
  the rrp-feeds job runs it once a day at 08:05 UTC (4:05 AM EDT), after filing text, over the last 14 days (extract_run)
"""
import re, datetime as dt

NUM = r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?"
MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"


def _flat(t):
    return re.sub(r"\s+", " ", t or "").strip()


def _num(s):
    try: return float(s.replace(",", ""))
    except (AttributeError, ValueError): return None


def _date(s):
    if not s: return None
    s = s.strip().replace(".", "")
    for f in ("%B %d, %Y", "%B %d,%Y", "%m/%d/%Y", "%b %d, %Y"):
        try: return dt.datetime.strptime(s, f).date()
        except ValueError: pass
    return None


def _before(f, label):
    m = re.search(r"([^()]{2,200}?)\s*\(\s*" + label, f, re.I)
    if not m: return None
    s = re.sub(r"^.*(?:Securities Exchange Act of 1934|Amendment No\.? ?\d+\)?|\(Rule 13d-\d+\)|SCHEDULE 13D(?:/A)?)\s*", "", m.group(1), flags=re.I)
    s = re.sub(r"^\(?(?:Amendment No\.? ?\d+)\)?\s*", "", s, flags=re.I)
    return s.strip(" _-–—:*|") or None


ROW = {  # row label -> pattern for its value
    "funds": r"SOURCE OF FUNDS(?: \(SEE INSTRUCTIONS\))?[:.]?\s*\|?\s*([A-Z]{2}(?:\s*[,/;]?\s*(?:and\s+)?[A-Z]{2})*)\b",
    "shares": rf"AGGREGATE AMOUNT BENEFICIALLY OWNED BY (?:EACH )?REPORTING PERSON[S]?(?: \(SEE INSTRUCTIONS\))?[:.]?\s*\|?\s*\(?\$?({NUM}|-0-|None|Zero)",
    "pct": rf"PERCENT OF CLASS REPRESENTED BY AMOUNT IN ROW \(?(?:11|9)\)?(?: \(SEE INSTRUCTIONS\))?[:.]?\s*\|?\s*(?:Approximately |approx\. )?({NUM}|-0-|0)\s*%?",
    "type": r"TYPE OF REPORTING PERSON(?: \(SEE INSTRUCTIONS\))?[:.]?\s*\|?\s*([A-Z]{2,3}(?:\s*[,/;]\s*[A-Z]{2,3})*)\b",
}


def persons(f):
    """Cover-page rows for every reporting person: name, funds (row 4), shares (row 11), pct (row 13), type (row 14)."""
    starts = [m for m in re.finditer(r"(?:\(1\)|\b1\.?)\s*\|?\s*NAMES? OF REPORTING PERSONS?\.?(?:\s*/?\s*(?:S\.S\.?\s*)?(?:OR\s*)?I\.?R\.?S\.?\s*IDENTIFICATION NOS?\.?\s*OF ABOVE PERSONS?(?:\s*\(ENTITIES ONLY\))?\.?)?[:.]?", f, re.I)]
    out = []
    for i, m in enumerate(starts):
        block = f[m.end(): starts[i + 1].start() if i + 1 < len(starts) else m.end() + 6000]
        nm = re.match(r"\s*\|?\s*([A-Za-z0-9].{1,119}?)\s*(?:I\.R\.S\.|I\.D\. No|S\.S\. or|Tax ID|(?:\(2\)|\b2\.?)\s*\|?\s*CHECK THE APPROPRIATE)", block, re.I)
        r = dict(name=nm.group(1).strip(" :|") if nm else None)
        for k, p in ROW.items():
            mm = re.search(p, block, re.I)
            v = mm.group(1) if mm else None
            if k in ("shares", "pct"):
                v = 0.0 if v and v.lower() in ("-0-", "none", "zero") else _num(v)
            elif v:
                v = re.sub(r"\s+", " ", v.upper()).strip()
            r[k] = v
        if r["pct"] is not None and r["pct"] > 100: r["pct"] = None
        out.append(r)
    return out


PURPOSE_FLAGS = {
    "board_seat": r"board (?:seat|representation|nominee)|nominat\w+ (?:\w+ ){0,4}(?:director|candidates?)|appoint\w* (?:\w+ ){0,3}to the board|designate (?:\w+ ){0,3}director",
    "proxy_fight": r"proxy (?:contest|fight|solicitation)|solicit proxies|withhold (?:votes|authority)|consent solicitation",
    "sale_or_merger": r"sale of the (?:company|issuer)|strategic alternatives|(?:proposal|offer) to acquire|take[- ]private|going[- ]private|business combination|merger (?:proposal|agreement)",
    "letter_to_board": r"(?:sent|delivered) (?:a )?letter to the (?:board|issuer|company)|letter (?:to|addressed to) the board",
    "cooperation_agreement": r"cooperation agreement|standstill agreement|settlement agreement",
    "investment_only": r"(?:solely )?for investment purposes|in the ordinary course of business and not with the purpose",
}


def purpose(f):
    m = re.search(r"Item\s*4\.?\s*\|?\s*\(?Purposes? of (?:the )?Transactions?\)?[:.]?(.*?)(?=Item\s*5\.?\s*\|?\s*\(?Interests? in Securities|$)", f, re.I)
    txt = m.group(1).strip(" .:|") if m else None
    flags = sorted(k for k, p in PURPOSE_FLAGS.items() if txt and re.search(p, txt, re.I))
    return (txt[:4000] if txt else None), flags


def extract(form, docs):
    """docs: [(seq, doc_type, text)] for one 13D filing -> dict of columns."""
    docs = sorted(docs, key=lambda d: d[0] or 0)
    # New-format SCHEDULE 13Ds (Dec 2024 on) store two seq-1 documents: primary_doc.html (the readable cover page) and
    # primary_doc.xml (tags stripped: values with no labels). Pick the one with the cover-page labels; before v3 the
    # first of the two was taken, so about half these filings came out blank.
    cands = [d for d in docs if (d[1] or "").upper().replace("SCHEDULE", "SC").startswith("SC 13D")]
    main = (next((d for d in cands if re.search(r"Name of Issuer", d[2] or "", re.I)), None)
            or max(cands, key=lambda d: len(d[2] or ""), default=None) or (docs[0] if docs else (0, "", "")))
    f = _flat(main[2])
    head = f[:15000]
    issuer = _before(head, r"Name of Issuer")
    cls = _before(head, r"Title of Class of Securities")
    cus = _before(head, r"CUSIP Number")
    cusip = (re.findall(r"\b[0-9A-Z]{6}[0-9A-Z]{2}[0-9]\b", (cus or "").upper()) or [None])[0]
    if cusip == "000000000": cusip = None
    ev = re.search(rf"((?:{MONTHS})\.? *\d{{1,2}}, *\d{{4}}|\d{{1,2}}/\d{{1,2}}/\d{{4}})\s*\(?\s*Date of Event", head, re.I)
    am = re.search(r"Amendment No\.?\s*:?\s*(\d+)", head, re.I)
    ps = persons(f)
    pcts = [p for p in ps if p["pct"] is not None]
    top = max(pcts, key=lambda p: (p["pct"], p["shares"] or 0)) if pcts else (ps[0] if ps else {})
    txt, flags = purpose(f)
    return dict(issuer=issuer, security_class=cls, cusip=cusip, event_date=_date(ev.group(1)) if ev else None,
                amendment_no=int(am.group(1)) if am and form.endswith("/A") else (0 if not form.endswith("/A") else None),
                lead_filer=ps[0]["name"] if ps else None, filers=len(ps),
                pct=top.get("pct"), shares=top.get("shares"), top_filer=top.get("name"),
                funds=",".join(sorted({x.strip() for p in ps for x in re.split(r"[ ,/;]+", p["funds"] or "") if re.fullmatch(r"[A-Z]{2}", x.strip())})) or None,
                filer_types=",".join(sorted({x for p in ps for x in re.split(r"[ ,/;]+", p["type"] or "") if x})) or None,
                purpose_flags=",".join(flags) or None, purpose_text=txt, persons=ps)


# ------------------------------------------------------------------ BigQuery
VERSION = 3   # v3 (2026-11-04): picks the HTML cover page over its tag-stripped XML twin (blank issuer / pct on ~half of SCHEDULE 13Ds)
# v2 was: v2 (2026-10-22): names behind "OR I.R.S. Identification No." labels (v1 read ~1,000 as "OR", "S.S. OR", ",")
TABLE = "sec.stake_terms"
PERSON = [("name", "STRING"), ("funds", "STRING"), ("shares", "FLOAT"), ("pct", "FLOAT"), ("type", "STRING")]
SCHEMA = [("acc18", "STRING"), ("accession", "STRING"), ("form", "STRING"), ("file_date", "DATE"), ("cik", "STRING"),
          ("issuer", "STRING"), ("security_class", "STRING"), ("cusip", "STRING"), ("event_date", "DATE"),
          ("amendment_no", "INTEGER"), ("lead_filer", "STRING"), ("filers", "INTEGER"), ("top_filer", "STRING"),
          ("pct", "FLOAT"), ("shares", "FLOAT"), ("funds", "STRING"), ("filer_types", "STRING"), ("purpose_flags", "STRING"),
          ("purpose_text", "STRING"), ("persons", PERSON), ("extractor_version", "INTEGER"), ("extracted_at", "TIMESTAMP")]


def _fields():
    from google.cloud import bigquery
    return [bigquery.SchemaField(c, "RECORD", mode="REPEATED", fields=[bigquery.SchemaField(a, b) for a, b in t])
            if isinstance(t, list) else bigquery.SchemaField(c, t) for c, t in SCHEMA]


def _months(start, end):
    d = start.replace(day=1)
    while d <= end:
        nxt = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        yield max(d, start), min(nxt - dt.timedelta(days=1), end); d = nxt


def run_range(bq, P, start, end, log=print):
    """Extract every 13D filed start..end that has text and no row at this VERSION, month by month."""
    from google.cloud import bigquery
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cfg_load = bigquery.LoadJobConfig(schema=_fields(), source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                      write_disposition="WRITE_APPEND")
    cols = [c for c, _ in SCHEMA]
    total = 0
    for a, b in _months(start, end):
        q = f"""
        WITH done AS (SELECT acc18 FROM `{P}.{TABLE}` WHERE file_date BETWEEN @a AND @b AND extractor_version = {VERSION})
        SELECT acc18, ANY_VALUE(accession) AS accession, ANY_VALUE(form) AS form, ANY_VALUE(file_date) AS file_date,
               ANY_VALUE(cik) AS cik, ARRAY_AGG(STRUCT(seq, doc_type, text) ORDER BY seq) AS docs
        FROM `{P}.rrp.sec_docs`
        WHERE file_date BETWEEN @a AND @b AND (STARTS_WITH(form, 'SC 13D') OR STARTS_WITH(form, 'SCHEDULE 13D'))
          AND (STARTS_WITH(doc_type, 'SC 13D') OR STARTS_WITH(doc_type, 'SCHEDULE 13D'))
          AND acc18 NOT IN (SELECT acc18 FROM done)
        GROUP BY acc18"""
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("a", "DATE", a),
                                                        bigquery.ScalarQueryParameter("b", "DATE", b)])
        rows = []
        for x in bq.query(q, job_config=cfg).result():
            try:
                r = extract(x.form, [(d["seq"], d["doc_type"], d["text"] or "") for d in x.docs])
            except Exception as e:
                log(f"  {x.acc18}: extract failed {type(e).__name__}: {e}"); continue
            r.update(acc18=x.acc18, accession=x.accession, form=x.form, file_date=str(x.file_date), cik=x.cik,
                     extractor_version=VERSION, extracted_at=stamp)
            if r.get("event_date") is not None: r["event_date"] = str(r["event_date"])
            rows.append({c: r.get(c) for c in cols})
        for i in range(0, len(rows), 2000):
            bq.load_table_from_json(rows[i:i + 2000], f"{P}.{TABLE}", job_config=cfg_load).result()
        total += len(rows)
        if rows: log(f"  stake terms {a:%Y-%m}: {len(rows)} filings")
    return total


def setup(bq, P):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    ref = f"{P}.{TABLE}"
    try:
        bq.get_table(ref); print(f"  {TABLE} exists")
    except NotFound:
        t = bigquery.Table(ref, schema=_fields())
        t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.YEAR, field="file_date")
        t.clustering_fields = ["cusip", "acc18"]
        bq.create_table(t); print(f"  created {TABLE}")
