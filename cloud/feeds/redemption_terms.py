"""Redemptions and calls as columns -> BigQuery `sec.redemption_terms` (b172, added 2026-10-22).

Reads the filing text in sec.filing_docs for every 8-K the redemption streams hit (redemption_notice, redemption_partial)
and decides, with rules and no model, whether the filing ANNOUNCES a call of the issuer's own securities (is_call) -
most hits only describe how notes may be redeemed (new-issue terms, indentures). For calls it reads around the
announcing sentence: the security (e.g. "5.750% Senior Notes due 2027"), its kind (notes / preferred / warrants),
amount (principal dollars or units), full or partial, redemption date, price (% of principal or $ per unit),
make-whole language, and the sentence itself (call_text).
Built against 70 sample filings (2015-2026): of 40 random stream hits, 6 flagged as calls, 5 real; of 30 known notices,
22 flagged. Security and date are the reliable fields; amount can be the size of a series rather than the amount called.
One row per filing per extractor version (append-only); rrp.redemptions keeps the newest version's calls.

  python main.py --redemption-terms-range 2015-01-01 2026-10-22   # history, month by month (from the PC)
  the rrp-feeds job runs it once a day at 08:05 UTC (4:05 AM EDT), after filing text, over the last 14 days (extract_run)
"""
import re, datetime as dt

MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
DATE = rf"(?:{MONTHS})\.? *\d{{1,2}}, *\d{{4}}"
NUM = r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?"

ANN = re.compile(
    r"(?:(?:issued|delivered|gave|sent|send|deliver|caused[^.]{0,30}to be (?:issued|delivered|sent|given))[^.]{0,60}?"
    r"notices? of (?:full |partial |conditional |optional |its )?redemption"
    r"|(?:has|have|was|were|had) (?:been )?called[^.]{0,120}?for redemption|elected to redeem|exercised its (?:option|right) to redeem"
    r"|announc\w+[^.]{0,80}?(?:the |a |its )?(?:partial |full )?(?:redemption of|intention to redeem|plan to redeem|election to redeem|decision to redeem)"
    r"|(?:will|intends to|plans to|expects to) redeem (?:all|\$|the|its|approximately|[\d,]+)|to redeem all (?:of )?(?:its|the) (?:outstanding|remaining))", re.I)
COND = re.compile(r"\b(?:if|unless|before|prior to|may|might|would|could|does not|do not|not constitute|required|deemed|any|"
                  r"in the event|upon|whether|until|following certain|shall)\b[^.]{0,40}$", re.I)
SEC = re.compile(r"((?:\d+(?:\.\d+)?%(?! of)(?! per)|Floating Rate|Fixed[- ]to[- ]Floating Rate)[^.;:()“”\"]{3,140}?"
                 r"(?:Notes|Debentures|Bonds|Preferred Stock|Preferred Shares|Preferred Units|Preferred Securities|Capital Securities|"
                 r"Trust Preferred Securities|Depositary Shares|Senior Securities|Baby Bonds)(?: due (?:\w+ \d{1,2}, )?\d{4})?)", re.I)
WARR = re.compile(r"\b(?:public |outstanding |redeemable )*warrants\b", re.I)


def _flat(t):
    t = re.sub(r"\s+", " ", t or "").strip()
    return re.sub(r"\b([apAP])\.\s?([mM])\.", r"\1\2", t)


def _num(s, unit=None):
    try: v = float(s.replace(",", ""))
    except (AttributeError, ValueError): return None
    u = (unit or "").lower()
    return v * 1e6 if u.startswith("million") else v * 1e9 if u.startswith("billion") else v


def _date(s):
    if not s: return None
    s = re.sub(r"\s+", " ", s.replace(".", "")).strip()
    for f in ("%B %d, %Y", "%B %d,%Y"):
        try: return dt.datetime.strptime(s, f).date()
        except ValueError: pass
    return None


def announcements(f):
    out = []
    for m in ANN.finditer(f):
        if COND.search(f[max(0, m.start() - 70):m.start()]): continue
        if re.search(r"called \(or deemed called\)", f[m.start():m.end() + 40], re.I): continue
        ctx = f[max(0, m.start() - 250):m.end() + 250]
        if re.search(r"within one year under arrangements|including by (?:adopting|delivering)|deliver(?:ing)? notices of redemption or prepayment"
                     r"|in accordance with the (?:terms|indenture) (?:of|governing)[^.]{0,40}may", ctx, re.I): continue   # boilerplate
        if not re.search(DATE, f[max(0, m.start() - 300):m.end() + 600]): continue
        out.append(m)
    return out


def extract(form, docs, file_date=None):
    """docs: [(seq, doc_type, text)] for one 8-K -> dict. is_call = the filing announces a redemption or call of the
    issuer's own securities (not boilerplate about how notes may be redeemed); the rest is read around that sentence."""
    f = _flat(" ".join(d[2] for d in sorted(docs, key=lambda d: d[0] or 0) if (d[1] or "").upper() != "INDEX"))
    anns = announcements(f)
    out = dict(is_call=bool(anns), security=None, security_kind=None, amount_usd=None, amount_units=None, full_call=None,
               redemption_date=None, price_pct=None, price_per_unit=None, make_whole=None, call_text=None)
    if not anns: return out
    m = anns[0]
    win = f[max(0, m.start() - 400):m.end() + 1500]
    out["call_text"] = f[max(0, m.start() - 200):m.end() + 600][:1200]
    s = SEC.search(f[m.start():m.end() + 600]) or SEC.search(win)
    if s:
        out["security"] = s.group(1).strip()[:200]
    elif WARR.search(f[m.start():m.end() + 300]):
        out["security"] = "warrants"
    sec = (out["security"] or "").lower()
    out["security_kind"] = ("warrants" if "warrant" in sec else "preferred" if re.search(r"preferred|depositary|capital securities", sec)
                            else "notes" if re.search(r"notes|debentures|bonds|senior securities", sec) else None)
    a = re.search(rf"\$\s?({NUM})\s*(million|billion)?\s*(?:in |of )?(?:the )?(?:aggregate |outstanding |remaining )*(?:aggregate )?principal amount", win, re.I)
    if a: out["amount_usd"] = _num(a.group(1), a.group(2))
    u = re.search(rf"({NUM})\s*(?:million )?(?:of (?:its|the) )?(?:outstanding |issued and outstanding )?(?:shares|depositary shares|units|warrants)\b", win, re.I)
    if u and not a:
        v = _num(u.group(1)); out["amount_units"] = v * 1e6 if v and "million" in u.group(0).lower() else v
    if re.search(r"all (?:of )?(?:its |the |of its )?(?:issued and )?(?:outstanding|remaining)|in full\b|in whole\b", win, re.I): out["full_call"] = True
    elif re.search(r"partial|a portion of|in part\b", win, re.I): out["full_call"] = False
    d = (re.search(rf"(?:redemption date|redeemed on|redeem[^.]{{0,160}}?\bon)\W{{0,4}}(?:\w+day, )?({DATE})", win, re.I)
         or re.search(rf"\bon (?:\w+day, )?({DATE})[^.]{{0,40}}\((?:the )?[“\"]?Redemption Date", win, re.I))
    if d: out["redemption_date"] = _date(d.group(1))
    p = re.search(rf"redemption price (?:will be |is |shall be |of |equal to |equals )*(?:equal to )?({NUM})\s?% of", win, re.I) or \
        re.search(rf"at (?:a price of )?({NUM})\s?% of (?:the )?(?:aggregate )?(?:principal|liquidation|par)", win, re.I)
    if p: out["price_pct"] = _num(p.group(1))
    q = re.search(rf"(?:redemption price|at a price|price) of \$\s?({NUM}) per (?:share|warrant|depositary share|unit|preferred)", win, re.I) or \
        re.search(rf"\$\s?({NUM}) per (?:share|warrant|depositary share|unit)[^.]{{0,40}}(?:redemption|plus accrued)", win, re.I)
    if q: out["price_per_unit"] = _num(q.group(1))
    out["make_whole"] = bool(re.search(r"make[- ]whole|treasury rate plus|present values? of the remaining", win, re.I))
    if out["price_pct"] is not None and not 50 <= out["price_pct"] <= 150: out["price_pct"] = None
    return out


# ------------------------------------------------------------------ BigQuery
VERSION = 2   # v2 (2026-10-22): "called for redemption" only when something has been called; ABS/merger-covenant boilerplate skipped
TABLE = "sec.redemption_terms"
STREAMS = ("redemption_notice", "redemption_partial")
SCHEMA = [("acc18", "STRING"), ("accession", "STRING"), ("form", "STRING"), ("file_date", "DATE"), ("cik", "STRING"),
          ("filer", "STRING"), ("is_call", "BOOLEAN"), ("security", "STRING"), ("security_kind", "STRING"),
          ("amount_usd", "FLOAT"), ("amount_units", "FLOAT"), ("full_call", "BOOLEAN"), ("redemption_date", "DATE"),
          ("price_pct", "FLOAT"), ("price_per_unit", "FLOAT"), ("make_whole", "BOOLEAN"), ("call_text", "STRING"),
          ("extractor_version", "INTEGER"), ("extracted_at", "TIMESTAMP")]


def _months(start, end):
    d = start.replace(day=1)
    while d <= end:
        nxt = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        yield max(d, start), min(nxt - dt.timedelta(days=1), end); d = nxt


def run_range(bq, P, start, end, log=print):
    from google.cloud import bigquery
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cfg_load = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(c, t) for c, t in SCHEMA],
                                      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition="WRITE_APPEND")
    cols = [c for c, _ in SCHEMA]
    total = calls = 0
    for a, b in _months(start, end):
        q = f"""
        WITH hits AS (SELECT REPLACE(accession, '-', '') AS acc18, ANY_VALUE(filer) AS filer FROM `{P}.rrp.sec_filings`
                      WHERE file_date BETWEEN @a AND @b AND stream IN UNNEST(@s) GROUP BY 1),
        done AS (SELECT acc18 FROM `{P}.{TABLE}` WHERE file_date BETWEEN @a AND @b AND extractor_version = {VERSION})
        SELECT d.acc18, ANY_VALUE(d.accession) AS accession, ANY_VALUE(d.form) AS form, ANY_VALUE(d.file_date) AS file_date,
               ANY_VALUE(d.cik) AS cik, ANY_VALUE(h.filer) AS filer, ARRAY_AGG(STRUCT(d.seq, d.doc_type, d.text) ORDER BY d.seq) AS docs
        FROM `{P}.rrp.sec_docs` d JOIN hits h USING (acc18)
        WHERE d.file_date BETWEEN @a AND @b AND d.doc_type != 'INDEX' AND d.acc18 NOT IN (SELECT acc18 FROM done)
        GROUP BY d.acc18"""
        cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("a", "DATE", a),
                                                        bigquery.ScalarQueryParameter("b", "DATE", b),
                                                        bigquery.ArrayQueryParameter("s", "STRING", list(STREAMS))])
        rows = []
        for x in bq.query(q, job_config=cfg).result():
            try:
                r = extract(x.form, [(d["seq"], d["doc_type"], d["text"] or "") for d in x.docs])
            except Exception as e:
                log(f"  {x.acc18}: extract failed {type(e).__name__}: {e}"); continue
            r.update(acc18=x.acc18, accession=x.accession, form=x.form, file_date=str(x.file_date), cik=x.cik,
                     filer=(x.filer or "")[:300], extractor_version=VERSION, extracted_at=stamp)
            if r.get("redemption_date") is not None: r["redemption_date"] = str(r["redemption_date"])
            rows.append({c: r.get(c) for c in cols})
        for i in range(0, len(rows), 2000):
            bq.load_table_from_json(rows[i:i + 2000], f"{P}.{TABLE}", job_config=cfg_load).result()
        total += len(rows); calls += sum(1 for r in rows if r["is_call"])
        if rows: log(f"  redemption terms {a:%Y-%m}: {len(rows)} filings, {sum(1 for r in rows if r['is_call'])} calls")
    return total


def setup(bq, P):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    ref = f"{P}.{TABLE}"
    try:
        bq.get_table(ref); print(f"  {TABLE} exists")
    except NotFound:
        t = bigquery.Table(ref, schema=[bigquery.SchemaField(c, ty) for c, ty in SCHEMA])
        t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.YEAR, field="file_date")
        t.clustering_fields = ["is_call", "acc18"]
        bq.create_table(t); print(f"  created {TABLE}")
