"""Filing text -> BigQuery `sec.filing_docs` (added 2026-10-10).

Downstream reads used to open every filing on www.sec.gov one at a time.
This step stores the text instead, so the tasks read it with one query:
  * every filing in sec.filing_hits (tenders, 13Ds, 8-Ks of every stream) except the Form 4 stream:
    the EDGAR index page is read, then the main document and the useful exhibits (offer to purchase,
    press releases and other EX-99, merger agreements) are fetched and turned into plain text;
  * every Form 4 with an open-market purchase or sale in sec.form4: the ownership XML,
    stored as filed (the tasks read the 10b5-1 box, holdings after and every footnote from it).
One row per document, plus one INDEX row per filing listing every document in it (seq, type,
description, name, size), which also marks the filing as done. A filing whose index cannot be read gets
no row and is tried again next run.

Called from main.py (python main.py runs it every hour; --docs-only runs nothing else;
--docs-days N reaches back N days instead of 3, for the first fill).
"""
import datetime as dt, html, re, time
from html.parser import HTMLParser

TABLE = "sec.filing_docs"
MAX_CHARS = 500_000          # longer documents are cut and flagged (merger agreements can run to 2M characters)
MAX_EXHIBITS = 5
EXHIBIT_KEEP = re.compile(r"^EX-(99|\(A\)|2\b|2\.)", re.I)          # EX-99.*, EX-99.(A)(1)(A), EX-(a)(1)(A), EX-2.1
EXHIBIT_FIRST = re.compile(r"\(A\)\s*\(1\)\s*\(A\)|\(A\)\(1\)\(I\)|99\.1$|99\.\(A\)\(1\)", re.I)
TEXT_EXT = re.compile(r"\.(htm|html|txt|xml)$", re.I)
# credit agreements, indentures and note forms a stream matched only on a phrase: not stored when the filing has a
# main document (34,658 such exhibits were deleted on 2026-10-26; the extractors read the main document)
MATCHED_SKIP = re.compile(r"^EX-(10|4)\b", re.I)
# history fill (--docs-range): main document + up to 2 exhibits, offer to purchase / press releases only
HIST_EXHIBITS = 2
EXHIBIT_KEEP_HIST = re.compile(r"^EX-(99|\(A\))", re.I)
HIST_ORDER = ["tender_issuer", "tender_third_party", "activist_13d", "activist_13d_old_code", "redemption_notice",
              "redemption_partial", "bankruptcy_ch11", "bankruptcy_receivership", "crypto_bitcoin", "crypto_ethereum",
              "crypto_solana", "crypto_digital_asset", "formd_digital_assets", "minerals_dpa", "minerals_critical",
              "minerals_lpo", "minerals_osc", "minerals_trs", "rights_offering", "merger_agreement"]

SCHEMA = [("accession", "STRING"), ("acc18", "STRING"), ("form", "STRING"), ("file_date", "DATE"), ("cik", "STRING"),
          ("seq", "INTEGER"), ("doc_type", "STRING"), ("description", "STRING"), ("doc_name", "STRING"), ("url", "STRING"),
          ("size_bytes", "INTEGER"), ("chars", "INTEGER"), ("truncated", "BOOLEAN"), ("text", "STRING"),
          ("source", "STRING"), ("fetched_at", "TIMESTAMP")]

class _Text(HTMLParser):
    BLOCK = {"p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table", "section", "article", "hr",
             "title", "center", "blockquote", "pre", "dd", "dt", "ul", "ol"}
    SKIP = {"script", "style", "head", "ix:header"}
    VOID = {"br", "img", "hr", "meta", "link", "input", "col", "area", "base", "wbr", "source", "param", "embed"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.out, self.stack, self.skip = [], [], 0

    def handle_starttag(self, tag, attrs):
        if tag in self.BLOCK: self.out.append("\n")
        elif tag in ("td", "th"): self.out.append(" | ")
        if tag in self.VOID: return
        style = (dict(attrs).get("style") or "").replace(" ", "").lower()
        flag = tag in self.SKIP or "display:none" in style
        self.stack.append((tag, flag))
        if flag: self.skip += 1

    def handle_endtag(self, tag):
        if tag in self.BLOCK: self.out.append("\n")
        if tag in self.VOID or not any(t == tag for t, _ in self.stack): return
        while self.stack:
            t, flag = self.stack.pop()
            if flag: self.skip = max(0, self.skip - 1)
            if t == tag: break

    def handle_startendtag(self, tag, attrs):
        if tag in self.BLOCK: self.out.append("\n")

    def handle_data(self, data):
        if not self.skip: self.out.append(data)


def html_to_text(raw):
    p = _Text()
    try:
        p.feed(raw); p.close()
        txt = "".join(p.out)
    except Exception:
        txt = re.sub(r"<[^>]+>", " ", raw)
    txt = html.unescape(txt).replace("\xa0", " ").replace("\u200b", "")
    txt = re.sub(r"[ \t\r\f\v]+", " ", txt)
    lines = []
    for l in txt.split("\n"):
        l = re.sub(r"( ?\| ?)+", " | ", l).strip(" |").strip()
        lines.append(l)
    txt = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", txt).strip()


def parse_index(page):
    """Rows of the 'Document Format Files' table: (seq, description, href, doc_name, doc_type, size)."""
    m = re.search(r'<table class="tableFile" summary="Document Format Files">(.*?)</table>', page, re.S)
    if not m: return []
    rows = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", m.group(1), re.S):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        if len(tds) < 5: continue
        a = re.search(r'href="([^"]+)"[^>]*>([^<]*)</a>', tds[2])
        if not a: continue
        href = a.group(1).replace("/ix?doc=", "")
        seq = re.sub(r"\D", "", tds[0])
        rows.append(dict(seq=int(seq) if seq else None, description=html.unescape(re.sub(r"<[^>]+>", "", tds[1])).strip(),
                         href="https://www.sec.gov" + href if href.startswith("/") else href, doc_name=a.group(2).strip(),
                         doc_type=html.unescape(re.sub(r"<[^>]+>", "", tds[3])).strip(),
                         size=int(re.sub(r"\D", "", tds[4]) or 0)))
    return rows


def pick(rows, form, matched=(), lean=False):
    """Main document first, then up to MAX_EXHIBITS useful exhibits (offer to purchase and EX-99.1 ahead of the rest),
    plus any document a stream matched (doc_url), except EX-10 / EX-4 exhibits (agreements, indentures, note forms that
    matched on a phrase): those are stored only when they are the filing's main document.
    lean (history fill): at most HIST_EXHIBITS exhibits and no merger agreements (EX-2.x), which are most of the bytes."""
    docs = [r for r in rows if TEXT_EXT.search(r["doc_name"])]
    if not docs: return []
    main = next((r for r in docs if r["doc_type"].upper() == (form or "").upper()), docs[0])
    keep = EXHIBIT_KEEP_HIST if lean else EXHIBIT_KEEP
    ex = [r for r in docs if r is not main and keep.search(r["doc_type"])]
    ex.sort(key=lambda r: (0 if EXHIBIT_FIRST.search(r["doc_type"]) else 1, r["seq"] or 999))
    out = [main] + ex[:HIST_EXHIBITS if lean else MAX_EXHIBITS]
    want = {m.lower() for m in matched if m}
    out += [r for r in docs if r["doc_name"].lower() in want and r not in out and not MATCHED_SKIP.search(r["doc_type"])]
    return out


def decode(r):
    """EDGAR documents rarely declare a charset; most are UTF-8, older ones Windows-1252."""
    b = r.content
    for enc in ("utf-8", "cp1252"):
        try: return b.decode(enc)
        except UnicodeDecodeError: pass
    return b.decode("latin-1")


def _row(base, **kw):
    r = dict(base); r.update(kw); return r


def fetch_filing(get, acc, cik, form, file_date, stamp, matched=(), lean=False):
    acc18 = acc.replace("-", "")
    dashed = f"{acc18[:10]}-{acc18[10:12]}-{acc18[12:]}"
    base = dict(accession=dashed, acc18=acc18, form=form, file_date=file_date, cik=str(int(cik)),
                source="filing_hits_history" if lean else "filing_hits", fetched_at=stamp)
    idx_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc18}/{dashed}-index.htm"
    r = get(idx_url, sec=True)
    if r is None: return []
    rows = parse_index(decode(r))
    if not rows: return []
    listing = "\n".join(f"{x['seq']} | {x['doc_type']} | {x['description']} | {x['doc_name']} | {x['size']}" for x in rows)
    out = [_row(base, seq=0, doc_type="INDEX", description="document list", doc_name="-index.htm", url=idx_url,
                size_bytes=None, chars=len(listing), truncated=False, text=listing)]
    for d in pick(rows, form, matched, lean):
        try:
            rr = get(d["href"], sec=True)
        except Exception:
            rr = None
        if rr is None: continue
        raw = decode(rr)
        txt = raw if d["doc_name"].lower().endswith(".txt") else html_to_text(raw)
        out.append(_row(base, seq=d["seq"], doc_type=d["doc_type"], description=d["description"], doc_name=d["doc_name"],
                        url=d["href"], size_bytes=d["size"], chars=len(txt), truncated=len(txt) > MAX_CHARS, text=txt[:MAX_CHARS]))
    return out


def fetch_form4(get, acc, cik, file_date, stamp):
    acc18 = acc.replace("-", "")
    dashed = f"{acc18[:10]}-{acc18[10:12]}-{acc18[12:]}"
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{dashed}.txt"
    r = get(url, sec=True)
    if r is None: return []
    sub = decode(r)
    m = re.search(r"(<ownershipDocument>.*?</ownershipDocument>)", sub, re.S)
    if not m: return []
    xml = "\n".join(l.rstrip() for l in m.group(1).splitlines() if l.strip())
    form = (re.search(r"CONFORMED SUBMISSION TYPE:\s*(\S+)", sub) or [None, "4"])[1]
    return [dict(accession=dashed, acc18=acc18, form=form, file_date=file_date, cik=str(int(cik)), seq=1, doc_type="FORM4_XML",
                 description="Form 4 ownership XML", doc_name=f"{dashed}.txt", url=url, size_bytes=len(r.content), chars=len(xml),
                 truncated=False, text=xml, source="insider_form4", fetched_at=stamp)]


def pending(bq, P, days, cap=600):
    q = f"""
    WITH done AS (SELECT DISTINCT acc18 FROM `{P}.{TABLE}` WHERE file_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days + 7} DAY)),
    hits AS (
      SELECT REPLACE(accession, '-', '') AS acc18, ANY_VALUE(form) AS form, MIN(file_date) AS file_date,
             ANY_VALUE(REGEXP_EXTRACT(index_url, r'/data/(\\d+)/')) AS cik, 'hits' AS kind,
             ARRAY_AGG(DISTINCT REGEXP_EXTRACT(doc_url, r'/([^/?#]+)$') IGNORE NULLS) AS matched
      FROM `{P}.rrp.sec_filings`
      WHERE file_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY) AND stream != 'form4_open_market' AND index_url IS NOT NULL
      GROUP BY 1),
    f4 AS (
      SELECT accession AS acc18, ANY_VALUE(form) AS form, MIN(filed) AS file_date,
             ANY_VALUE(NULLIF(issuer_cik, '')) AS cik, 'form4' AS kind, ARRAY<STRING>[] AS matched
      FROM `{P}.sec.form4`
      WHERE filed >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY) AND NULLIF(issuer_cik, '') IS NOT NULL
      GROUP BY 1)
    SELECT * FROM (SELECT * FROM hits UNION ALL SELECT * FROM f4)
    WHERE acc18 NOT IN (SELECT acc18 FROM done) AND LENGTH(acc18) = 18 AND cik IS NOT NULL
    ORDER BY kind = 'form4', file_date DESC
    LIMIT {cap}"""
    return list(bq.query(q).result())


def load(bq, P, rows):
    from google.cloud import bigquery
    if not rows: return 0
    cfg = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(c, t) for c, t in SCHEMA],
                                 source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition="WRITE_APPEND")
    for r in rows:
        if r.get("file_date") is not None: r["file_date"] = str(r["file_date"])
    bq.load_table_from_json(rows, f"{P}.{TABLE}", job_config=cfg).result()
    return len(rows)


def pull_docs(now, bq, P, get, days=3, budget=900):
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    todo = pending(bq, P, days, cap=6000 if days > 3 else 600)
    t0, batch, n_fil, n_rows, missed = time.time(), [], 0, 0, 0
    for x in todo:
        if time.time() - t0 > budget: break
        try:
            rows = (fetch_form4(get, x.acc18, x.cik, x.file_date, stamp) if x.kind == "form4"
                    else fetch_filing(get, x.acc18, x.cik, x.form, x.file_date, stamp, x.matched))
        except Exception:
            rows = []
        if not rows: missed += 1; continue
        batch += rows; n_fil += 1
        if sum(len(r["text"] or "") for r in batch) > 20_000_000:   # keep load jobs well under the 100 MB request limit
            n_rows += load(bq, P, batch); batch = []
    n_rows += load(bq, P, batch)
    left = len(todo) - n_fil - missed
    return {"_direct_rows": n_rows}, f"{n_fil} filings stored ({n_rows} documents); {missed} not readable; {max(left, 0)} left for the next run; window {days} days"


def pending_range(bq, P, start, end, stream):
    """History fill: filings of one stream filed start..end that have no text yet (any stream's earlier fetch counts)."""
    from google.cloud import bigquery
    q = f"""
    WITH done AS (SELECT DISTINCT acc18 FROM `{P}.{TABLE}` WHERE file_date BETWEEN @a AND @b)
    SELECT REPLACE(accession, '-', '') AS acc18, ANY_VALUE(form) AS form, MIN(file_date) AS file_date,
           ANY_VALUE(REGEXP_EXTRACT(index_url, r'/data/(\\d+)/')) AS cik,
           ARRAY_AGG(DISTINCT REGEXP_EXTRACT(doc_url, r'/([^/?#]+)$') IGNORE NULLS) AS matched
    FROM `{P}.rrp.sec_filings`
    WHERE file_date BETWEEN @a AND @b AND stream = @s AND index_url IS NOT NULL
    GROUP BY 1
    HAVING acc18 NOT IN (SELECT acc18 FROM done) AND LENGTH(acc18) = 18 AND cik IS NOT NULL
    ORDER BY file_date"""
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("a", "DATE", start),
                                                    bigquery.ScalarQueryParameter("b", "DATE", end),
                                                    bigquery.ScalarQueryParameter("s", "STRING", stream)])
    return list(bq.query(q, job_config=cfg).result())


def pull_docs_range(now, bq, P, get, start, end, only=None, log=print):
    """History fill (--docs-range START END [--only streams]): stream by stream in HIST_ORDER (tenders, 13Ds and
    redemptions first: the extraction bricks read them), year by year. Lean pick: main document + up to 2 exhibits.
    Resumes by itself: a filing that already has text is never fetched again. Stops a stream-year after 50 misses
    in a row (EDGAR blocking or down) so a bad stretch does not burn through the list. log(stream, year, ok, n, note)."""
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    for stream in [s for s in HIST_ORDER if only is None or s in only]:
        for y in range(start.year, end.year + 1):
            a, b = max(start, dt.date(y, 1, 1)), min(end, dt.date(y, 12, 31))
            t0, err, batch, n_fil, n_rows, missed, streak = time.time(), None, [], 0, 0, 0, 0
            try:
                todo = pending_range(bq, P, a, b, stream)
                if not todo: continue
                for x in todo:
                    try:
                        rows = fetch_filing(get, x.acc18, x.cik, x.form, x.file_date, stamp, x.matched, lean=True)
                    except Exception:
                        rows = []
                    if not rows:
                        missed += 1; streak += 1
                        if streak >= 50: raise RuntimeError(f"50 filings in a row not readable (last {x.acc18})")
                        continue
                    streak = 0; batch += rows; n_fil += 1
                    if sum(len(r["text"] or "") for r in batch) > 20_000_000:
                        n_rows += load(bq, P, batch); batch = []
                n_rows += load(bq, P, batch); batch = []
            except Exception as e:
                err = f"{type(e).__name__}: {e}"[:500]
                try: n_rows += load(bq, P, batch)
                except Exception: pass
            log(stream, y, err is None, n_rows, f"{stream} {y}: {n_fil} filings ({n_rows} documents), {missed} not readable",
                err, round(time.time() - t0, 1))


def setup(bq, P):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    ref = f"{P}.{TABLE}"
    try:
        bq.get_table(ref); print(f"  {TABLE} exists")
    except NotFound:
        t = bigquery.Table(ref, schema=[bigquery.SchemaField(c, ty) for c, ty in SCHEMA])
        t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field="file_date")
        t.clustering_fields = ["acc18", "doc_type"]
        bq.create_table(t); print(f"  created {TABLE}")
