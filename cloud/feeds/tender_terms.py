"""Tender offer terms as columns -> BigQuery `sec.tender_terms` (b170, added 2026-10-21).

Reads the filing text already stored in sec.filing_docs (filing_docs.py) for every Schedule TO (SC TO-I, SC TO-T and
their amendments) and pulls the terms out with rules, no model:
  originals   subject company, filer, class, CUSIP, offer kind, price (fixed, Dutch-auction range, or at NAV), size
              (shares, % of class, or dollars), expiration date, transaction value from the fee table
  amendments  shares tendered, shares accepted, final price, a new expiration date, whether it reports final results
One row per filing per extractor version (append-only). rrp.tender_terms keeps the newest version per filing; raising
VERSION and re-running re-extracts everything without touching older rows.

  python main.py --tender-terms-range 2015-01-01 2026-10-21   # history, month by month (from the PC)
  the rrp-feeds job runs it once a day at 08:05 UTC (4:05 AM EDT), after filing text, over the last 14 days (extract_run)
Accuracy (b173, 2026-10-21, 65 random filings checked by hand against the text, version 2):
  offer_kind ~93% right; price 25 of 27 right (every cash tender for common stock right; misses were a notes put and
  an exchange offer); expiration 39 of 40 right, found for 89%; final-results flag 20 of 20 amendments right.
  Size (max_shares, max_pct, max_amount_usd) is right only about half the time: treat it as a lead, not a fact.
  Debt, preferred, exchange and option offers carry no per-share price (offer_kind debt_or_preferred etc.).
"""
import re, datetime as dt

MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
DATE = rf"(?:{MONTHS})\.? *\d{{1,2}}, *\d{{4}}"
NUM = r"\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?"
MONEY = rf"\$ *({NUM})( *(?:million|billion))?"


def _flat(t):
    t = re.sub(r"\s+", " ", t or "").strip()
    return re.sub(r"\b([apAP])\.\s?([mM])\.", r"\1\2", t)   # "5:00 p.m., on May 1" -> "5:00 pm, on May 1": keeps [^.] spans whole


def _num(s, unit=None):
    if s is None: return None
    try: v = float(s.replace(",", ""))
    except ValueError: return None
    u = (unit or "").strip().lower()
    return v * 1e6 if u == "million" else v * 1e9 if u == "billion" else v


def _date(s):
    if not s: return None
    s = re.sub(r"\s+", " ", s.replace(".", "")).strip()
    for f in ("%B %d, %Y", "%B %d,%Y"):
        try: return dt.datetime.strptime(s, f).date()
        except ValueError: pass
    return None


def cover(t):
    """Cover page of the Schedule TO: names, class, CUSIP, transaction valuation."""
    f = _flat(t[:20000])
    # a name may carry a short parenthetical of its own ("FUND (TI 2), LLC"): allow one after a space, unless it is
    # itself a cover label ("(Name of Issuer)"); v2 stopped at any "(" and kept only ", LLC"
    name_part = r"(?:[^()]|(?<=\s)\((?!\s*(?:Name|Title|CUSIP|Date|Exact|Address|Amendment|Rule|Issuer|Offeror|Purchaser|Parent|Bidder|Filing))[^()]{1,25}\))"
    def before(label):
        m = re.search(r"(" + name_part + r"{2,200}?)\s*\(\s*" + label, f, re.I)
        return m.group(1).strip(" _-–—:") if m else None
    subj = before(r"Name of Subject Company") or before(r"Name of Issuer")
    filer = (before(r"Names? of Filing Persons?") or before(r"Names? of Person\(?s?\)? Filing") or before(r"Offeror")
             or before(r"Bidders?"))
    # "(Name of Subject Company (Issuer) and Filing Person (Offeror))": one name is both; v2 kept "and Filing Person"
    if filer and re.fullmatch(r"and Filing Persons?", filer.strip(), re.I): filer = subj
    cls = before(r"Title of Class of Securities")
    cusip = before(r"CUSIP Number")
    for k in ("subj", "filer", "cls", "cusip"):
        v = locals()[k]
        if v: locals()[k] = v
    if cusip: cusip = (re.findall(r"[0-9A-Z]{6}[0-9A-Z]{2}[0-9]", cusip.upper()) or [None])[0]
    tv = None
    m = re.search(r"Transaction Valuation\*?\(?\w?\)?:?\s*(?:Amount of Filing Fee\*{0,2}\(?\w?\)?:?\s*)?" + MONEY, f, re.I)
    if m: tv = _num(m.group(1), m.group(2))
    for name in ("subj", "filer", "cls"):
        pass
    return dict(subject_company=_clean_name(subj), filer=_clean_name(filer), security_class=_clean_name(cls),
                cusip=cusip, transaction_value_usd=tv)


def _clean_name(s):
    if not s: return None
    s = re.sub(r"^.*?(?:Securities Exchange Act of 1934|Amendment No\. ?\d+\)?|SCHEDULE TO(?:-[IT])?(?:/A)?)\s*", "", s, flags=re.I)
    s = re.sub(r"^(?:\(?(?:final )?amendment(?: no\.)? ?\d*\)?\s*)+", "", s, flags=re.I)
    s = re.sub(r"\s*\((?:Issuer|Offeror|Purchaser|Parent of Offeror|Bidders?)\)\s*", "; ", s, flags=re.I)
    s = s.strip(" ;,_-–—")
    return s[:200] or None


def terms(t):
    """Offer terms from the offer to purchase (or the cover document when there is no separate offer document)."""
    f = _flat(t[:150000])
    out = dict(price_per_share=None, price_low=None, price_high=None, at_nav=False, dutch_auction=False,
               max_shares=None, max_pct=None, max_amount_usd=None, expiration_date=None, consideration_note=None)
    # modified Dutch auction: "not greater than $X nor less than $Y" / "price not less than $Y nor greater than $X"
    m = (re.search(rf"not (?:greater|more) than {MONEY} (?:per [A-Za-z ]{{1,20}} )?nor less than {MONEY}", f, re.I)
         or (re.search(r"Dutch auction", f, re.I) and re.search(rf"range of {MONEY} to {MONEY}", f, re.I)))
    if m:
        a, b = _num(m.group(1)), _num(m.group(3))
        out.update(price_high=max(a, b), price_low=min(a, b), dutch_auction=True)
    if not m:
        m = re.search(rf"not less than {MONEY} (?:per [A-Za-z ]{{1,20}} )?nor (?:greater|more) than {MONEY}", f, re.I)
        if m:
            a, b = _num(m.group(1)), _num(m.group(3))
            out.update(price_high=max(a, b), price_low=min(a, b), dutch_auction=True)
    if re.search(r"modified [\"“]?Dutch auction", f, re.I): out["dutch_auction"] = True
    if re.search(r"(?:at a price|purchase price) (?:per [A-Za-z ]{1,20} )?equal to (?:the|their|its) net asset value|"
                 r"at (?:their|the|its) (?:respective )?net asset value|(?:equal|equivalent) to (?:the )?(?:NAV|net asset value) per", f, re.I):
        out["at_nav"] = True
    if out["price_low"] is None and not out["at_nav"]:
        m = re.search(rf"(?:for|at|of) {MONEY},? (?:net )?per (?:share|unit|ADS|depositary|common share|class [a-z] |ordinary)"
                      r"[^.]{0,80}?(?:net to the (?:seller|holder)|in cash|without interest)", f, re.I)
        m = m or re.search(rf"(?:offer price|purchase price) of {MONEY},? (?:net )?per", f, re.I)
        m = m or re.search(rf"(?:for|at|of) {MONEY},? (?:net )?per (?:share|unit)", f, re.I)
        if m: out["price_per_share"] = _num(m.group(1))
    # size: up to N shares / up to N% / up to $X
    m = re.search(rf"up to ({NUM})(?: million)? (?:shares|Shares|units|Units|of (?:its|the) (?:outstanding )?(?:shares|Shares|units|Units))", f)
    if m:
        v = _num(m.group(1)); out["max_shares"] = v * 1e6 if "million" in m.group(0) else v
    for m in re.finditer(rf"(?:up to|not (?:to )?exceed(?:ing)?|maximum of) ({NUM}) ?%(?: of (?:the|its|our|each)?)", f, re.I):
        near = f[max(0, m.start() - 200):m.end() + 200].lower()
        if re.search(r"additional|without (?:amending|extending)|13e-4\(f\)|in excess of", near): continue   # the 2% extra the rules allow
        out["max_pct"] = _num(m.group(1)); break
    m = re.search(rf"(?:up to|aggregate purchase price of (?:up to )?|not (?:to )?exceed(?:ing)?) {MONEY} (?:of|in) (?:aggregate|value|its|our|the)", f, re.I)
    if m: out["max_amount_usd"] = _num(m.group(1), m.group(2))
    # expiration
    WD = r"(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),? )?"
    m = (re.search(rf"\bexpire[sd]?\b[^;]{{0,160}}?\b(?:on|of) {WD}({DATE})", f, re.I)
         or re.search(rf"after ({DATE}),? or such other date to which (?:the|this) Offers? may be extended", f, re.I)
         or re.search(rf"Expiration Date[^.]{{0,60}}?({DATE})", f, re.I)
         or re.search(rf"(?:must be received|deadline)[^.]{{0,80}}?\bon {WD}({DATE})", f, re.I))
    if m: out["expiration_date"] = _date(m.group(1))
    # sanity: a percentage cap is a slice of the class (0-50%); share caps under 1,000 are footnote noise
    if out["max_pct"] is not None and not 0 < out["max_pct"] <= 50: out["max_pct"] = None
    if out["max_shares"] is not None and out["max_shares"] < 1000: out["max_shares"] = None
    if out["max_amount_usd"] is not None and out["max_amount_usd"] < 100000: out["max_amount_usd"] = None
    if out["price_low"] is not None and not out["dutch_auction"]: out.update(price_low=None, price_high=None)
    return out


def amendment(t):
    """What an amendment (SC TO-I/A, SC TO-T/A) adds: final count, acceptance, a new expiration, a new price."""
    f = _flat(t[:120000])
    out = dict(shares_tendered=None, shares_accepted=None, final_price=None, new_expiration_date=None, is_final=False)
    m = re.search(rf"(?:a total of |approximately |an aggregate of )?({NUM}) (?:Shares|shares|Units|units|ADSs)[^.;]{{0,120}}?(?:had been |were |was )?(?:validly |properly )tendered", f)
    if m: out["shares_tendered"] = _num(m.group(1))
    m = re.search(rf"accepted for (?:purchase|payment)[^.;]{{0,40}}?({NUM}) (?:Shares|shares|Units|units)", f) or \
        re.search(rf"({NUM}) (?:Shares|shares|Units|units)[^.;]{{0,60}}?(?:have been|were|was) accepted for (?:purchase|payment)", f)
    if m: out["shares_accepted"] = _num(m.group(1))
    m = re.search(rf"(?:final )?purchase price of {MONEY} per", f, re.I) if re.search(r"final (?:results|count|purchase price)", f, re.I) else None
    if m: out["final_price"] = _num(m.group(1))
    WD = r"(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),? )?"
    m = (re.search(rf"extend(?:ed|ing|s)? (?:the )?(?:expiration|Expiration|Offer|Tender Offer)[^.]{{0,200}}?(?:until|to)\b[^.]{{0,80}}?\b(?:on )?{WD}({DATE})", f, re.I)
         or re.search(rf"(?:Offer|Expiration Date|Expiration Time|Offer Period)[^.]{{0,60}}?(?:has been|is being|was|is|will be) extended"
                      rf"[^.]{{0,160}}?\b(?:on|until|to) {WD}({DATE})", f, re.I))
    if m: out["new_expiration_date"] = _date(m.group(1))
    # the cover page of every Schedule TO carries the box "final amendment reporting the results of the tender offer";
    # only a checked box means final (checked: x, ☒, ☑, [X], Wingdings x; unchecked: ☐, ¨, o, [ ], c)
    m = re.search(r"final amendment reporting the results of the tender offers?[:.]?\s*(\[\s*[xX_ ]?\s*\]|\S)", f[:30000], re.I)
    if m and not re.match(r"[A-Za-z]$", m.group(1)) or (m and m.group(1) in "xX"):
        out["is_final"] = bool(re.match(r"\[\s*[xX]\s*\]|[xX☒☑✓✔þý■\uf078\uf0fd\uf0fe]$", m.group(1)))
    elif re.search(r"(?:offer|tender offer) (?:expired|terminated)[^;]{0,200}?(?:validly tendered|were tendered|purchased all|accepted for (?:purchase|payment))", f[:40000], re.I):
        out["is_final"] = True   # no box glyph in the text: a filing that reports the expired offer's count is the results filing
    else:
        out["is_final"] = bool(re.search(r"final amendment|reports? the final results", f[:20000], re.I))
    return out


def classify(form, cov, trm, text):
    f = _flat(text[:150000]).lower()
    filer = (cov.get("filer") or "").lower()
    cls = (cov.get("security_class") or "").lower()
    # a class line naming common stock as well ("Common Stock ... Series A Convertible Preferred Stock") is an equity
    # offer for both: classify on the rest (v3 read such lines in full for the first time and called them debt)
    if re.search(r"\b(?:notes?|debentures?|bonds?|preferred|auction rate|auction-rate)\b", cls) \
            and not re.search(r"\bcommon (?:stock|shares)|\bordinary shares", cls):
        return "debt_or_preferred"
    if form.startswith("SC TO-T"):
        if re.search(r"merger sub|acquisition (?:sub|corp)|purchaser[^.]{0,40}wholly[- ]owned subsidiary|wholly[- ]owned subsidiary of", f[:30000]):
            return "acquisition"
        if re.search(r"mackenzie|everest|coastal realty|lapis|comrit|cmg partners|madison|sutter|anson|equity resource", filer + f[:5000]) \
                or "units" in cls or "limited partnership" in cls:
            return "mini_tender"
        return "third_party_other"
    if trm["at_nav"] or "beneficial interest" in cls or re.search(r"interval fund|closed-end|business development company|net asset value per", f[:40000]):
        return "fund_repurchase"
    if trm["dutch_auction"]: return "dutch_auction"
    if re.search(r"offer to exchange|exchange offer", f[:3000]): return "exchange_offer"
    if re.search(r"warrant", cls): return "warrant_tender"
    if re.search(r"options?|restricted stock units", cls): return "option_exchange"
    return "fixed_price"


def extract(form, docs):
    """docs: [(seq, doc_type, text)] for one filing -> dict of columns."""
    docs = sorted(docs, key=lambda d: d[0] or 0)
    main = next((d for d in docs if (d[1] or "").upper().startswith("SC TO")), docs[0] if docs else (0, "", ""))
    offer = next((d for d in docs if re.search(r"\(A\)\s*\(1\)\s*\((A|I)\)|\(A\)\(1\)$|\(1\)\(I\)|A\.?1\.?(I|A)\b|^EX-99\.?A1?$|^EX-99\.?\(A\)$|^EX-99\.1$|^EX-99\.12|^EX-99$|OFF",
                                          (d[1] or "").upper()) and d is not main), None)
    allt = "\n".join(d[2] for d in docs if d is not offer and d is not main)
    cov = cover(main[2])
    if cov["filer"] and re.match(r"(?:and )?Name of Filing", cov["filer"], re.I): cov["filer"] = None
    trm = terms((offer[2] if offer else "") + "\n" + main[2] + "\n" + allt)
    kind = classify(form, cov, trm, (offer[2] if offer else "") + main[2])
    if kind in ("debt_or_preferred", "exchange_offer", "option_exchange"):   # per-share cash price does not apply
        trm.update(price_per_share=None, price_low=None, price_high=None)
    amd = amendment(" ".join(d[2] for d in docs)) if form.endswith("/A") else {}
    return dict(cov, **trm, **amd, offer_kind=kind, offer_doc=offer[1] if offer else None)


# ------------------------------------------------------------------ BigQuery
VERSION = 4   # v4 (2026-10-28): common + preferred class lines stay equity offers (v3 regression on 19 rows)
# v3 (2026-10-28): cover names keep their own parentheticals ("(TI 2), LLC") and "Subject Company (Issuer)
#   and Filing Person (Offeror)" filers take the subject name (v2 stored "LLC" / "and Filing Person" on ~2,000 rows)
# v2 (2026-10-21): is_final reads the cover-page checkbox (v1 matched its label on every amendment); wider extension dates
TABLE = "sec.tender_terms"
SCHEMA = [("acc18", "STRING"), ("accession", "STRING"), ("form", "STRING"), ("file_date", "DATE"), ("cik", "STRING"),
          ("subject_company", "STRING"), ("filer", "STRING"), ("security_class", "STRING"), ("cusip", "STRING"),
          ("offer_kind", "STRING"), ("transaction_value_usd", "FLOAT"), ("price_per_share", "FLOAT"), ("price_low", "FLOAT"),
          ("price_high", "FLOAT"), ("at_nav", "BOOLEAN"), ("dutch_auction", "BOOLEAN"), ("max_shares", "FLOAT"),
          ("max_pct", "FLOAT"), ("max_amount_usd", "FLOAT"), ("expiration_date", "DATE"), ("shares_tendered", "FLOAT"),
          ("shares_accepted", "FLOAT"), ("final_price", "FLOAT"), ("new_expiration_date", "DATE"), ("is_final", "BOOLEAN"),
          ("offer_doc", "STRING"), ("extractor_version", "INTEGER"), ("extracted_at", "TIMESTAMP")]


def _months(start, end):
    d = start.replace(day=1)
    while d <= end:
        nxt = (d.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        yield max(d, start), min(nxt - dt.timedelta(days=1), end); d = nxt


def run_range(bq, P, start, end, log=print):
    """Extract every Schedule TO filed start..end that has text and no row at this VERSION. Month by month, so each
    query reads one month of sec.filing_docs (partitioned by month)."""
    from google.cloud import bigquery
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    cfg_load = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(c, t) for c, t in SCHEMA],
                                      source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, write_disposition="WRITE_APPEND")
    total = 0
    for a, b in _months(start, end):
        q = f"""
        WITH done AS (SELECT acc18 FROM `{P}.{TABLE}` WHERE file_date BETWEEN @a AND @b AND extractor_version = {VERSION})
        SELECT acc18, ANY_VALUE(accession) AS accession, ANY_VALUE(form) AS form, ANY_VALUE(file_date) AS file_date,
               ANY_VALUE(cik) AS cik, ARRAY_AGG(STRUCT(seq, doc_type, text) ORDER BY seq) AS docs
        FROM `{P}.rrp.sec_docs`
        WHERE file_date BETWEEN @a AND @b AND STARTS_WITH(form, 'SC TO-') AND doc_type != 'INDEX'
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
            r = {c: r.get(c) for c, _ in SCHEMA if c in r}
            r.update(acc18=x.acc18, accession=x.accession, form=x.form, file_date=str(x.file_date), cik=x.cik,
                     extractor_version=VERSION, extracted_at=stamp)
            for k in ("expiration_date", "new_expiration_date"):
                if r.get(k) is not None: r[k] = str(r[k])
            rows.append(r)
        if rows: bq.load_table_from_json(rows, f"{P}.{TABLE}", job_config=cfg_load).result()
        total += len(rows)
        if rows: log(f"  tender terms {a:%Y-%m}: {len(rows)} filings")
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
        t.clustering_fields = ["offer_kind", "acc18"]
        bq.create_table(t); print(f"  created {TABLE}")
