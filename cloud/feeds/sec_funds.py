"""SEC fund filings -> BigQuery `sec`: Form D (exempt offerings: private raises, hedge / PE / VC / crypto funds) and
Form 13F (institutional managers' quarterly long holdings). Added 2026-10-23 (b167).

Tables
  sec.formd               one row per Form D / D/A: issuer, industry and fund type, offering size, amount sold, investors,
                          fees, exemptions, related persons. History from the SEC Form D data sets (2008 on).
  sec.form13f_filings     one row per 13F-HR / 13F-HR/A / 13F-NT: manager, period, report type, entry count, total value.
  sec.form13f_holdings    one row per information-table line: issuer, class, CUSIP, value, shares, put/call, discretion,
                          voting authority. value_usd is in dollars: filings before 2023-01-03 reported thousands, which
                          is converted here (value_reported keeps the number as filed). History from the 13F data sets
                          (2013 Q3 on).
  sec.form13f_totals      per filing: the sum and count of its holding lines (cover totals as typed carry typos).
  sec.form13f_unit_fixes  filings whose units were corrected (see _units): kind holdings / holdings_no_sh /
                          holdings_post2023 / cover, with the evidence (median price per share, or cover / holdings ratio).
Units rule (2026-10-26 audit): some filers reported dollars when the form said thousands (and, after 2023, values 1,000x
too big). Per filing, the median value per share of its stock lines (all long lines when it has no stock lines) is
checked: $2,000 or more means the units are 1,000x off, and value_usd is divided accordingly. A cover total 300x or more
the corrected holdings sum is divided by 1,000.
Each accession is written once, by whichever source reaches it first: the daily pull (EDGAR daily form index, then the
filing's full submission text, XML parsed) or the quarterly data set. Both skip accessions already in the table, so
re-running anything is safe and nothing is MERGEd over the 100M+ holdings rows.

In the rrp-feeds job (main.py): pull_daily once a day (7 AM ET run) over the last 5 daily indexes.
History, run once from the PC (resumes: data sets already loaded are skipped):
  py sec_funds.py --formd-history                 # every Form D data set (~75 quarters, small)
  py sec_funds.py --13f-history                   # every 13F data set (~55 sets, ~90 MB each, ~3M holdings each)
  py sec_funds.py --13f-history --since 2020      # only data sets covering 2020 on
  py sec_funds.py --formd-history --dry           # parse the newest set, print counts and columns, write nothing
  py sec_funds.py --144-history                   # Form 144 since April 2023 (XML era), quarter by quarter
  py sec_funds.py --13g-history                   # Schedule 13G since Dec 2024 (XML era)
  py sec_funds.py --adv-history                   # every monthly ADV spreadsheet since 2019 (registered + exempt)
  add --dry to any history: newest filings only, prints samples (and every XML tag of one filing), writes nothing
  py sec_funds.py --daily --dry                   # the daily pull without writing
  py sec_funds.py --daily --days 45               # catch up: every Form D / 13F in the last 45 days not loaded yet
SEC fair access: <= 8 requests/second with a descriptive User-Agent.
"""
import csv, datetime as dt, gzip, io, json, os, re, sys, tempfile, time, zipfile
import xml.etree.ElementTree as ET
import bq_cap  # noqa: F401  hard cap on BigQuery query bytes (b247)

PROJECT = os.environ.get("RRP_PROJECT", "rrp-technologies")
UA = {"User-Agent": "RRP Technologies research bot (fund filings)", "Accept-Encoding": "gzip, deflate"}
LISTS = {"formd": "https://www.sec.gov/dera/data/form-d", "13f": "https://www.sec.gov/dera/data/form-13f"}
FORMD_FORMS = {"D", "D/A"}
F13_FORMS = {"13F-HR", "13F-HR/A", "13F-NT", "13F-NT/A"}
F144_FORMS = {"144", "144/A"}                         # notice of proposed sale (XML since April 2023)
F13G_FORMS = {"SCHEDULE 13G", "SCHEDULE 13G/A"}       # passive 5%+ stakes (XML since Dec 18 2024; older SC 13G is text)
ALL_FORMS = FORMD_FORMS | F13_FORMS | F144_FORMS | F13G_FORMS
IDX_LINE = re.compile(r"^(\S.*?)\s{2,}(.+?)\s{2,}(\d+)\s{2,}(\d{4}-?\d{2}-?\d{2})\s{2,}(edgar/data/\S+)\s*$")
THOUSANDS_UNTIL = "2023-01-03"
SET_TYPES = (("ISEQUITYTYPE", "equity"), ("ISDEBTTYPE", "debt"), ("ISOPTIONTOACQUIRETYPE", "option"),
             ("ISSECURITYTOBEACQUIREDTYPE", "to_be_acquired"), ("ISPOOLEDINVESTMENTFUNDTYPE", "pooled_fund"),
             ("ISTENANTINCOMMONTYPE", "tenant_in_common"), ("ISMINERALPROPERTYTYPE", "mineral"), ("ISOTHERTYPE", "other"))   # 13F values were reported in $ thousands on filings before this date
csv.field_size_limit(10**9)

F, I, D, B, St, T = "FLOAT", "INTEGER", "DATE", "BOOLEAN", "STRING", "TIMESTAMP"
SCHEMAS = {
    "formd": [("accession", St), ("filed", D), ("form", St), ("cik", St), ("entity_name", St), ("entity_type", St),
              ("city", St), ("state", St), ("jurisdiction", St), ("year_of_inc", St), ("industry_group", St),
              ("fund_type", St), ("is_40act", B), ("revenue_range", St), ("nav_range", St), ("exemptions", St),
              ("is_amendment", B), ("prev_accession", St), ("first_sale_date", D), ("first_sale_yet_to_occur", B),
              ("more_than_one_year", B), ("is_equity", B), ("is_debt", B), ("is_pooled_fund", B), ("security_types", St),
              ("min_investment", F), ("total_offering", F), ("offering_indefinite", B), ("total_sold", F),
              ("total_remaining", F), ("investors_count", I), ("has_nonaccredited", B), ("nonaccredited_count", I),
              ("sales_commissions", F), ("finders_fees", F), ("related_persons", St), ("sic", St), ("source", St),
              ("ingested_at", T)],
    "form13f_filings": [("accession", St), ("filed", D), ("form", St), ("cik", St), ("manager", St), ("period", D),
                        ("report_type", St), ("is_amendment", B), ("amendment_type", St), ("file_number", St),
                        ("crd", St), ("sec_file_number", St), ("city", St), ("state", St),
                        ("other_managers_count", I), ("entry_total", I), ("value_total_usd", F), ("source", St),
                        ("ingested_at", T)],
    "form13f_holdings": [("accession", St), ("filed", D), ("period", D), ("cik", St), ("line", I), ("issuer", St),
                         ("title_of_class", St), ("cusip", St), ("figi", St), ("value_usd", F), ("value_reported", F),
                         ("shares", F), ("shares_type", St), ("put_call", St), ("discretion", St),
                         ("other_manager", St), ("vote_sole", F), ("vote_shared", F), ("vote_none", F), ("source", St)],
}
SCHEMAS["form144"] = [("accession", St), ("filed", D), ("form", St), ("line", I), ("issuer_cik", St), ("issuer_name", St),
                      ("seller", St), ("relationship", St), ("security_class", St), ("broker", St), ("units_to_sell", F),
                      ("market_value", F), ("units_outstanding", F), ("approx_sale_date", D), ("exchange", St),
                      ("acquired_date", D), ("acquisition_nature", St), ("sold_past_3m_units", F), ("sold_past_3m_value", F),
                      ("plan_adoption_date", D), ("notice_date", D), ("source", St), ("ingested_at", T)]
SCHEMAS["form13g"] = [("accession", St), ("filed", D), ("form", St), ("issuer_cik", St), ("issuer_name", St), ("cusip", St),
                      ("security_class", St), ("event_date", D), ("filer", St), ("filer_type", St), ("pct", F), ("shares", F),
                      ("sole_vote", F), ("shared_vote", F), ("sole_dispositive", F), ("shared_dispositive", F),
                      ("persons", St), ("rule", St), ("amendment_no", St), ("source", St), ("ingested_at", T)]
SCHEMAS["adv_firms"] = [("as_of", D), ("kind", St), ("crd", St), ("sec_number", St), ("cik", St), ("firm_name", St),
                        ("legal_name", St), ("city", St), ("state", St), ("country", St), ("adv_filing_date", D),
                        ("website", St), ("employees", F), ("aum_discretionary", F), ("aum_nondiscretionary", F),
                        ("aum_total", F), ("accounts_discretionary", F), ("accounts_nondiscretionary", F),
                        ("accounts_total", F), ("source_file", St), ("ingested_at", T)]
SCHEMAS["form13f_totals"] = [("accession", St), ("period", D), ("holdings_value_usd", F), ("holdings_lines", I),
                             ("long_value_usd", F), ("computed_at", T)]
SCHEMAS["form13f_unit_fixes"] = [("accession", St), ("filed", D), ("kind", St), ("evidence", F)]
TABLES = {"formd": "sec.formd", "form13f_filings": "sec.form13f_filings", "form13f_holdings": "sec.form13f_holdings",
          "form144": "sec.form144", "form13g": "sec.form13g", "adv_firms": "sec.adv_firms",
          "form13f_totals": "sec.form13f_totals", "form13f_unit_fixes": "sec.form13f_unit_fixes"}
PARTITION = {"formd": ("filed", "YEAR"), "form13f_filings": ("filed", "YEAR"), "form13f_holdings": ("period", "MONTH"),
             "form144": ("filed", "MONTH"), "form13g": ("filed", "MONTH"), "adv_firms": ("as_of", "MONTH")}
CLUSTER = {"formd": ["industry_group", "cik"], "form13f_filings": ["cik"], "form13f_holdings": ["cusip", "cik"],
           "form144": ["issuer_cik"], "form13g": ["cusip", "issuer_cik"], "adv_firms": ["crd"],
           "form13f_totals": ["accession"], "form13f_unit_fixes": ["accession"]}


# ------------------------------------------------------------------ small helpers
def _num(v):
    v = (v or "").strip().replace(",", "").replace("$", "")
    if not v: return None
    try: return float(v)
    except ValueError: return None


def _int(v):
    x = _num(v)
    return int(x) if x is not None else None


def _bool(v):
    v = (v or "").strip().lower()
    if v in ("y", "yes", "true", "1", "x"): return True
    if v in ("n", "no", "false", "0"): return False
    return None


def _date(s):
    """ISO date from the formats the SEC data sets and XML use: 30-SEP-2026, 2026-09-30 (with or without a time),
    09-30-2026, 9/30/2026 (with or without a time), 20260930. None when nothing matches."""
    s = (s or "").strip().split(" ")[0]
    if re.match(r"\d{4}-\d{2}-\d{2}T", s): s = s[:10]   # ISO with a time ('OCT' has a T: never split on it blindly)
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y%m%d", "%d-%b-%y", "%Y/%m/%d"):
        try: return dt.datetime.strptime(s, fmt).date().isoformat()
        except ValueError: pass
    return None


def _acc(s):
    """18-digit accession from '0001104659-26-112421', its digits, or an EDGAR path (edgar/data/<cik>/<accession>.txt)."""
    m = re.search(r"(\d{10})-?(\d{2})-?(\d{6})(?!\d)", s or "")
    return "".join(m.groups()) if m else re.sub(r"[^0-9]", "", s or "")


def _stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


_last = [0.0]


def _get(url, stream=False, tries=5):
    import requests
    for i in range(tries):
        wait = 0.125 - (time.time() - _last[0])
        if wait > 0: time.sleep(wait)
        _last[0] = time.time()
        try:
            r = requests.get(url, headers=UA, timeout=600, stream=stream)
            if r.status_code in (403, 404): return None   # sec.gov answers a missing file with 403 as well as 404
            if r.status_code in (429, 500, 502, 503): time.sleep(5 * (i + 1)); continue
            r.raise_for_status(); return r
        except Exception:
            if i == tries - 1: raise
            time.sleep(3 * (i + 1))
    raise RuntimeError(f"gave up on {url}")


# ------------------------------------------------------------------ quarterly data sets
def dataset_links(kind):
    """Every data set zip on the SEC listing page, oldest first: (name, url)."""
    r = _get(LISTS[kind])
    if r is None: raise RuntimeError(f"{kind} data set page not found")
    hrefs = sorted(set(re.findall(r'href="([^"]+?\.zip)"', r.text)))
    out = []
    for h in hrefs:
        if kind == "formd" and "form-d-data-sets" not in h: continue
        if kind == "13f" and "form-13f-data-sets" not in h: continue
        url = h if h.startswith("http") else "https://www.sec.gov" + h
        out.append((url.rsplit("/", 1)[1], url))
    return sorted(out, key=lambda x: _set_order(x[0]))


def _set_order(name):
    """Sort key for data set file names: 2023q4_form13f.zip / 2013q2_d_0.zip / 01jun2024-31aug2024_form13f.zip."""
    m = re.match(r"(\d{4})q(\d)", name)
    if m: return (int(m.group(1)), (int(m.group(2)) - 1) * 3 + 1)
    m = re.match(r"\d{2}([a-z]{3})(\d{4})-", name)
    if m: return (int(m.group(2)), dt.datetime.strptime(m.group(1), "%b").month)
    return (0, 0)


def _download(url):
    r = _get(url, stream=True)
    if r is None: return None
    f = tempfile.NamedTemporaryFile(suffix="." + url.rsplit(".", 1)[-1].lower()[:5], delete=False)
    for chunk in r.iter_content(1 << 20): f.write(chunk)
    f.close()
    return f.name


def _tsv(z, base):
    """Rows of the member whose file name is `base` (any folder inside the zip), header-keyed (upper case)."""
    names = [n for n in z.namelist() if n.rsplit("/", 1)[-1].upper() == base.upper()]
    if not names: return
    with z.open(names[0]) as fh:
        rd = csv.reader(io.TextIOWrapper(fh, encoding="utf-8", errors="replace", newline=""), delimiter="\t",
                        quoting=csv.QUOTE_NONE)
        head = [h.strip().upper() for h in next(rd)]
        for r in rd:
            if len(r) >= len(head): yield dict(zip(head, (c.strip() for c in r)))


def _g(r, *names):
    for n in names:
        v = r.get(n)
        if v not in (None, ""): return v
    return ""


def formd_set_rows(path, skip=frozenset()):
    with zipfile.ZipFile(path) as z:   # closed before the temp file is deleted (Windows locks open files)
        return _formd_set_rows(z, skip)


def _formd_set_rows(z, skip):
    stamp = _stamp()
    sub = {_acc(r["ACCESSIONNUMBER"]): r for r in _tsv(z, "FORMDSUBMISSION.tsv")}
    iss = {}
    for r in _tsv(z, "ISSUERS.tsv"):
        a = _acc(r.get("ACCESSIONNUMBER"))
        if _bool(r.get("IS_PRIMARYISSUER_FLAG")) is not False and a not in iss: iss[a] = r
    people = {}
    for r in _tsv(z, "RELATEDPERSONS.tsv"):
        a = _acc(r.get("ACCESSIONNUMBER"))
        nm = " ".join(x for x in (_g(r, "FIRSTNAME"), _g(r, "LASTNAME")) if x)
        rel = "/".join(x for x in (_g(r, "RELATIONSHIP_1"), _g(r, "RELATIONSHIP_2"), _g(r, "RELATIONSHIP_3")) if x)
        if nm: people.setdefault(a, []).append(f"{nm} ({rel})" if rel else nm)
    out = []
    for r in _tsv(z, "OFFERING.tsv"):
        a = _acc(r.get("ACCESSIONNUMBER"))
        if a in skip or a not in sub: continue
        s, i = sub[a], iss.get(a, {})
        tot = _g(r, "TOTALOFFERINGAMOUNT")
        sec_types = [lab for k, lab in SET_TYPES if _bool(r.get(k))]
        out.append(dict(
            accession=a, filed=_date(_g(s, "FILING_DATE")), form=_g(s, "SUBMISSIONTYPE"), cik=_g(i, "CIK").lstrip("0"),
            entity_name=_g(i, "ENTITYNAME"), entity_type=_g(i, "ENTITYTYPE"), city=_g(i, "CITY"),
            state=_g(i, "STATEORCOUNTRY"), jurisdiction=_g(i, "JURISDICTIONOFINC"),
            year_of_inc=_g(i, "YEAROFINC_VALUE_ENTERED", "YEAROFINC_TIMESPAN_CHOICE"),
            industry_group=_g(r, "INDUSTRYGROUPTYPE"), fund_type=_g(r, "INVESTMENTFUNDTYPE") or None,
            is_40act=_bool(r.get("IS40ACT")), revenue_range=_g(r, "REVENUERANGE") or None,
            nav_range=_g(r, "AGGREGATENETASSETVALUERANGE") or None, exemptions=_g(r, "FEDERALEXEMPTIONS_ITEMS_LIST"),
            is_amendment=_bool(r.get("ISAMENDMENT")), prev_accession=_acc(_g(r, "PREVIOUSACCESSIONNUMBER")) or None,
            first_sale_date=_date(_g(r, "SALE_DATE")), first_sale_yet_to_occur=_bool(r.get("YETTOOCCUR")),
            more_than_one_year=_bool(r.get("MORETHANONEYEAR")), is_equity=_bool(r.get("ISEQUITYTYPE")),
            is_debt=_bool(r.get("ISDEBTTYPE")), is_pooled_fund=_bool(r.get("ISPOOLEDINVESTMENTFUNDTYPE")),
            security_types=",".join(sec_types),
            min_investment=_num(r.get("MINIMUMINVESTMENTACCEPTED")),
            total_offering=_num(tot), offering_indefinite=tot.strip().lower() == "indefinite" if tot else None,
            total_sold=_num(r.get("TOTALAMOUNTSOLD")), total_remaining=_num(r.get("TOTALREMAINING")),
            investors_count=_int(r.get("TOTALNUMBERALREADYINVESTED")), has_nonaccredited=_bool(r.get("HASNONACCREDITEDINVESTORS")),
            nonaccredited_count=_int(r.get("NUMBERNONACCREDITEDINVESTORS")),
            sales_commissions=_num(r.get("SALESCOMM_DOLLARAMOUNT")), finders_fees=_num(r.get("FINDERSFEE_DOLLARAMOUNT")),
            related_persons="; ".join(people.get(a, []))[:2000] or None, sic=_g(s, "SIC_CODE") or None,
            source="dataset", ingested_at=stamp))
    return out


def f13_set_rows(path, skip=frozenset()):
    """(filings, holdings iterator factory) for one 13F data set. The holdings iterator opens the zip itself and closes
    it when done, so the temp file can be deleted afterwards (Windows locks open files)."""
    stamp = _stamp()
    z = zipfile.ZipFile(path)
    sub = {_acc(r["ACCESSION_NUMBER"]): r for r in _tsv(z, "SUBMISSION.tsv")}
    cov = {_acc(r["ACCESSION_NUMBER"]): r for r in _tsv(z, "COVERPAGE.tsv")}
    summ = {_acc(r["ACCESSION_NUMBER"]): r for r in _tsv(z, "SUMMARYPAGE.tsv")}
    z.close()
    filings = {}
    for a, s in sub.items():
        if a in skip: continue
        c, m = cov.get(a, {}), summ.get(a, {})
        filed = _date(_g(s, "FILING_DATE"))
        tv = _num(m.get("TABLEVALUETOTAL"))
        if tv is not None and filed and filed < THOUSANDS_UNTIL: tv *= 1000
        filings[a] = dict(accession=a, filed=filed, form=_g(s, "SUBMISSIONTYPE"), cik=_g(s, "CIK").lstrip("0"),
                          manager=_g(c, "FILINGMANAGER_NAME"),
                          period=_date(_g(s, "PERIODOFREPORT")) or _date(_g(c, "REPORTCALENDARORQUARTER")),
                          _raw=f"FILING_DATE={_g(s, 'FILING_DATE')!r} PERIODOFREPORT={_g(s, 'PERIODOFREPORT')!r}",
                          report_type=_g(c, "REPORTTYPE"), is_amendment=_bool(c.get("ISAMENDMENT")),
                          amendment_type=_g(c, "AMENDMENTTYPE") or None, file_number=_g(c, "FORM13FFILENUMBER") or None,
                          crd=_g(c, "CRDNUMBER") or None, sec_file_number=_g(c, "SECFILENUMBER") or None,
                          city=_g(c, "FILINGMANAGER_CITY"), state=_g(c, "FILINGMANAGER_STATEORCOUNTRY"),
                          other_managers_count=_int(m.get("OTHERINCLUDEDMANAGERSCOUNT")),
                          entry_total=_int(m.get("TABLEENTRYTOTAL")), value_total_usd=tv, source="dataset",
                          ingested_at=stamp)

    # first pass over the information table: each filing's lines, for the units rule (values, shares, type, put/call)
    lines = {}
    with zipfile.ZipFile(path) as hz:
        for r in _tsv(hz, "INFOTABLE.tsv"):
            a = _acc(r.get("ACCESSION_NUMBER"))
            if a in filings:
                lines.setdefault(a, []).append((_num(r.get("VALUE")), _num(r.get("SSHPRNAMT")), _g(r, "SSHPRNAMTTYPE"),
                                                _g(r, "PUTCALL") or None))
    extras = {"form13f_totals": [], "form13f_unit_fixes": []}
    for a, f in filings.items():
        ls = lines.get(a, [])
        factor, kind, med = _unit_factor(f["filed"], ls)
        f["_factor"] = factor
        if kind: extras["form13f_unit_fixes"].append(dict(accession=a, filed=f["filed"], kind=kind, evidence=med))
        total = sum(v * factor for v, s, t, pc in ls if v is not None)
        tv = f["value_total_usd"]
        if tv and total > 0 and tv / total >= 300:
            f["value_total_usd"] = tv / 1000
            extras["form13f_unit_fixes"].append(dict(accession=a, filed=f["filed"], kind="cover", evidence=round(tv / total, 1)))
        extras["form13f_totals"].append(dict(accession=a, period=f["period"], holdings_value_usd=total, holdings_lines=len(ls),
                                             long_value_usd=sum(v * factor for v, s, t, pc in ls if v is not None and not pc),
                                             computed_at=stamp))
    del lines

    def holdings():
        line = {}
        with zipfile.ZipFile(path) as hz:
            yield from _holding_rows(hz, filings, line)
    holdings.extras = extras
    return list(filings.values()), holdings


def _holding_rows(z, filings, line):
    if True:
        for r in _tsv(z, "INFOTABLE.tsv"):
            a = _acc(r.get("ACCESSION_NUMBER"))
            f = filings.get(a)
            if f is None: continue
            line[a] = line.get(a, 0) + 1
            v = _num(r.get("VALUE"))
            yield dict(accession=a, filed=f["filed"], period=f["period"], cik=f["cik"], line=line[a],
                       issuer=_g(r, "NAMEOFISSUER"), title_of_class=_g(r, "TITLEOFCLASS"), cusip=_g(r, "CUSIP").upper(),
                       figi=_g(r, "FIGI") or None,
                       value_usd=(v * f["_factor"] if v is not None else None),
                       value_reported=v, shares=_num(r.get("SSHPRNAMT")), shares_type=_g(r, "SSHPRNAMTTYPE"),
                       put_call=_g(r, "PUTCALL") or None, discretion=_g(r, "INVESTMENTDISCRETION"),
                       other_manager=_g(r, "OTHERMANAGER") or None, vote_sole=_num(r.get("VOTING_AUTH_SOLE")),
                       vote_shared=_num(r.get("VOTING_AUTH_SHARED")), vote_none=_num(r.get("VOTING_AUTH_NONE")),
                       source="dataset")


# ------------------------------------------------------------------ daily: EDGAR daily index + full submission text
def _strip_ns(root):
    for el in root.iter():
        if isinstance(el.tag, str) and "}" in el.tag: el.tag = el.tag.split("}", 1)[1]
    return root


def _xml_blocks(txt):
    """Parsed XML documents inside an EDGAR full submission .txt."""
    out = []
    for m in re.finditer(r"<XML>\s*(.*?)\s*</XML>", txt, re.S | re.I):
        body = m.group(1).strip()
        try: out.append(_strip_ns(ET.fromstring(body.encode("utf-8"))))
        except ET.ParseError: continue
    return out


def _t(el, path):
    if el is None: return ""
    x = el.find(path)
    return (x.text or "").strip() if x is not None and x.text else ""


def parse_formd(acc, form, filed, txt):
    roots = [r for r in _xml_blocks(txt) if r.tag == "edgarSubmission"]
    if not roots: return None
    x = roots[0]; iss = x.find("primaryIssuer"); od = x.find("offeringData")
    people = []
    for p in x.findall("relatedPersonsList/relatedPersonInfo"):
        nm = " ".join(v for v in (_t(p, "relatedPersonName/firstName"), _t(p, "relatedPersonName/lastName")) if v)
        rel = "/".join((r.text or "").strip() for r in p.findall("relatedPersonRelationshipList/relationship") if r.text)
        if nm: people.append(f"{nm} ({rel})" if rel else nm)
    st = od.find("typesOfSecuritiesOffered") if od is not None else None
    sec_types = [lab for k, lab in (("isEquityType", "equity"), ("isDebtType", "debt"), ("isOptionToAcquireType", "option"),
                                    ("isSecurityToBeAcquiredType", "to_be_acquired"), ("isPooledInvestmentFundType", "pooled_fund"),
                                    ("isTenantInCommonType", "tenant_in_common"), ("isMineralPropertyType", "mineral"),
                                    ("isOtherType", "other")) if _bool(_t(st, k))]
    tot = _t(od, "offeringSalesAmounts/totalOfferingAmount")
    return dict(
        accession=acc, filed=filed, form=form, cik=_t(iss, "cik").lstrip("0"), entity_name=_t(iss, "entityName"),
        entity_type=_t(iss, "entityType"), city=_t(iss, "issuerAddress/city"), state=_t(iss, "issuerAddress/stateOrCountry"),
        jurisdiction=_t(iss, "jurisdictionOfInc"), year_of_inc=_t(iss, "yearOfInc/value") or _t(iss, "yearOfInc/withinFiveYears"),
        industry_group=_t(od, "industryGroup/industryGroupType"),
        fund_type=_t(od, "industryGroup/investmentFundInfo/investmentFundType") or None,
        is_40act=_bool(_t(od, "industryGroup/investmentFundInfo/is40Act")),
        revenue_range=_t(od, "issuerSize/revenueRange") or None, nav_range=_t(od, "issuerSize/aggregateNetAssetValueRange") or None,
        exemptions=",".join((i.text or "").strip() for i in (od.findall("federalExemptionsExclusions/item") if od is not None else []) if i.text),
        is_amendment=_bool(_t(od, "typeOfFiling/newOrAmendment/isAmendment")),
        prev_accession=_acc(_t(od, "typeOfFiling/newOrAmendment/previousAccessionNumber")) or None,
        first_sale_date=_date(_t(od, "typeOfFiling/dateOfFirstSale/value")),
        first_sale_yet_to_occur=_bool(_t(od, "typeOfFiling/dateOfFirstSale/yetToOccur")),
        more_than_one_year=_bool(_t(od, "durationOfOffering/moreThanOneYear")),
        is_equity="equity" in sec_types, is_debt="debt" in sec_types, is_pooled_fund="pooled_fund" in sec_types,
        security_types=",".join(sec_types), min_investment=_num(_t(od, "minimumInvestmentAccepted")),
        total_offering=_num(tot), offering_indefinite=tot.lower() == "indefinite" if tot else None,
        total_sold=_num(_t(od, "offeringSalesAmounts/totalAmountSold")),
        total_remaining=_num(_t(od, "offeringSalesAmounts/totalRemaining")),
        investors_count=_int(_t(od, "investors/totalNumberAlreadyInvested")),
        has_nonaccredited=_bool(_t(od, "investors/hasNonAccreditedInvestors")),
        nonaccredited_count=_int(_t(od, "investors/numberNonAccreditedInvestors")),
        sales_commissions=_num(_t(od, "salesCommissionsFindersFees/salesCommissions/dollarAmount")),
        finders_fees=_num(_t(od, "salesCommissionsFindersFees/findersFees/dollarAmount")),
        related_persons="; ".join(people)[:2000] or None, sic=None, source="edgar_xml", ingested_at=_stamp())


# ------------------------------------------------------------------ 13F units rule (added 2026-10-26)
def _median(xs):
    xs = sorted(xs)
    if not xs: return None
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def _unit_factor(filed, lines):
    """lines = (value_reported, shares, shares_type, put_call) of one filing. Returns (factor, kind, evidence): value_usd
    = value_reported * factor. The form's unit is $ thousands before 2023-01-03 and dollars after; a median value per
    share of $2,000 or more (stock lines, else every long line) means the filer was 1,000x off that unit."""
    base = 1000 if filed and filed < THOUSANDS_UNTIL else 1
    sh = [v * base / s for v, s, t, pc in lines if t == "SH" and not pc and v and s and v > 0 and s > 0]
    allx = [v * base / s for v, s, t, pc in lines if not pc and v and s and v > 0 and s > 0]
    med = _median(sh) if sh else _median(allx)
    if med is not None and med >= 2000:
        return base / 1000, ("holdings" if sh else "holdings_no_sh") if base == 1000 else "holdings_post2023", med
    return base, None, med


def _units(filing, holdings):
    """Apply the units rule to one parsed filing in place; returns (totals row, unit-fix rows)."""
    factor, kind, med = _unit_factor(filing["filed"], [(h["value_reported"], h["shares"], h["shares_type"], h["put_call"])
                                                        for h in holdings])
    for h in holdings:
        h["value_usd"] = h["value_reported"] * factor if h["value_reported"] is not None else None
    fixes = [dict(accession=filing["accession"], filed=filing["filed"], kind=kind, evidence=med)] if kind else []
    total = sum(h["value_usd"] or 0 for h in holdings)
    tv = filing.get("value_total_usd")
    if tv and total > 0 and tv / total >= 300:
        filing["value_total_usd"] = tv / 1000
        fixes.append(dict(accession=filing["accession"], filed=filing["filed"], kind="cover", evidence=round(tv / total, 1)))
    totals = dict(accession=filing["accession"], period=filing["period"], holdings_value_usd=total, holdings_lines=len(holdings),
                  long_value_usd=sum(h["value_usd"] or 0 for h in holdings if not h["put_call"]), computed_at=_stamp())
    return totals, fixes


def parse_13f(acc, form, filed, cik, txt):
    roots = _xml_blocks(txt)
    sub = next((r for r in roots if r.tag == "edgarSubmission"), None)
    if sub is None: return None, []
    fd = sub.find("formData"); cp = fd.find("coverPage") if fd is not None else None; sp = fd.find("summaryPage") if fd is not None else None
    tv = _num(_t(sp, "tableValueTotal"))
    if tv is not None and filed < THOUSANDS_UNTIL: tv *= 1000
    period = _date(_t(sub, "headerData/filerInfo/periodOfReport")) or _date(_t(cp, "reportCalendarOrQuarter"))
    filing = dict(accession=acc, filed=filed, form=form, cik=cik, manager=_t(cp, "filingManager/name"), period=period,
                  report_type=_t(cp, "reportType"), is_amendment=_bool(_t(cp, "isAmendment")),
                  amendment_type=_t(cp, "amendmentInfo/amendmentType") or None,
                  file_number=_t(cp, "form13FFileNumber") or None, crd=_t(cp, "crdNumber") or None,
                  sec_file_number=_t(cp, "secFileNumber") or None, city=_t(cp, "filingManager/address/city"),
                  state=_t(cp, "filingManager/address/stateOrCountry"),
                  other_managers_count=_int(_t(sp, "otherIncludedManagersCount")),
                  entry_total=_int(_t(sp, "tableEntryTotal")), value_total_usd=tv, source="edgar_xml", ingested_at=_stamp())
    holdings = []
    for it in (r for r in roots if r.tag == "informationTable"):
        for n, h in enumerate(it.findall("infoTable"), 1):
            v = _num(_t(h, "value"))
            holdings.append(dict(accession=acc, filed=filed, period=period, cik=cik, line=n, issuer=_t(h, "nameOfIssuer"),
                                 title_of_class=_t(h, "titleOfClass"), cusip=_t(h, "cusip").upper(), figi=_t(h, "figi") or None,
                                 value_usd=(v * 1000 if v is not None and filed < THOUSANDS_UNTIL else v), value_reported=v,
                                 shares=_num(_t(h, "shrsOrPrnAmt/sshPrnamt")), shares_type=_t(h, "shrsOrPrnAmt/sshPrnamtType"),
                                 put_call=_t(h, "putCall") or None, discretion=_t(h, "investmentDiscretion"),
                                 other_manager=_t(h, "otherManager") or None, vote_sole=_num(_t(h, "votingAuthority/Sole")),
                                 vote_shared=_num(_t(h, "votingAuthority/Shared")), vote_none=_num(_t(h, "votingAuthority/None")),
                                 source="edgar_xml"))
    return filing, holdings


def _desc(el, name):
    """Every descendant element with this local name (namespaces already stripped)."""
    return [] if el is None else [x for x in el.iter() if x.tag == name]


def _first(el, *names):
    """Text of the first descendant with any of these local names, '' when none."""
    if el is None: return ""
    for n in names:
        for x in el.iter():
            if x.tag == n and x.text and x.text.strip(): return x.text.strip()
    return ""


def parse_144(acc, form, filed, txt):
    """One row per security block of a Form 144 (most notices have one). Element names are read loosely."""
    roots = [r for r in _xml_blocks(txt) if r.tag == "edgarSubmission"]
    if not roots: return []
    x = roots[0]
    issuer = (_desc(x, "issuerInfo") or [x])[0]
    seller = _first(issuer, "nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold", "personForWhoseAccount", "sellerName")
    rel = "/".join(sorted({(r.text or "").strip() for r in _desc(issuer, "relationshipToIssuer") if r.text}))
    tobe = (_desc(x, "securitiesToBeSold") or [None])[0]
    past = _desc(x, "securitiesSoldInPast3Months")
    past_units = sum(_num(_first(p, "amountOfSecuritiesSold")) or 0 for p in past) if past else None
    past_value = sum(_num(_first(p, "grossProceeds")) or 0 for p in past) if past else None
    plan = _first(x, "planAdoptionDate")
    notice = _first(x, "noticeDate")
    blocks = _desc(x, "securitiesInformation") or [x]
    out = []
    for n, b in enumerate(blocks, 1):
        out.append(dict(
            accession=acc, filed=filed, form=form, line=n, issuer_cik=_first(issuer, "issuerCik").lstrip("0"),
            issuer_name=_first(issuer, "issuerName"), seller=seller, relationship=rel or None,
            security_class=_first(b, "securitiesClassTitle"), broker=_first(b, "name", "brokerOrMarketmakerName") or None,
            units_to_sell=_num(_first(b, "noOfUnitsSold")), market_value=_num(_first(b, "aggregateMarketValue")),
            units_outstanding=_num(_first(b, "noOfUnitsOutstanding")), approx_sale_date=_date(_first(b, "approxSaleDate")),
            exchange=_first(b, "securitiesExchangeName") or None, acquired_date=_date(_first(tobe, "acquiredDate")),
            acquisition_nature=_first(tobe, "natureOfAcquisitionTransaction") or None,
            sold_past_3m_units=past_units, sold_past_3m_value=past_value, plan_adoption_date=_date(plan),
            notice_date=_date(notice), source="edgar_xml", ingested_at=_stamp()))
    return out


def parse_13g(acc, form, filed, txt):
    """One row per Schedule 13G (XML era): the issuer, CUSIP, the lead reporting person's stake, every person named."""
    roots = [r for r in _xml_blocks(txt) if r.tag == "edgarSubmission"]
    if not roots: return None
    x = roots[0]
    persons = _desc(x, "coverPageHeaderReportingPersonDetails") or _desc(x, "reportingPersonDetails")
    def stake(p):
        return dict(name=_first(p, "reportingPersonName"), type=_first(p, "typeOfReportingPerson"),
                    pct=_num(_first(p, "classPercent", "percentOfClass")),
                    shares=_num(_first(p, "reportingPersonBeneficiallyOwnedAggregateNumberOfShares", "aggregateAmountOwned")),
                    sole_vote=_num(_first(p, "soleVotingPower")), shared_vote=_num(_first(p, "sharedVotingPower")),
                    sole_disp=_num(_first(p, "soleDispositivePower")), shared_disp=_num(_first(p, "sharedDispositivePower")))
    st = [stake(p) for p in persons]
    lead = max(st, key=lambda d: (d["pct"] or 0, d["shares"] or 0)) if st else {}
    cusip = re.sub(r"[^0-9A-Za-z]", "", _first(x, "issuerCusipNumber", "issuerCusip", "cusipNumber", "cusip")).upper()
    return dict(accession=acc, filed=filed, form=form, issuer_cik=_first(x, "issuerCik", "issuerCIK").lstrip("0"),
                issuer_name=_first(x, "issuerName"), cusip=cusip or None,
                security_class=_first(x, "securitiesClassTitle", "titleOfClassOfSecurities") or None,
                event_date=_date(_first(x, "dateOfEvent", "eventDateRequiresFilingThisStatement")),
                filer=lead.get("name"), filer_type=lead.get("type"), pct=lead.get("pct"), shares=lead.get("shares"),
                sole_vote=lead.get("sole_vote"), shared_vote=lead.get("shared_vote"),
                sole_dispositive=lead.get("sole_disp"), shared_dispositive=lead.get("shared_disp"),
                persons=json.dumps(st)[:4000] if st else None,
                rule=_first(x, "designateRulePursuantThisScheduleFiled", "ruleFiled") or None,
                amendment_no=_first(x, "amendmentNo") or None, source="edgar_xml", ingested_at=_stamp())


def _index_entries(get, day):
    r = get(f"https://www.sec.gov/Archives/edgar/daily-index/{day.year}/QTR{(day.month - 1) // 3 + 1}/form.{day:%Y%m%d}.idx", sec=True)
    out = []
    for line in (r.text.splitlines() if r is not None else []):
        m = IDX_LINE.match(line)
        if not m: continue
        form = m.group(1).strip()
        if form in ALL_FORMS:
            out.append((form, m.group(3).lstrip("0"), _date(m.group(4)), m.group(5)))
    return out


def pull_daily(now, get, bq=None, days=5, max_filings=4000, workers=6, verbose=False):
    """Form D, 13F, Form 144 and Schedule 13G filings from the last `days` daily form indexes that are not loaded yet.
    Returns (out, note) for main.run()."""
    entries = []
    for i in range(1, days + 1):
        day = (now - dt.timedelta(days=i)).date()
        if day.weekday() >= 5: continue
        try: entries += _index_entries(get, day)
        except Exception: continue
    out, note = process(entries, get, bq, max_filings=max_filings, workers=workers, verbose=verbose)
    if bq is not None:   # the monthly ADV snapshot: one page read a day, a load only when a new month is posted
        try:
            n = load_adv(bq, newest_only=True)
            if n: note += f"; ADV: {n} firms"; out["_direct_rows"] = out.get("_direct_rows", 0) + n
        except Exception as e:
            note += f"; ADV check failed: {type(e).__name__}"
    return out, note


def process(entries, get, bq=None, max_filings=4000, workers=6, verbose=False):
    """Fetch each new filing's full submission text and parse its XML. Fetches run on `workers` threads under one shared
    throttle (SEC fair access: <= 8 requests/second); rows are written every 500 filings, so a stopped run keeps what it
    finished and a rerun skips it. With bq the rows are written; without it (dry run) returned."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    have = _loaded(bq, min((e[2] for e in entries if e[2]), default=None)) if bq is not None else set()
    seen, todo = set(have), []
    for e in entries:   # a filing with several issuers / filers is listed once per CIK: fetch it once
        a = _acc(e[3])
        if a not in seen: seen.add(a); todo.append(e)
    todo = todo[:max_filings]
    if verbose: print(f"{len(entries)} index entries, {len(todo)} to fetch", flush=True)
    lock, last = threading.Lock(), [0.0]

    def one(e):
        form, cik, filed, path = e
        with lock:   # one request every 0.13 s across all threads
            wait = 0.13 - (time.time() - last[0])
            if wait > 0: time.sleep(wait)
            last[0] = time.time()
        try:
            r = get("https://www.sec.gov/Archives/" + path, sec=False)
            if r is None: return None
            acc = _acc(path)
            if form in FORMD_FORMS: return ("formd", [parse_formd(acc, form, filed, r.text)], [])
            if form in F144_FORMS: return ("form144", parse_144(acc, form, filed, r.text), [])
            if form in F13G_FORMS: return ("form13g", [parse_13g(acc, form, filed, r.text)], [])
            f, h = parse_13f(acc, form, filed, cik, r.text)
            if f is None: return ("form13f_filings", [], [])
            t, fx = _units(f, h)
            return ("form13f_filings", [f], h, t, fx)
        except Exception:
            return ("x", [], [])

    out = {k: [] for k in ("formd", "form13f_filings", "form13f_holdings", "form144", "form13g", "form13f_totals",
                           "form13f_unit_fixes")}
    counts = {k: 0 for k in out}; failed = written = 0
    with ThreadPoolExecutor(workers) as ex:
        for k in range(0, len(todo), 500):
            batch = {kk: [] for kk in out}
            for res in ex.map(one, todo[k:k + 500]):
                if res is None: continue
                kind, rows, h = res[:3]
                if kind == "x": failed += 1; continue
                batch[kind] += [r for r in rows if r]; batch["form13f_holdings"] += h
                if len(res) == 5:
                    batch["form13f_totals"].append(res[3]); batch["form13f_unit_fixes"] += res[4]
            for kk, rows in batch.items(): counts[kk] += len(rows)
            if bq is not None:   # holdings before filings: a filing row marks its holdings as complete
                for kk in ("formd", "form144", "form13g", "form13f_holdings", "form13f_totals", "form13f_unit_fixes",
                           "form13f_filings"):
                    written += append(bq, kk, batch[kk])
            else:
                for kk, rows in batch.items(): out[kk] += rows
            if verbose:
                print(f"  {min(k + 500, len(todo))}/{len(todo)}: {counts['formd']} Form D, {counts['form13f_filings']} 13F "
                      f"({counts['form13f_holdings']:,} holdings), {counts['form144']} Form 144 lines, "
                      f"{counts['form13g']} 13G, {failed} failed", flush=True)
    note = (f"{len(entries)} index entries, {len(todo)} new: {counts['formd']} Form D, {counts['form13f_filings']} 13F "
            f"({counts['form13f_holdings']} holdings), {counts['form144']} Form 144 lines, {counts['form13g']} 13G, {failed} failed")
    if bq is None: return out, note
    return {"_direct_rows": written}, note


def history_index(kinds, since=(2023, 2), dry=False, workers=6):
    """One-off: every Form 144 / Schedule 13G filing listed in the quarterly full-index form.idx files since `since`
    (year, quarter), quarter by quarter; filings already loaded are skipped, so a rerun resumes."""
    forms = set().union(*[{"144": F144_FORMS, "13g": F13G_FORMS}[k] for k in kinds])
    bq = None if dry else _bq()
    today = dt.date.today()
    y, q = since
    while (y, q) <= (today.year, (today.month - 1) // 3 + 1):
        r = _get(f"https://www.sec.gov/Archives/edgar/full-index/{y}/QTR{q}/form.idx")
        entries = []
        for line in (r.text.splitlines() if r is not None else []):
            m = IDX_LINE.match(line)
            if m and m.group(1).strip() in forms:
                entries.append((m.group(1).strip(), m.group(3).lstrip("0"), _date(m.group(4)), m.group(5)))
        print(f"{y}Q{q}: {len(entries)} filings listed", flush=True)
        if dry:
            entries = entries[-20:]   # the newest filings in the quarter
            if entries:   # print every XML tag of one filing, to check the field names the parser reads
                r1 = _get("https://www.sec.gov/Archives/" + entries[-1][3])
                for root in (_xml_blocks(r1.text) if r1 is not None else []):
                    print(f"  tags in {entries[-1][0]} {root.tag}: {sorted({el.tag for el in root.iter()})}"[:3000])
        out, note = process(entries, lambda url, sec=False, **kw: _get(url), bq, max_filings=10**6, workers=workers, verbose=True)
        if dry:
            for kk in ("form144", "form13g"):
                if out[kk]: print(f"  sample {kk}: {out[kk][0]}")
        q += 1
        if q == 5: y, q = y + 1, 1



# ------------------------------------------------------------------ Form ADV: SEC-registered and exempt reporting advisers
# The SEC posts a monthly snapshot of every adviser's Form ADV Part 1 (Items 1-11): one file of registered advisers and
# one of exempt reporting advisers (mostly VC and private fund managers), each a zip holding a spreadsheet whose column
# headings are Form ADV item numbers (5A = employees, 5F(2)(a)-(f) = regulatory AUM and accounts). Spreadsheets (.xlsx)
# from 2019 on load; older .xls / PDF files are skipped. crd = the adviser's CRD number, the key 13F cover pages carry.
ADV_LIST = "https://www.sec.gov/help/foiadocsinvafoiahtm.html"


def adv_links():
    """Every monthly ADV zip on the SEC page: (as_of date, kind, url), oldest first."""
    r = _get(ADV_LIST)
    if r is None: raise RuntimeError("ADV page not found")
    out = []
    # zips (2019-May 2023 and Dec 2025 on) and bare .xlsx files (June 2023-Sep 2025); 'ia-no-data' PDFs are skipped
    for h in sorted(set(re.findall(r'href="([^"]+?\.(?:zip|xlsx))"', r.text, re.I))):
        if "information-about-registered-investment-advisers" not in h and "/ia" not in h.lower(): continue
        name = h.rsplit("/", 1)[1].lower()
        m = re.match(r"ia(\d{6,8})", name)
        if not m: continue
        d = m.group(1)
        try:
            as_of = dt.datetime.strptime(d, "%m%d%Y" if len(d) == 8 else "%m%d%y").date().isoformat()
        except ValueError:
            continue
        url = h if h.startswith("http") else "https://www.sec.gov" + h
        out.append((as_of, "exempt" if "exempt" in name else "registered", url))
    return sorted(out)


def _norm(h):
    return re.sub(r"\s+", " ", str(h or "")).strip().lower()


def adv_rows(path, as_of, kind):
    """Rows of one monthly ADV spreadsheet (inside the zip). Columns are found by heading, whichever order they come in."""
    stamp = _stamp()
    if path.lower().endswith(".xlsx"):   # posted as a bare spreadsheet, not zipped
        import openpyxl
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        return _adv_from_rows(wb.worksheets[0].iter_rows(values_only=True), as_of, kind, stamp)
    with zipfile.ZipFile(path) as z:
        members = z.namelist()
        xlsx = [n for n in members if n.lower().endswith(".xlsx")]
        csvs = [n for n in members if n.lower().endswith((".csv", ".txt"))]
        if xlsx:
            import openpyxl
            with z.open(xlsx[0]) as fh:
                wb = openpyxl.load_workbook(io.BytesIO(fh.read()), read_only=True, data_only=True)
            it = wb.worksheets[0].iter_rows(values_only=True)
        elif csvs:   # newer months come as CSV
            raw = z.read(csvs[0])
            text = raw.decode("utf-8-sig", errors="replace") if raw[:3] == b"\xef\xbb\xbf" else raw.decode("latin-1")
            it = iter(list(csv.reader(io.StringIO(text))))
        else:
            print(f"  zip holds {members[:5]}: no .xlsx or .csv", flush=True)
            return None
    return _adv_from_rows(it, as_of, kind, stamp)


def _adv_from_rows(it, as_of, kind, stamp):
    head = None
    for row in it:   # the heading row is the first one naming the CRD column
        if any("crd" in _norm(c) for c in row): head = [_norm(c) for c in row]; break
    if head is None: return []
    def col(*keys):
        for k in keys:
            for i, h in enumerate(head):
                if h == k or h.startswith(k): return i
        return None
    ix = dict(crd=col("organization crd", "crd"), sec=col("sec#", "sec number"), cik=col("cik#", "cik"),
              firm=col("primary business name", "business name"), legal=col("legal name"),
              city=col("main office city"), state=col("main office state"), country=col("main office country"),
              filed=col("latest adv filing date", "adv filing date"), web=col("website address", "website"),
              emp=col("5a"), ad=col("5f(2)(a)"), an=col("5f(2)(b)"), at=col("5f(2)(c)"),
              cd=col("5f(2)(d)"), cn=col("5f(2)(e)"), ct=col("5f(2)(f)"))
    def v(row, k):
        i = ix.get(k)
        return row[i] if i is not None and i < len(row) else None
    def s_(x):
        return None if x is None else (x.date().isoformat() if hasattr(x, "date") else str(x).strip() or None)
    def d_(x):
        if x is None: return None
        if hasattr(x, "date"): return x.date().isoformat()
        return _date(str(x))
    out = []
    for row in it:
        crd = s_(v(row, "crd"))
        if not crd: continue
        out.append(dict(as_of=as_of, kind=kind, crd=crd.split(".")[0], sec_number=s_(v(row, "sec")),
                        cik=(s_(v(row, "cik")) or "").split(".")[0].lstrip("0") or None, firm_name=s_(v(row, "firm")),
                        legal_name=s_(v(row, "legal")), city=s_(v(row, "city")), state=s_(v(row, "state")),
                        country=s_(v(row, "country")), adv_filing_date=d_(v(row, "filed")), website=s_(v(row, "web")),
                        employees=_num(s_(v(row, "emp"))), aum_discretionary=_num(s_(v(row, "ad"))),
                        aum_nondiscretionary=_num(s_(v(row, "an"))), aum_total=_num(s_(v(row, "at"))),
                        accounts_discretionary=_num(s_(v(row, "cd"))), accounts_nondiscretionary=_num(s_(v(row, "cn"))),
                        accounts_total=_num(s_(v(row, "ct"))), source_file=None, ingested_at=stamp))
    return out


def load_adv(bq=None, dry=False, newest_only=False, since="2019-01-01"):
    """Every monthly ADV spreadsheet not loaded yet (as_of + kind), oldest first; newest_only for the daily check."""
    links = [l for l in adv_links() if l[0] >= since]
    if newest_only: links = [l for l in links if l[0] == max(x[0] for x in links)] if links else []
    if dry: links = links[-2:]
    have = set()
    if bq is not None:
        have = {(str(r.a), r.k) for r in bq.query(f"SELECT DISTINCT as_of AS a, kind AS k FROM `{PROJECT}.{TABLES['adv_firms']}`").result()}
    loaded = 0
    for as_of, kind, url in links:
        if (as_of, kind) in have: continue
        path = _download(url)
        if path is None: print(f"  adv {as_of} {kind}: not found"); continue
        try:
            rows = adv_rows(path, as_of, kind)
            for r in rows or []: r["source_file"] = url.rsplit("/", 1)[1]
            if rows is None: print(f"  adv {as_of} {kind}: no spreadsheet in the zip, skipped", flush=True); continue
            if dry:
                print(f"  adv {as_of} {kind}: {len(rows)} firms; sample: {rows[0] if rows else None}"); continue
            loaded += append(bq, "adv_firms", rows)
            print(f"  adv {as_of} {kind}: {len(rows)} firms", flush=True)
        except Exception as e:
            print(f"  adv {as_of} {kind}: FAILED {type(e).__name__}: {e}"[:300], flush=True)
        finally:
            try: os.remove(path)
            except OSError: pass
    return loaded


# ------------------------------------------------------------------ BigQuery
def _bq():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT)


def _loaded(bq, since=None, kinds=("formd", "form13f_filings", "form144", "form13g")):
    """Accessions already in the tables (filed on or after `since`, when given)."""
    have = set()
    for k in kinds:
        w = f" WHERE filed >= DATE '{since}'" if since else ""
        have |= {r.a for r in bq.query(f"SELECT DISTINCT accession AS a FROM `{PROJECT}.{TABLES[k]}`{w}").result()}
    return have


def append(bq, table, rows):
    """Append rows through a gzip NDJSON temp file (one load job, low memory)."""
    from google.cloud import bigquery
    rows = list(rows) if not isinstance(rows, list) else rows
    if not rows: return 0
    return _load_file(bq, table, iter(rows))


def _load_file(bq, table, it):
    from google.cloud import bigquery
    cols = [c for c, _ in SCHEMAS[table]]
    n = 0
    with tempfile.NamedTemporaryFile(suffix=".json.gz", delete=False) as tmp:
        with gzip.open(tmp, "wt", encoding="utf-8") as gz:
            for r in it:
                gz.write(json.dumps({c: r.get(c) for c in cols}, separators=(",", ":")) + "\n"); n += 1
        name = tmp.name
    try:
        if n:
            cfg = bigquery.LoadJobConfig(schema=[bigquery.SchemaField(c, t) for c, t in SCHEMAS[table]],
                                         source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                         write_disposition="WRITE_APPEND")
            with open(name, "rb") as f:
                bq.load_table_from_file(f, f"{PROJECT}.{TABLES[table]}", job_config=cfg).result()
    finally:
        os.remove(name)
    return n


def setup(bq, project=PROJECT):
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    for name, loc in TABLES.items():
        ref = f"{project}.{loc}"
        try: bq.get_table(ref); print(f"  {loc} exists"); continue
        except NotFound: pass
        t = bigquery.Table(ref, schema=[bigquery.SchemaField(c, ty) for c, ty in SCHEMAS[name]])
        col, unit = PARTITION.get(name, (None, None))
        if col: t.time_partitioning = bigquery.TimePartitioning(type_=getattr(bigquery.TimePartitioningType, unit), field=col)
        t.clustering_fields = CLUSTER[name]
        bq.create_table(t); print(f"  created {loc}")


# ------------------------------------------------------------------ history (PC)
def history(kind, dry=False, since=0):
    sets = [(n, u) for n, u in dataset_links(kind) if _set_order(n)[0] >= since]
    if dry: sets = sets[-1:]
    bq = None if dry else _bq()
    done = set()
    if bq is not None:   # data sets already loaded, from ops.feeds_runs notes
        done = {r.n for r in bq.query(f"SELECT DISTINCT note AS n FROM `{PROJECT}.ops.feeds_runs` "
                                      f"WHERE source = '{kind}_history' AND ok").result()}
    redo = set(sys.argv[sys.argv.index("--redo") + 1].split(",")) if "--redo" in sys.argv else set()
    if "all-null-dates" in redo and bq is not None:   # the Form D sets whose dates did not parse on the first load
        redo = {r.n for r in bq.query(f"SELECT DISTINCT note AS n FROM `{PROJECT}.ops.feeds_runs` WHERE source = '{kind}_history' "
                                      f"AND REGEXP_CONTAINS(note, r'^(2008|2009|201[0-9]|2020q[12])')").result()}
    done -= redo
    print(f"{kind}: {len(sets)} data sets listed, {len(done)} already loaded, {len(redo)} to redo", flush=True)
    # 13F: filings whose holdings are already in the table count as loaded (an Oct 23 restart between a set's holdings
    # and its filings loaded 1.77M holdings twice)
    have_h = set()
    if bq is not None and kind == "13f":
        have_h = {r.a for r in bq.query(f"SELECT DISTINCT accession AS a FROM `{PROJECT}.{TABLES['form13f_holdings']}`").result()}
    for name, url in sets:
        if name in done: continue
        t0 = time.time(); err = None; n = 0
        path = _download(url)
        if path is None: print(f"  {name}: not found"); continue
        try:
            have = _loaded(bq) if bq is not None else set()
            if kind == "formd":
                rows = formd_set_rows(path, have)
                if dry:
                    print(f"  {name}: {len(rows)} filings; columns read OK; sample: {rows[0] if rows else None}"); continue
                bad = sum(1 for r in rows if not r["filed"])
                if bad: raise RuntimeError(f"{bad} of {len(rows)} filing dates did not parse (sample: {rows[0]})"[:400])
                n = append(bq, "formd", rows)
                print(f"  {name}: {n} Form D filings", flush=True)
            else:
                filings, holdings = f13_set_rows(path, have | have_h)
                bad = [r["_raw"] for r in filings if not r["filed"] or not r["period"]]
                if bad: raise RuntimeError(f"{len(bad)} of {len(filings)} filing/period dates did not parse; raw values: {sorted(set(bad))[:6]}"[:500])
                if dry:
                    it = holdings(); h = [next(it) for _ in range(3)]; it.close()
                    print(f"  {name}: {len(filings)} filings; sample filing: {filings[0] if filings else None}; sample holdings: {h}")
                    continue
                nh = _load_file(bq, "form13f_holdings", holdings())
                have_h |= {f["accession"] for f in filings}
                for kk, rows in holdings.extras.items(): append(bq, kk, rows)
                n = append(bq, "form13f_filings", filings)   # filings last
                print(f"  {name}: {n} 13F filings, {nh:,} holdings", flush=True)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:500]; print(f"  {name}: FAILED {err}", flush=True)
        finally:
            try: os.remove(path)
            except OSError: pass   # a file Windows still holds is left in %TEMP%
        if bq is not None:
            from google.cloud import bigquery
            cfg = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=[bigquery.SchemaField(c, t) for c, t in (
                ("ts", T), ("source", St), ("ok", B), ("row_count", I), ("seconds", F), ("note", St), ("error", St))])
            bq.load_table_from_json([dict(ts=_stamp(), source=f"{kind}_history", ok=err is None, row_count=n,
                                          seconds=round(time.time() - t0, 1), note=name, error=err)],
                                    f"{PROJECT}.ops.feeds_runs", job_config=cfg).result()


if __name__ == "__main__":
    dry = "--dry" in sys.argv
    since = int(sys.argv[sys.argv.index("--since") + 1]) if "--since" in sys.argv else 0
    if "--formd-history" in sys.argv: history("formd", dry, since)
    elif "--13f-history" in sys.argv: history("13f", dry, since)
    elif "--144-history" in sys.argv or "--13g-history" in sys.argv:
        kinds = (["144"] if "--144-history" in sys.argv else []) + (["13g"] if "--13g-history" in sys.argv else [])
        start = (2023, 2) if kinds == ["144"] else (2024, 4)
        if "--since" in sys.argv: start = (int(sys.argv[sys.argv.index("--since") + 1]), 1)
        history_index(kinds, start, dry)
    elif "--adv-history" in sys.argv:
        print(f"adv history: {load_adv(None if dry else _bq(), dry)} firm rows loaded")
    elif "--daily" in sys.argv:
        import requests

        def _g2(url, sec=False, **kw):
            return _get(url)
        days = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 5
        out, note = pull_daily(dt.datetime.now(dt.timezone.utc), _g2, None if dry else _bq(), days=days, max_filings=50000, verbose=True)
        print(note)
        if dry:
            for k, v in out.items(): print(f"  {k}: {len(v)} rows; first: {v[0] if v else None}")
    elif "--setup" in sys.argv: setup(_bq())
    else: print(__doc__)
