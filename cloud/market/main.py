"""RRP market-data ingest -> BigQuery dataset `markets` (run log in `ops.market_runs`).

Runs as the Cloud Run job `rrp-market`. A failing source never stops the others, and the job always exits 0 so
Cloud Run does not retry and double-insert snapshot rows. The rrp.* views over these tables are built by
warehouse/build.py, not here. Schedules (Cloud Scheduler, see deploy.ps1):
  rrp-market-15m           every 30 min   crypto spot, hourly/daily OHLC + indicators, derivatives, global, trending,
                                          BTC fees; first run of each hour also stablecoins, Fear & Greed, macro
                                          (FRED, Treasury, Cboe, Yahoo), the CoinPaprika top-2,000 snapshot and
                                          Farside spot ETF flows (BTC, ETH, SOL); second run of each hour (:30) also
                                          hourly candles for every Coinbase USD product, last 6 hours (added 2026-10-27)
  rrp-market-equities      22:20 UTC M-F  --equities-only: US symbol directory + daily bars for rrp.equity_coverage,
                                          FINRA daily short volume (last 5 sessions) and short interest (new files),
                                          Treasury Daily Treasury Statement (last 14 days), EIA grid demand + fuel mix,
                                          EIA weekly gas storage + petroleum status, NOAA degree days,
                                          ETF shares outstanding + NAV (SPDR and iShares, for daily ETF flows)
  rrp-market-crypto-daily  00:15 UTC      --crypto-daily: daily candles for every Kraken and Coinbase USD pair, then
                                          Kraken perpetual funding (last 3 days)

  python main.py                      # one 15-minute run          python main.py --dry   # pull only, write nothing
  python main.py --setup              # create any missing table
  python main.py --equities-backfill  # first load / extend short histories: 5 years per covered ticker
  python main.py --equities-deep      # extend every covered ticker back to 2006 (older bars only, staged, then one insert)
  python main.py --equities-deep-commit   # just the insert, if the quota blocked it
  python main.py --macro-backfill     # full daily macro history from 2000
  python main.py --crypto-backfill    # every venue pair's full daily history
  python main.py --dry --only farside_etf   # test one source (any puller name) without writing
  python main.py --only farside_history     # one-off backfills, run from the PC (never in scheduled runs):
  python main.py --only funding_history     #   Kraken perps (about a year) + Deribit BTC/ETH funding since 2019
  python main.py --only fear_greed_history  #   Fear & Greed every day since Feb 2018
  python main.py --dry --only funding       # the daily pulls can be tested the same way (funding)
  python main.py --dry --only short_volume  # FINRA Reg SHO daily short volume, last 5 sessions (added 2026-10-23)
  python main.py --dry --only short_interest   # FINRA short interest, files for settlement dates in the last 45 days
  python main.py --only short_volume_history   # one-off: daily short volume since 2018-08-01, appended month by month
  python main.py --only short_interest_history # one-off: every short interest file since 2014 (listed stocks from Jun 2021)
  python main.py --dry --only treasury_dts     # Daily Treasury Statement, last 14 days (prints each table's fields)
  python main.py --only treasury_dts_history   # one-off: every DTS table since Oct 2005, a year at a time
  python main.py --dry --only eia              # EIA grid: daily (all) + hourly (regions); needs $env:EIA_API_KEY
  python main.py --only eia_history            # one-off: EIA daily demand + fuel mix since July 2015, a quarter at a time
  python main.py --dry --only eia_weekly       # EIA weekly gas storage + petroleum status, last 70 days (prints fields)
  python main.py --only eia_weekly_history     # one-off: those weekly series since 2000
  python main.py --only fred_liquidity_history # one-off: Fed assets, bank reserves, ON RRP since 2003
  python main.py --dry --only degree_days      # NOAA CPC heating / cooling degree days, this year
  python main.py --only degree_days_history    # one-off: every year since 2000
  python main.py --dry --only etf_shares       # ETF shares outstanding + NAV: SPDR last 10 days, every iShares ETF today
  python main.py --only etf_shares_history     # one-off: full SPDR, GLD and VanEck (GDX, GDXJ, SMH) history
  python main.py --dry --only crypto_hourly    # hourly candles, every Coinbase USD product, last 6 hours (prints a count)
  python main.py --only crypto_hourly_history  # one-off: the same for the last 45 days ; add --days 500 for a longer history
  python main.py --dry --only crypto_orderbook # order-book snapshot, top 150 Coinbase USD coins (prints a count + 3 samples)
"""
import datetime as dt, html, io, json, math, os, re, sys, time
from html.parser import HTMLParser
import requests
import bq_cap  # noqa: F401  hard cap on BigQuery query bytes (b247)

PROJECT = os.environ.get("RRP_PROJECT", "rrp-technologies")
# where each table lives (key = the name used in this file); MERGE scratch tables go to `staging` (expire after a day)
TABLES = {"spot": "markets.crypto_spot", "ohlc": "markets.crypto_ohlc", "indicators_daily": "markets.crypto_indicators_daily",
          "derivs": "markets.crypto_derivs", "global": "markets.crypto_global", "trending": "markets.crypto_trending",
          "fear_greed": "markets.crypto_fear_greed", "btc_fees": "markets.btc_fees", "stablecoins": "markets.stablecoins",
          "crypto_market": "markets.crypto_market",
          "crypto_daily": "markets.crypto_daily", "macro": "markets.macro", "equity_daily": "markets.equity_daily",
          "equity_universe": "markets.equity_universe", "etf_flows": "markets.etf_flows", "funding": "markets.funding_rates",
          "short_volume": "markets.short_volume", "short_interest": "markets.short_interest",
          "treasury_dts": "gov.treasury_dts", "eia_grid": "gov.eia_grid", "eia_weekly": "gov.eia_weekly",
          "degree_days": "gov.degree_days", "etf_shares": "markets.etf_shares", "crypto_hourly": "markets.crypto_hourly",
          "crypto_orderbook": "markets.crypto_orderbook", "runs": "ops.market_runs"}
UA = {"User-Agent": "RRP Technologies research bot (market ingest)"}
KRAKEN_PAIRS = ["XBTUSD", "ETHUSD", "SOLUSD", "ARBUSD", "AAVEUSD", "UNIUSD", "LINKUSD", "TAOUSD",
                "FETUSD", "ZECUSD", "PYTHUSD", "HYPEUSD"]
KRAKEN_KEY = {"XXBTZUSD": "BTC", "XETHZUSD": "ETH", "XZECZUSD": "ZEC"}   # Kraken's legacy result keys
DERIBIT = ["BTC-PERPETUAL", "ETH-PERPETUAL"]
S = requests.Session(); S.headers.update(UA)


def sym(pair_key):
    return KRAKEN_KEY.get(pair_key) or ("BTC" if pair_key.startswith("XBT") else pair_key.replace("USD", ""))


def get(url, **kw):
    for i in range(3):
        try:
            r = S.get(url, timeout=20, **kw)
            if r.status_code in (429, 502, 503, 504): time.sleep(3 * (i + 1)); continue
            r.raise_for_status(); return r.json()
        except requests.RequestException:
            if i == 2: raise
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"gave up on {url}")


def f(x):
    try:
        v = float(x); return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def ts_iso(t):
    return dt.datetime.fromtimestamp(float(t), dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------ pullers (each returns {table: rows})
def pull_kraken_ticker(now):
    d = get("https://api.kraken.com/0/public/Ticker", params={"pair": ",".join(KRAKEN_PAIRS)})
    if d.get("error"): raise RuntimeError(d["error"])
    rows = []
    for k, v in d["result"].items():
        rows.append(dict(ts=now, source="kraken", symbol=sym(k), pair=k, last=f(v["c"][0]), bid=f(v["b"][0]),
                         ask=f(v["a"][0]), open_utc_day=f(v["o"]), high_24h=f(v["h"][1]), low_24h=f(v["l"][1]),
                         volume_24h=f(v["v"][1]), vwap_24h=f(v["p"][1]), trades_24h=int(v["t"][1]), exchange_time=None))
    return {"spot": rows}


def pull_coinbase(now):
    rows = []
    for base in ("BTC", "HYPE", "ETH"):
        d = get(f"https://api.coinbase.com/v2/prices/{base}-USD/spot")["data"]
        rows.append(dict(ts=now, source="coinbase_retail", symbol=base, pair=f"{base}-USD", last=f(d["amount"]),
                         bid=None, ask=None, open_utc_day=None, high_24h=None, low_24h=None, volume_24h=None,
                         vwap_24h=None, trades_24h=None, exchange_time=None))
    for base in ("BTC", "ETH"):
        d = get(f"https://api.exchange.coinbase.com/products/{base}-USD/ticker")
        et = d.get("time", "")[:19].replace("T", " ") or None
        rows.append(dict(ts=now, source="coinbase_exchange", symbol=base, pair=f"{base}-USD", last=f(d["price"]),
                         bid=f(d.get("bid")), ask=f(d.get("ask")), open_utc_day=None, high_24h=None, low_24h=None,
                         volume_24h=f(d.get("volume")), vwap_24h=None, trades_24h=None, exchange_time=et))
    return {"spot": rows}


def pull_coinbase_trending(now):
    d = get("https://api.coinbase.com/v2/assets/search", params={"filter": "trending", "resolution": "day", "limit": 10})["data"]
    rows = []
    for i, a in enumerate(d, 1):
        pc = (a.get("latest_price") or {}).get("percent_change") or {}
        rows.append(dict(ts=now, list_rank=i, symbol=a.get("symbol"), name=a.get("name"), market_cap_rank=a.get("rank"),
                         market_cap_usd=f(a.get("market_cap")), price_usd=f(a.get("latest")), volume_24h_usd=f(a.get("volume_24h")),
                         pct_change_hour=f(pc.get("hour")), pct_change_day=f(pc.get("day")), pct_change_week=f(pc.get("week"))))
    return {"trending": rows}


FULL_OHLC = [False]   # set per run: first run of each hour pulls full windows


def pull_kraken_ohlc(now):
    """First run of each hour: full 720-bar windows (daily ~2 years, hourly 30 days) and the daily
    indicators on the full series. Other runs: only the last 3 days of bars. MERGE keeps one row per bar
    and refreshes the bar that is still open."""
    bars, ind = [], []
    since = None if FULL_OHLC[0] else int(time.time()) - 3 * 86400
    for pair in KRAKEN_PAIRS:
        for interval in (1440, 60):
            time.sleep(1.1)   # Kraken public limit
            params = {"pair": pair, "interval": interval}
            if since: params["since"] = since
            d = get("https://api.kraken.com/0/public/OHLC", params=params)
            if d.get("error"): raise RuntimeError(f"{pair}: {d['error']}")
            key = next(k for k in d["result"] if k != "last")
            s, series = sym(key), d["result"][key]
            for b in series:
                bars.append(dict(source="kraken", symbol=s, interval_min=interval, bar_start=ts_iso(b[0]),
                                 open=f(b[1]), high=f(b[2]), low=f(b[3]), close=f(b[4]), vwap=f(b[5]),
                                 volume=f(b[6]), trades=int(b[7]), is_open=b is series[-1], ingested_at=now))
            if interval == 1440 and since is None:
                ind += indicators(s, series[:-1])   # completed daily bars only
    return {"ohlc": bars, "indicators_daily": ind}


def indicators(symbol, series):
    closes = [float(b[4]) for b in series]
    out, gain, loss = [], None, None
    for i, b in enumerate(series):
        c = closes[i]
        ma20 = sum(closes[i - 19:i + 1]) / 20 if i >= 19 else None
        ma50 = sum(closes[i - 49:i + 1]) / 50 if i >= 49 else None
        rsi = None
        if i >= 1:
            ch = c - closes[i - 1]; g, l = max(ch, 0.0), max(-ch, 0.0)
            if i < 14:
                gain = (gain or 0) + g; loss = (loss or 0) + l
            elif i == 14:
                gain = (gain + g) / 14; loss = (loss + l) / 14
            else:                               # Wilder smoothing
                gain = (gain * 13 + g) / 14; loss = (loss * 13 + l) / 14
            if i >= 14:
                rsi = 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
        out.append(dict(symbol=symbol, date=ts_iso(b[0])[:10], close=c, ma20=ma20, ma50=ma50,
                        rsi14=round(rsi, 4) if rsi is not None else None))
    return out[-400:]   # plenty of overlap; MERGE dedupes


def pull_deribit(now):
    rows = []
    for inst in DERIBIT:
        r = get("https://www.deribit.com/api/v2/public/ticker", params={"instrument_name": inst})["result"]
        st = r.get("stats") or {}
        rows.append(dict(ts=now, venue="deribit", instrument=inst, symbol=inst.split("-")[0], mark=f(r.get("mark_price")),
                         index_price=f(r.get("index_price")), last=f(r.get("last_price")), funding_8h=f(r.get("funding_8h")),
                         current_funding=f(r.get("current_funding")), open_interest_usd=f(r.get("open_interest")),
                         volume_24h_usd=f(st.get("volume_usd")), high_24h=f(st.get("high")), low_24h=f(st.get("low")),
                         price_change_24h_pct=f(st.get("price_change")), exchange_time=ts_iso(r["timestamp"] / 1000)))
    return {"derivs": rows}


def pull_global(now):
    g = get("https://api.coinpaprika.com/v1/global")
    return {"global": [dict(ts=now, source="coinpaprika", market_cap_usd=f(g.get("market_cap_usd")),
                            volume_24h_usd=f(g.get("volume_24h_usd")), btc_dominance_pct=f(g.get("bitcoin_dominance_percentage")),
                            market_cap_change_24h_pct=f(g.get("market_cap_change_24h")),
                            volume_change_24h_pct=f(g.get("volume_24h_change_24h")),
                            source_updated=ts_iso(g["last_updated"]) if g.get("last_updated") else None)]}


def pull_fng(now):
    d = get("https://api.alternative.me/fng/", params={"limit": 30})["data"]
    return {"fear_greed": [dict(date=ts_iso(x["timestamp"])[:10], value=int(x["value"]),
                                classification=x.get("value_classification"), ingested_at=now) for x in d]}


def pull_fng_history(now):
    """One-off: the whole index since Feb 2018 (limit=0 returns every day). --only fear_greed_history"""
    d = get("https://api.alternative.me/fng/", params={"limit": 0})["data"]
    return {"fear_greed": [dict(date=ts_iso(x["timestamp"])[:10], value=int(x["value"]),
                                classification=x.get("value_classification"), ingested_at=now) for x in d]}


def pull_fees(now):
    r = S.get("https://mempool.space/api/v1/fees/recommended", timeout=8)   # one try, short: it averaged 58 s with retries
    r.raise_for_status(); d = r.json()
    return {"btc_fees": [dict(ts=now, fastest=f(d.get("fastestFee")), half_hour=f(d.get("halfHourFee")),
                              hour=f(d.get("hourFee")), economy=f(d.get("economyFee")), minimum=f(d.get("minimumFee")))]}


def pull_stablecoins(now):
    d = get("https://stablecoins.llama.fi/stablecoins", params={"includePrices": "false"})["peggedAssets"]
    rows = []
    for a in d:
        peg = a.get("pegType") or ""
        k = peg if peg else "peggedUSD"
        val = lambda fld: f((a.get(fld) or {}).get(k))
        rows.append(dict(ts=now, id=str(a.get("id")), symbol=a.get("symbol"), name=a.get("name"), peg_type=peg,
                         peg_mechanism=a.get("pegMechanism"), circulating=val("circulating"), prev_day=val("circulatingPrevDay"),
                         prev_week=val("circulatingPrevWeek"), prev_month=val("circulatingPrevMonth")))
    return {"stablecoins": rows}


# ------------------------------------------------------------------ spot ETF flows (Farside, added 2026-10-12)
# Farside publishes one table per asset: a row per US trading day, a column per fund, values in US$ millions,
# outflows in parentheses, "-" for no flow. Stored long: one row per (asset, date, fund), plus fund "TOTAL".
# MERGE on those keys, so each hourly pull refreshes the newest days (Farside fills late funds in) and re-pulled
# history is a no-op. Farside sits behind Cloudflare, so this source sends a browser User-Agent.
FARSIDE = {"BTC": "https://farside.co.uk/btc/", "ETH": "https://farside.co.uk/eth/", "SOL": "https://farside.co.uk/sol/"}
FARSIDE_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/129.0 Safari/537.36", "Accept": "text/html,application/xhtml+xml"}


class _Tables(HTMLParser):
    """Every <table> on the page as a list of rows of cell text (a colspan=n cell is repeated n times)."""
    def __init__(self):
        super().__init__(); self.tables, self._row, self._cell, self._span = [], None, None, 1
    def handle_starttag(self, tag, attrs):
        if tag == "table": self.tables.append([])
        elif tag == "tr" and self.tables: self._row = []
        elif tag in ("td", "th") and self._row is not None:
            self._cell = []
            try: self._span = max(1, int(dict(attrs).get("colspan") or 1))
            except ValueError: self._span = 1
    def handle_endtag(self, tag):
        if tag in ("td", "th") and self._cell is not None and self._row is not None:
            self._row.extend([" ".join("".join(self._cell).split())] * self._span); self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row and self.tables: self.tables[-1].append(self._row)
            self._row = None
    def handle_data(self, data):
        if self._cell is not None: self._cell.append(data)


def _farside_date(s):
    for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y"):
        try: return dt.datetime.strptime(s.strip(), fmt).date().isoformat()
        except ValueError: pass
    return None


def _farside_num(s):
    s = s.strip().replace(",", "").replace("$", "")
    if s == "": return None            # not reported yet: left out, filled by a later pull
    if s in ("-", "\u2013", "\u2014"): return 0.0   # Farside's "no flow"
    neg = s.startswith("(") and s.endswith(")")
    v = f(s.strip("()"))
    return None if v is None else (-v if neg else v)


_TICKER = re.compile(r"^[A-Z][A-Z0-9]{1,5}$")


def _farside_funds(head_rows, width):
    """Column names from the header rows above the first dated row. Farside stacks issuer, ticker and fee rows,
    so per column take the ticker-looking cell; the Total column becomes TOTAL; anything else is skipped."""
    funds = []
    for i in range(1, width):
        cells = [r[i].strip() for r in head_rows if i < len(r) and r[i].strip()]
        if any(c.lower() == "total" for c in cells): funds.append("TOTAL"); continue
        tick = [c for c in cells if _TICKER.match(c)]
        funds.append(tick[-1] if tick else "")
    return funds


FARSIDE_HISTORY = {"BTC": "https://farside.co.uk/bitcoin-etf-flow-all-data/",
                   "ETH": "https://farside.co.uk/ethereum-etf-flow-all-data/",
                   "SOL": "https://farside.co.uk/sol/"}


def _farside_page(asset, url, now, today_et):
    r = S.get(url, headers=FARSIDE_UA, timeout=60); r.raise_for_status()
    p = _Tables(); p.feed(r.text)
    best, tbl_best = [], None
    for tbl in p.tables:   # the flow table: the one with the most rows whose first cell is a date
        dated = [row for row in tbl if row and _farside_date(row[0])]
        if len(dated) > len(best): best, tbl_best = dated, tbl
    if len(best) < 5: raise RuntimeError(f"{asset}: flow table not found ({len(p.tables)} tables on page)")
    first = tbl_best.index(best[0])
    funds = _farside_funds(tbl_best[:first], max(len(r) for r in best))
    if "TOTAL" not in funds or sum(1 for x in funds if x and x != "TOTAL") < 2:
        raise RuntimeError(f"{asset}: fund header not read ({tbl_best[:first]})")
    rows = []
    for row in best:
        d = _farside_date(row[0])
        vals = [(fund, _farside_num(cell)) for fund, cell in zip(funds, row[1:]) if fund]
        if d >= today_et and not any(v for fd, v in vals if fd != "TOTAL"):
            continue   # today's placeholder row (all blank or "-") until Farside posts the day
        for fund, v in vals:
            if v is not None:
                rows.append(dict(asset=asset, date=d, fund=fund, flow_usd_m=v, source_url=url, ingested_at=now))
    return rows


def pull_farside(now):
    today_et = (dt.datetime.utcnow() - dt.timedelta(hours=4)).date().isoformat()
    return {"etf_flows": [r for asset, url in FARSIDE.items() for r in _farside_page(asset, url, now, today_et)]}


def pull_farside_history(now):
    """One-off: Farside's all-data pages (every day since each ETF launched). --only farside_history"""
    today_et = (dt.datetime.utcnow() - dt.timedelta(hours=4)).date().isoformat()
    rows = []
    for asset, url in FARSIDE_HISTORY.items():
        got = _farside_page(asset, url, now, today_et)
        print(f"  farside {asset}: {len(got)} rows, {min(r['date'] for r in got)} to {max(r['date'] for r in got)}", flush=True)
        rows += got
    return {"etf_flows": rows}


# ------------------------------------------------------------------ perpetual funding rates (added 2026-10-12)
# Kraken Futures publishes every perpetual's full funding history (hourly) with no key. Normal runs keep the last
# 3 days of every PF_ perpetual; --only funding_history loads everything since each contract listed.
# relative_funding_rate is the hourly rate as a fraction of notional; funding_rate is per contract in quote currency.
KRAKEN_FUT = "https://futures.kraken.com/derivatives/api"


def _kraken_perps():
    js = get(f"{KRAKEN_FUT}/v3/instruments")
    syms = [i["symbol"] for i in js.get("instruments", []) if str(i.get("symbol", "")).startswith("PF_") and i.get("tradeable", True)]
    return sorted(set(syms)) + ["PI_XBTUSD", "PI_ETHUSD"]   # the old inverse perps carry history back to 2018


def _perp_base(sym_):
    b = sym_.split("_", 1)[1]
    b = b[:-3] if b.endswith("USD") else b
    return "BTC" if b == "XBT" else b


def _funding(now, days=None):
    rows, cut = [], None
    if days: cut = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S")
    for inst in _kraken_perps():
        try: js = get(f"{KRAKEN_FUT}/v4/historicalfundingrates", params={"symbol": inst})
        except Exception: continue
        for x in js.get("rates", []):
            ts = str(x.get("timestamp", ""))[:19]
            if not ts or (cut and ts < cut): continue
            rows.append(dict(venue="kraken_futures", instrument=inst, symbol=_perp_base(inst), ts=ts.replace("T", " "),
                             funding_rate=f(x.get("fundingRate")), relative_funding_rate=f(x.get("relativeFundingRate")),
                             ingested_at=now))
    return rows


DERIBIT_PERPS = {"BTC-PERPETUAL": "BTC", "ETH-PERPETUAL": "ETH"}


def _deribit_funding(now, start):
    """Deribit BTC and ETH perpetual funding, one row per hour from `start` (date). Kraken's history endpoint only
    serves about a year; Deribit goes back to 2019. relative_funding_rate = interest_1h (hourly, fraction of notional),
    the same unit as Kraken's; funding_rate (per contract) does not apply and stays empty."""
    rows, step = [], 30 * 86400000
    end_ms = int(time.time() * 1000)
    for inst, base in DERIBIT_PERPS.items():
        s = int(dt.datetime.combine(start, dt.time(), dt.timezone.utc).timestamp() * 1000)
        while s < end_ms:
            js = get("https://www.deribit.com/api/v2/public/get_funding_rate_history",
                     params={"instrument_name": inst, "start_timestamp": s, "end_timestamp": min(s + step, end_ms)})
            for x in js.get("result", []):
                rows.append(dict(venue="deribit", instrument=inst, symbol=base, ts=ts_iso(x["timestamp"] / 1000),
                                 funding_rate=None, relative_funding_rate=f(x.get("interest_1h")), ingested_at=now))
            s += step; time.sleep(0.2)
    seen = {}
    for r in rows: seen[(r["instrument"], r["ts"])] = r
    return list(seen.values())


def pull_funding(now):
    return {"funding": _funding(now, days=3) + _deribit_funding(now, dt.date.today() - dt.timedelta(days=3))}


def pull_funding_history(now):
    """One-off: every Kraken perpetual's full funding history (about a year: all Kraken serves) plus Deribit BTC/ETH
    since 2019. --only funding_history"""
    return {"funding": _funding(now) + _deribit_funding(now, dt.date(2019, 1, 1))}


# ------------------------------------------------------------------ macro benchmarks (added 2026-09-28)
# One row per (source, series, obs_date): official daily closes from FRED, Treasury and Cboe; futures, DXY,
# gold and silver from Yahoo (LBMA fallback removed 2026-10-19: 403 from the cloud since Sep 30). rrp.macro_latest picks, per series, the newest observation (ties: the
# official source) and computes the change vs the prior observation from the same source.
FRED_SERIES = {"SP500": "SPX", "NASDAQ100": "NDX", "DJIA": "DJIA", "VIXCLS": "VIX", "DCOILWTICO": "WTI",
               "DCOILBRENTEU": "BRENT", "DTWEXBGS": "USD_BROAD", "DEXJPUS": "USDJPY", "DEXUSEU": "EURUSD",
               "DEXBZUS": "USDBRL", "DEXMXUS": "USDMXN", "DEXHKUS": "USDHKD", "DEXCAUS": "USDCAD",
               "WALCL": "FED_ASSETS", "WRESBAL": "BANK_RESERVES", "RRPONTSYD": "ON_RRP"}
# Fed liquidity (added 2026-10-23): WALCL = Fed total assets ($ millions, weekly, Wednesday level, out Thursday);
# WRESBAL = reserve balances with Federal Reserve Banks ($ millions, weekly); RRPONTSYD = overnight reverse repo
# take-up ($ billions, daily). History from 2003: `python main.py --only fred_liquidity_history`.
LIQUIDITY_SERIES = ("WALCL", "WRESBAL", "RRPONTSYD")
# Currency per US dollar (EURUSD is the one quoted the other way). Used to convert Form 4 buys priced in local
# currency (rrp.insider_filings buy_type non_usd). History from 2006: `python main.py --fx-backfill`.
FX_SERIES = ("DEXBZUS", "DEXMXUS", "DEXHKUS", "DEXCAUS", "DEXJPUS", "DEXUSEU")
YAHOO_SERIES = {"DX-Y.NYB": "DXY", "GC=F": "GOLD", "SI=F": "SILVER", "CL=F": "WTI", "BZ=F": "BRENT",
                "ES=F": "ES_FUT", "NQ=F": "NQ_FUT", "^GSPC": "SPX", "^NDX": "NDX", "^DJI": "DJIA", "^VIX": "VIX",
                "EURUSD=X": "EURUSD", "JPY=X": "USDJPY", "HG=F": "COPPER"}
TREASURY_COLS = {"3 Mo": "UST3M", "2 Yr": "UST2Y", "10 Yr": "UST10Y", "30 Yr": "UST30Y"}


def _macro_row(now, source, series, label, obs_date, close, open_=None, high=None, low=None, quote_time=None):
    return dict(source=source, series=series, label=label, obs_date=obs_date, close=close, open=open_, high=high,
                low=low, quote_time=quote_time, ingested_at=now)


def pull_fred(now, since=None, only=None):
    import csv as _csv, io as _io
    since = since or (dt.date.today() - dt.timedelta(days=45)).isoformat()
    rows = []
    for sid, label in FRED_SERIES.items():
        if only and sid not in only: continue
        r = S.get("https://fred.stlouisfed.org/graph/fredgraph.csv", params={"id": sid, "cosd": since}, timeout=30)
        r.raise_for_status()
        if not r.text.startswith("observation_date"): raise RuntimeError(f"{sid}: unexpected response")
        for rec in _csv.DictReader(_io.StringIO(r.text)):
            v = f(rec.get(sid))
            if v is not None: rows.append(_macro_row(now, "fred", sid, label, rec["observation_date"], v))
        time.sleep(0.3)
    return {"macro": rows}


def pull_fred_liquidity_history(now):
    """One-off: the Fed liquidity series since 2003."""
    return pull_fred(now, since="2003-01-01", only=LIQUIDITY_SERIES)


def pull_treasury(now, first_year=None):
    import csv as _csv, io as _io
    rows, today = [], dt.date.today()
    years = set(range(first_year, today.year + 1)) if first_year else {today.year} | ({today.year - 1} if today.month == 1 else set())
    for y in sorted(years):
        r = S.get(f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{y}/all",
                  params={"type": "daily_treasury_yield_curve", "field_tdr_date_value": y, "page": "", "_format": "csv"}, timeout=30)
        r.raise_for_status()
        for rec in _csv.DictReader(_io.StringIO(r.text)):
            d = dt.datetime.strptime(rec["Date"], "%m/%d/%Y").date()
            if not first_year and (today - d).days > 45: continue
            for col, label in TREASURY_COLS.items():
                v = f(rec.get(col))
                if v is not None: rows.append(_macro_row(now, "treasury", col, label, d.isoformat(), v))
    return {"macro": rows}


def pull_cboe_vix(now, full=False):
    import csv as _csv, io as _io
    r = S.get("https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv", timeout=30)
    r.raise_for_status()
    recs = list(_csv.DictReader(_io.StringIO(r.text)))
    recs = recs if full else recs[-45:]
    rows = []
    for rec in recs:
        d = dt.datetime.strptime(rec["DATE"], "%m/%d/%Y").date().isoformat()
        rows.append(_macro_row(now, "cboe", "VIX", "VIX", d, f(rec.get("CLOSE")), f(rec.get("OPEN")), f(rec.get("HIGH")), f(rec.get("LOW"))))
    return {"macro": rows}


def pull_yahoo(now, start=None):
    """Daily bars (1 month) per symbol from Yahoo's chart API: futures, DXY, metals, oil, and index
    cross-checks. The newest bar is the current session while a market is open (quote_time says when).
    start (YYYY-MM-DD) pulls full daily history from that date instead (used by --macro-backfill)."""
    rows = []
    for sym, label in YAHOO_SERIES.items():
        time.sleep(0.5)
        params = {"range": "1mo", "interval": "1d"}
        if start:
            p1 = int(dt.datetime.fromisoformat(start).replace(tzinfo=dt.timezone.utc).timestamp())
            params = {"period1": p1, "period2": int(time.time()), "interval": "1d"}
        r = S.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(sym)}",
                  params=params, headers={"User-Agent": "Mozilla/5.0 RRP Technologies research bot"}, timeout=60)
        if r.status_code != 200: raise RuntimeError(f"{sym}: HTTP {r.status_code}")
        res = (r.json().get("chart") or {}).get("result") or []
        if not res: raise RuntimeError(f"{sym}: empty result")
        res = res[0]; meta = res.get("meta", {}); off = meta.get("gmtoffset", 0)
        q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
        ts = res.get("timestamp") or []
        last_t = meta.get("regularMarketTime")
        col = lambda k, i: f((q.get(k) or [])[i]) if i < len(q.get(k) or []) else None
        for i, t in enumerate(ts):
            c = col("close", i)
            if c is None: continue
            d = (_EPOCH + dt.timedelta(seconds=t + off)).date().isoformat()
            rows.append(_macro_row(now, "yahoo", sym, label, d, c, col("open", i), col("high", i), col("low", i),
                                   ts_iso(last_t) if (i == len(ts) - 1 and last_t) else None))
    seen = {}
    for x in rows: seen[(x["series"], x["obs_date"])] = x
    return {"macro": list(seen.values())}



# ------------------------------------------------------------------ equity daily closes (added 2026-09-28)
# Daily bars for every ticker in rrp.equity_watchlist (core ETFs, crypto equities, defense primes, plus any
# ticker from clean insider buys or filing hits). Runs once a day on the first run after 22:00 UTC, Mon-Fri,
# fetching the last 5 sessions per ticker; a ticker with no history yet gets 1 year. First-time load of the
# whole list runs from the PC:  python main.py --equities-backfill
# Since build 0.2 (2026-10-01) the list is rrp.equity_coverage: every US-listed stock and ETF (Nasdaq Trader symbol
# directory, refreshed daily into markets.equity_universe) plus the watchlist (funds, filing and insider names). The
# daily pass runs as its own execution (--equities-only, scheduler rrp-market-equities, 1-hour timeout), not inside
# the 15-minute run. Full first load: python main.py --equities-backfill (5y per ticker; extends short histories).
EQUITY_HOUR_UTC = 22
EQUITY_THREADS = 12
EQUITY_MAX_NEW_PER_RUN = 400      # new tickers backfilled per daily run (keeps the job inside its timeout)
NASDAQ_SYMDIR = {"nasdaqlisted": "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
                 "otherlisted": "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"}
OTHER_EXCH = {"A": "NYSE American", "N": "NYSE", "P": "NYSE Arca", "Z": "Cboe BZX", "V": "IEX"}


def pull_universe(now):
    """Every US-listed security in the Nasdaq Trader symbol directory (Nasdaq, NYSE, NYSE American, NYSE Arca, Cboe,
    IEX), one row per symbol per day. Nothing is filtered here; rrp.equity_coverage decides what gets prices."""
    as_of = dt.datetime.now(dt.timezone.utc).date().isoformat()
    rows = []
    for name, url in NASDAQ_SYMDIR.items():
        r = S.get(url, timeout=60)
        r.raise_for_status()
        lines = [x for x in r.text.splitlines() if x.strip()]
        hdr = lines[0].split("|")
        if "Test Issue" not in hdr: raise RuntimeError(f"{name}: unexpected header {hdr[:4]}")
        for line in lines[1:]:
            if line.startswith("File Creation Time"): continue
            rec = dict(zip(hdr, line.split("|")))
            sym = rec.get("Symbol") or rec.get("ACT Symbol")
            if not sym: continue
            rows.append(dict(as_of=as_of, symbol=sym.strip(), name=(rec.get("Security Name") or "").strip()[:300],
                             exchange="Nasdaq" if name == "nasdaqlisted" else OTHER_EXCH.get(rec.get("Exchange"), rec.get("Exchange")),
                             market_category=rec.get("Market Category"), is_etf=rec.get("ETF") == "Y",
                             test_issue=rec.get("Test Issue") == "Y", financial_status=rec.get("Financial Status"),
                             source_file=name, ingested_at=now))
    if len(rows) < 5000: raise RuntimeError(f"only {len(rows)} symbols - directory looks truncated")
    return {"equity_universe": rows}


_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)   # epoch + timedelta: Windows' fromtimestamp rejects pre-1970 bars


def _yahoo_symbol(t):
    return t.replace(".", "-")


def _yahoo_daily(ticker, rng):
    """rng: a Yahoo range ("5d", "5y") or "since:YYYY-MM-DD" (explicit start). Never range=max: Yahoo then quietly
    returns monthly bars instead of daily (found 2026-10-20)."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(_yahoo_symbol(ticker))}"
    hdr = {"User-Agent": "Mozilla/5.0 RRP Technologies research bot"}
    params = {"interval": "1d", "includeAdjustedClose": "true"}
    if rng.startswith("since:"):
        params.update(period1=int((dt.datetime.fromisoformat(rng[6:]).replace(tzinfo=dt.timezone.utc) - _EPOCH).total_seconds()),
                      period2=int(time.time()))
    else:
        params["range"] = rng
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, headers=hdr, timeout=20)
        except requests.RequestException as e:
            err = type(e).__name__; time.sleep(2 * (attempt + 1)); continue
        if r.status_code == 429 or r.status_code >= 500:
            err = f"HTTP {r.status_code}"; time.sleep(5 * (attempt + 1)); continue
        if r.status_code != 200: return ticker, [], f"HTTP {r.status_code}"
        res = (r.json().get("chart") or {}).get("result") or []
        if not res: return ticker, [], "empty"
        res = res[0]; meta = res.get("meta", {}); off = meta.get("gmtoffset", 0)
        q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
        adj = (((res.get("indicators") or {}).get("adjclose") or [{}])[0]).get("adjclose") or []
        out = []
        for i, t in enumerate(res.get("timestamp") or []):
            col = lambda k: f((q.get(k) or [None] * (i + 1))[i]) if i < len(q.get(k) or []) else None
            c = col("close")
            if c is None: continue
            out.append(dict(symbol=ticker, date=(_EPOCH + dt.timedelta(seconds=t + off)).date().isoformat(),
                            open=col("open"), high=col("high"), low=col("low"), close=c,
                            adj_close=f(adj[i]) if i < len(adj) else None, volume=col("volume"),
                            currency=meta.get("currency"), exchange=meta.get("exchangeName"),
                            instrument_type=meta.get("instrumentType")))
        seen = {}
        for x in out: seen[x["date"]] = x
        return ticker, list(seen.values()), None
    return ticker, [], err


def _fetch_bars(jobs, now):
    from concurrent.futures import ThreadPoolExecutor
    rows, fails = [], []
    with ThreadPoolExecutor(EQUITY_THREADS) as ex:
        for t, bars, err in ex.map(lambda j: _yahoo_daily(*j), jobs):
            if err: fails.append(f"{t}:{err}")
            for b in bars: b["ingested_at"] = now
            rows += bars
    hard = [x for x in fails if not x.endswith((":HTTP 404", ":empty"))]   # 404/empty = delisted or unknown to Yahoo
    return rows, fails, hard


def _first_range(t):
    """History pulled the first time a ticker is loaded: 5 years for everything (completeness rule, build 0.2;
    mutual funds already got 5y from 2026-09-29)."""
    return "5y"


def _watchlist(bq):
    wl = [r.ticker for r in bq.query(f"SELECT ticker FROM `{PROJECT}.rrp.equity_coverage` ORDER BY priority, ticker").result()]
    have = {r.symbol for r in bq.query(f"SELECT DISTINCT symbol FROM `{PROJECT}.{TABLES["equity_daily"]}`").result()}
    return wl, have


def pull_equities(now):
    """Daily pass: last 5 sessions for tickers already loaded, first history (_first_range) for up to EQUITY_MAX_NEW_PER_RUN new ones."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    wl, have = _watchlist(bq)
    new = [t for t in wl if t not in have][:EQUITY_MAX_NEW_PER_RUN]
    jobs = [(t, _first_range(t)) for t in new] + [(t, "5d") for t in wl if t in have]
    rows, fails, hard = _fetch_bars(jobs, now)
    print(f"  equities: {len(jobs)} tickers requested ({len(new)} new), {len(rows)} bars, {len(fails)} failed", flush=True)
    if jobs and len(hard) > 0.25 * len(jobs):
        raise RuntimeError(f"{len(hard)}/{len(jobs)} tickers failed, e.g. " + ", ".join(hard[:8]))
    return {"equity_daily": rows}


def equities_backfill(batch=100):
    """First load for every rrp.equity_coverage ticker: 5 years each, 100 tickers per batch so memory stays small.
    Tickers with no rows are appended (no MERGE needed - nothing to collide with); tickers already loaded with less
    than ~4.5 years are re-fetched at 5y and MERGEd. Safe to re-run: each pass only does what is still missing."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"universe: {write(bq, 'equity_universe', pull_universe(now)['equity_universe'])} symbols refreshed", flush=True)
    wl, have = _watchlist(bq)
    short = {r.symbol for r in bq.query(f"""SELECT symbol FROM `{PROJECT}.{TABLES["equity_daily"]}` GROUP BY symbol
        HAVING MIN(date) > DATE_SUB(CURRENT_DATE(), INTERVAL 1640 DAY) AND COUNT(*) >= 200""").result()}
    new = [t for t in wl if t not in have]
    ext = [t for t in wl if t in short]
    print(f"equity backfill: {len(wl)} covered, {len(new)} new to load, {len(ext)} short histories to extend", flush=True)
    total, nfail = 0, 0
    for label, todo, append in (("new", new, True), ("extend", ext, False)):
        for i in range(0, len(todo), batch):
            chunk = todo[i:i + batch]
            rows, fails, hard = _fetch_bars([(t, "5y") for t in chunk], now)
            total += write(bq, "equity_daily", rows, append=append); nfail += len(fails)
            print(f"  {label} batch {i // batch + 1}/{-(-len(todo) // batch)}: {len(chunk)} tickers, {len(rows)} bars, {len(fails)} failed"
                  + (f" (non-404: {', '.join(hard[:5])})" if hard else ""), flush=True)
    print(f"equity backfill done: {total} bars written, {nfail} tickers failed")


DEEP_STAGE = "staging.equity_deep"   # unpartitioned holding table for the 2006+ history (kept 14 days)


def equities_deep(batch=50, since="2006-01-01"):
    """Extend every covered ticker back to 2006 (where the insider history starts), or its listing.
    Step 1: Yahoo daily bars from `since`, only bars older than the ticker's first stored bar, appended batch by batch
    to staging.equity_deep. That table is unpartitioned: hundreds of loads straight into markets.equity_daily (monthly
    partitions) hit BigQuery's daily limit on partition modifications (2026-10-20). Re-running skips tickers already
    staged or already reaching back to `since`.
    Step 2 (equities_deep_commit): one INSERT moves the staged bars in. adj_close of the older bars is adjusted as of
    today: a split since the first load shows as a step at the join, so use close-to-close for returns across it."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    bq = bigquery.Client(project=PROJECT)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ref = f"{PROJECT}.{DEEP_STAGE}"
    try:
        staged = {r.symbol for r in bq.query(f"SELECT DISTINCT symbol FROM `{ref}`").result()}
    except NotFound:
        t = bigquery.Table(ref, schema=bq_schema("equity_daily"))
        t.expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=14)
        bq.create_table(t); staged = set()
    first = {r.symbol: r.d.isoformat() for r in bq.query(f"""SELECT symbol, MIN(date) AS d FROM `{PROJECT}.{TABLES["equity_daily"]}`
        GROUP BY symbol HAVING MIN(date) > DATE_ADD(DATE '{since}', INTERVAL 7 DAY)""").result()}
    wl = set(_watchlist(bq)[0])
    todo = sorted(t for t in first if t in wl and t not in staged)
    print(f"equity deep history: {len(todo)} tickers to fetch ({len(staged)} already staged)", flush=True)
    cfg = bigquery.LoadJobConfig(schema=bq_schema("equity_daily"), source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                 write_disposition="WRITE_APPEND")
    cols = [c for c, _ in SCHEMAS["equity_daily"]]
    total = 0
    for i in range(0, len(todo), batch):
        chunk = todo[i:i + batch]
        rows, fails, hard = _fetch_bars([(t, f"since:{since}") for t in chunk], now)
        rows = [r for r in rows if since <= r["date"] < first[r["symbol"]]]
        per = {}
        for r in rows: per.setdefault(r["symbol"], []).append(r["date"])
        coarse = {t for t, ds in per.items() if len(ds) > 20 and
                  (dt.date.fromisoformat(max(ds)) - dt.date.fromisoformat(min(ds))).days / (len(ds) - 1) > 4}
        if coarse: print(f"  skipped (not daily bars): {', '.join(sorted(coarse)[:10])}", flush=True)
        rows = [{c: r.get(c) for c in cols} for r in rows if r["symbol"] not in coarse]
        if rows:
            bq.load_table_from_json(rows, ref, job_config=cfg).result(); total += len(rows)
        print(f"  batch {i // batch + 1}/{-(-len(todo) // batch)}: {len(chunk)} tickers, {len(rows)} older bars, {len(fails)} failed", flush=True)
    print(f"equity deep history: {total} bars staged", flush=True)
    equities_deep_commit(bq)


def equities_deep_commit(bq=None):
    """Move staged 2006+ bars into markets.equity_daily in one job: only bars older than the ticker's first stored bar,
    one per (symbol, date), so re-running inserts nothing twice. python main.py --equities-deep-commit"""
    from google.cloud import bigquery
    from google.api_core.exceptions import Forbidden
    bq = bq or bigquery.Client(project=PROJECT)
    cols = ", ".join(c for c, _ in SCHEMAS["equity_daily"])
    q = f"""INSERT INTO `{PROJECT}.{TABLES["equity_daily"]}` ({cols})
    SELECT {cols} FROM (
      SELECT s.* FROM `{PROJECT}.{DEEP_STAGE}` s
      JOIN (SELECT symbol, MIN(date) AS first_date FROM `{PROJECT}.{TABLES["equity_daily"]}` GROUP BY symbol) f USING (symbol)
      WHERE s.date < f.first_date
      QUALIFY ROW_NUMBER() OVER (PARTITION BY s.symbol, s.date ORDER BY s.ingested_at DESC) = 1)"""
    try:
        j = bq.query(q); j.result()
        print(f"equity deep history: {j.num_dml_affected_rows} bars inserted into {TABLES['equity_daily']}")
    except Forbidden as e:
        print(f"insert blocked ({str(e)[:160]}). The bars stay in {DEEP_STAGE} for 14 days: "
              f"run  py main.py --equities-deep-commit  after the quota window resets (24 hours).")


def macro_backfill(start="2000-01-01"):
    """One-time (re-runnable) load of full daily history into markets.macro: Yahoo futures/FX/indexes from `start`,
    FRED full series, Treasury par yields since 1990, and Cboe VIX full files. MERGE on
    (source, series, obs_date), so re-running only refreshes rows. Each source is independent."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    jobs = [("yahoo", lambda: pull_yahoo(now, start=start)), ("fred", lambda: pull_fred(now, since="1990-01-01")),
            ("treasury", lambda: pull_treasury(now, first_year=1990)), ("cboe", lambda: pull_cboe_vix(now, full=True))]
    total = 0
    for name, fn in jobs:
        try:
            rows = fn()["macro"]
            n = write(bq, "macro", rows); total += n
            by = {}
            for r in rows: by.setdefault(r["label"], [r["obs_date"], r["obs_date"], 0]); b = by[r["label"]]; b[0] = min(b[0], r["obs_date"]); b[1] = max(b[1], r["obs_date"]); b[2] += 1
            print(f"  {name}: {n} rows written; " + "; ".join(f"{k} {v[2]} ({v[0]}..{v[1]})" for k, v in sorted(by.items())), flush=True)
        except Exception as e:
            print(f"  {name}: FAILED {type(e).__name__}: {e}", flush=True)
    print(f"macro backfill done: {total} rows")


# ------------------------------------------------------------------ crypto universe (build 0.3, 2026-10-01)
# a) Whole market, hourly: CoinPaprika /v1/tickers (one call, top 2,000 coins: rank, price, cap, volume, % changes,
#    supply, ATH) -> markets.crypto_market. Append-only snapshots, filters live in views. (Per-venue tickers for every
#    Kraken and Coinbase pair, markets.crypto_venue_ticker, were retired 2026-10-26: nothing read them.)
# b) Daily OHLCV for every Kraken and Coinbase USD pair -> markets.crypto_daily (MERGE on venue/pair/date).
#    Coinbase back to each product's listing (BTC-USD from Jul 2015); Kraken serves the last 720 days.
#    --crypto-backfill loads all history once; --crypto-daily (own scheduler, 00:15 UTC) refreshes the last 5 days.
KRAKEN_BASE_FIX = {"XBT": "BTC", "XDG": "DOGE"}


def pull_crypto_market(now):
    r = S.get("https://api.coinpaprika.com/v1/tickers", params={"quotes": "USD"}, timeout=60)
    r.raise_for_status()
    mk = []
    for c in r.json():
        q = (c.get("quotes") or {}).get("USD") or {}
        mk.append(dict(ts=now, id=c.get("id"), symbol=c.get("symbol"), name=c.get("name"), rank=c.get("rank"),
                       price=f(q.get("price")), volume_24h=f(q.get("volume_24h")), market_cap=f(q.get("market_cap")),
                       pct_1h=f(q.get("percent_change_1h")), pct_24h=f(q.get("percent_change_24h")), pct_7d=f(q.get("percent_change_7d")),
                       pct_30d=f(q.get("percent_change_30d")), pct_1y=f(q.get("percent_change_1y")), ath_price=f(q.get("ath_price")),
                       ath_date=(q.get("ath_date") or "")[:10] or None, pct_from_ath=f(q.get("percent_from_price_ath")),
                       circulating_supply=f(c.get("circulating_supply")), total_supply=f(c.get("total_supply")),
                       max_supply=f(c.get("max_supply")), first_data_at=(c.get("first_data_at") or "")[:10] or None,
                       last_updated=ts_iso_str(c.get("last_updated"))))
    if len(mk) < 500: raise RuntimeError(f"coinpaprika returned {len(mk)} coins")
    return {"crypto_market": mk}


def ts_iso_str(x):
    return x.replace("T", " ").replace("Z", "") if x else None


def _kraken_usd_pairs():
    ap = S.get("https://api.kraken.com/0/public/AssetPairs", timeout=60).json()["result"]
    out = []
    for key, v in ap.items():
        if v.get("quote") not in ("ZUSD", "USD") or v.get("status", "online") != "online": continue
        base = (v.get("wsname") or "/").split("/")[0]
        out.append((key, KRAKEN_BASE_FIX.get(base, base)))
    return out


def _kraken_daily(key, base, now, since=None):
    for attempt in range(4):
        params = {"pair": key, "interval": 1440}
        if since: params["since"] = since
        j = S.get("https://api.kraken.com/0/public/OHLC", params=params, timeout=30).json()
        if j.get("error"):
            if any("Too many" in e for e in j["error"]): time.sleep(5 * (attempt + 1)); continue
            return [], ",".join(j["error"])
        bars = next((v for k, v in j["result"].items() if k != "last"), [])
        today = dt.datetime.now(dt.timezone.utc).date()
        rows = []
        for b in bars:
            d = dt.datetime.fromtimestamp(int(b[0]), dt.timezone.utc).date()
            rows.append(dict(venue="kraken", pair=key, base=base, quote="USD", date=d.isoformat(), open=f(b[1]), high=f(b[2]),
                             low=f(b[3]), close=f(b[4]), vwap=f(b[5]), volume=f(b[6]), trades=int(b[7]),
                             is_open=d >= today, ingested_at=now))
        return rows, None
    return [], "rate limited"


def _coinbase_daily(pid, base, now, start=None):
    """Daily candles for one Coinbase product, paging back 300 days per call until the venue returns nothing."""
    end = dt.datetime.now(dt.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)
    stop = dt.datetime.fromisoformat(start).replace(tzinfo=dt.timezone.utc) if start else None
    today, rows = end.date() - dt.timedelta(days=1), []
    while True:
        beg = end - dt.timedelta(days=300)
        if stop and beg < stop: beg = stop
        for attempt in range(4):
            r = S.get(f"https://api.exchange.coinbase.com/products/{pid}/candles",
                      params={"granularity": 86400, "start": beg.isoformat(), "end": end.isoformat()}, timeout=30)
            if r.status_code == 429: time.sleep(2 * (attempt + 1)); continue
            break
        if r.status_code != 200: return rows, f"HTTP {r.status_code}"
        bars = r.json()
        for b in bars:
            d = dt.datetime.fromtimestamp(int(b[0]), dt.timezone.utc).date()
            rows.append(dict(venue="coinbase", pair=pid, base=base, quote="USD", date=d.isoformat(), open=f(b[3]), high=f(b[2]),
                             low=f(b[1]), close=f(b[4]), vwap=None, volume=f(b[5]), trades=None, is_open=d >= today, ingested_at=now))
        time.sleep(0.15)
        if not bars or (stop and beg <= stop): break
        end = beg
    seen = {}
    for x in rows: seen[x["date"]] = x
    return list(seen.values()), None


def crypto_daily(backfill=False):
    """Daily OHLCV for every Kraken and Coinbase USD pair. backfill=True pulls all history each venue serves; otherwise
    the last 5 days. Written every 50 pairs so memory stays small; MERGE makes re-runs safe."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    recent = (dt.date.today() - dt.timedelta(days=5)).isoformat()
    since_ts = int(time.time()) - 6 * 86400
    cb = [(p["id"], p["base_currency"]) for p in S.get("https://api.exchange.coinbase.com/products", timeout=60).json()
          if p.get("quote_currency") == "USD" and p.get("status") == "online"]
    kr = _kraken_usd_pairs()
    print(f"crypto daily ({'backfill' if backfill else 'recent'}): {len(cb)} Coinbase + {len(kr)} Kraken USD pairs", flush=True)
    total, fails, buf = 0, [], []
    jobs = [("coinbase", p, b) for p, b in cb] + [("kraken", p, b) for p, b in kr]
    for i, (venue, pair, base) in enumerate(jobs, 1):
        try:
            if venue == "coinbase": rows, err = _coinbase_daily(pair, base, now, None if backfill else recent)
            else:
                rows, err = _kraken_daily(pair, base, now, None if backfill else since_ts); time.sleep(1.1)
        except Exception as e:
            rows, err = [], f"{type(e).__name__}: {e}"[:200]
        if err: fails.append(f"{venue}:{pair}:{err}")
        # drop bad bars: Kraken has sent an epoch (1970-01-01) bar with every price 0 for a new pair
        rows = [r for r in rows if r["date"] >= "2009-01-03" and all((r[k] or 0) > 0 for k in ("open", "high", "low", "close"))]
        buf += rows
        if i % 50 == 0 or i == len(jobs):
            total += write(bq, "crypto_daily", buf); buf = []
            print(f"  {i}/{len(jobs)} pairs, {total} bars written, {len(fails)} failed", flush=True)
    print(f"crypto daily done: {total} bars, {len(fails)} failed" + (f" e.g. {fails[:5]}" if fails else ""))
    if not backfill and len(fails) > 0.25 * len(jobs): raise RuntimeError(f"{len(fails)}/{len(jobs)} pairs failed")


# c) Hourly OHLCV for every Coinbase USD product -> markets.crypto_hourly (MERGE on venue/pair/ts), added 2026-10-27.
#    Hourly bars for the whole Coinbase USD universe, not only the 12 core coins. The second run of each
#    hour refreshes the last 6 hours (so a missed run heals itself); --only crypto_hourly_history loads 45 days.
def _coinbase_products():
    return [(p["id"], p["base_currency"]) for p in S.get("https://api.exchange.coinbase.com/products", timeout=60).json()
            if p.get("quote_currency") == "USD" and p.get("status") == "online"]


def _coinbase_hourly(pid, base, now, hours, end_at=None, start_at=None):
    """Hourly candles for one Coinbase product over the last `hours` hours (300 bars per call); with end_at and
    start_at, the bars from start_at up to (not including) end_at instead."""
    top = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) + dt.timedelta(hours=1)
    end = end_at or top
    stop, rows, cur_hour = start_at or end - dt.timedelta(hours=hours), {}, top - dt.timedelta(hours=1)
    while end > stop:
        beg = max(stop, end - dt.timedelta(hours=300))
        for attempt in range(4):
            r = S.get(f"https://api.exchange.coinbase.com/products/{pid}/candles",
                      params={"granularity": 3600, "start": beg.isoformat(), "end": end.isoformat()}, timeout=30)
            if r.status_code == 429: time.sleep(2 * (attempt + 1)); continue
            break
        if r.status_code != 200: return list(rows.values()), f"HTTP {r.status_code}"
        bars = r.json()
        if not bars and rows and start_at: break   # deep backfill: walked back past the product's first bar
        for b in bars:
            t = dt.datetime.fromtimestamp(int(b[0]), dt.timezone.utc)
            if end_at and t >= end_at: continue
            if not all((x or 0) > 0 for x in (b[1], b[2], b[3], b[4])): continue
            rows[t] = dict(venue="coinbase", pair=pid, base=base, ts=t.strftime("%Y-%m-%d %H:%M:%S"), open=f(b[3]),
                           high=f(b[2]), low=f(b[1]), close=f(b[4]), volume=f(b[5]), is_open=t >= cur_hour, ingested_at=now)
        time.sleep(0.12)
        end = beg
    return list(rows.values()), None


def _crypto_hourly(now, hours, bq=None, end_at=None, append=False, start_at=None):
    """Every Coinbase USD product's hourly bars for the last `hours` hours. With bq, writes every 50 products and
    returns {}; without (a normal run), returns the rows for run() to write."""
    prods, rows, fails, total = _coinbase_products(), [], [], 0
    for i, (pid, base) in enumerate(prods, 1):
        try:
            ea = end_at.get(pid, end_at.get(None)) if isinstance(end_at, dict) else end_at
            got, err = _coinbase_hourly(pid, base, now, hours, ea, start_at) if not (start_at and ea and ea <= start_at) else ([], None)
        except Exception as e:
            got, err = [], f"{type(e).__name__}: {e}"[:200]
        if err: fails.append(f"{pid}:{err}")
        rows += got
        if bq is not None and (i % 50 == 0 or i == len(prods)):
            total += write(bq, "crypto_hourly", rows, append=append); rows = []
            print(f"  {i}/{len(prods)} products, {total} bars written, {len(fails)} failed", flush=True)
    if len(fails) > 0.25 * max(len(prods), 1): raise RuntimeError(f"{len(fails)}/{len(prods)} products failed, e.g. {fails[:3]}")
    if "--dry" in sys.argv: print(f"  crypto_hourly: {len(prods)} products, {len(rows)} bars, {len(fails)} failed", flush=True)
    return {} if bq is not None else {"crypto_hourly": rows}


def pull_crypto_hourly(now):
    return _crypto_hourly(now, 6)


def pull_crypto_hourly_history(now):
    """One-off: hourly bars for every Coinbase USD product over the last --days N days (default 45).
    --only crypto_hourly_history --days 500 reaches back to mid-2025 for the model rebuild (b237); about 40 calls per
    product, a couple of hours, written every 50 products (MERGE, so a re-run or an overlap is harmless). Coinbase lists
    only products still online, so coins delisted before today are missing (survivorship)."""
    from google.cloud import bigquery
    days = int(sys.argv[sys.argv.index("--days") + 1]) if "--days" in sys.argv else 45
    bq = bigquery.Client(project=PROJECT)
    if days <= 45:
        print(f"  crypto_hourly_history: last {days} days", flush=True)
        return _crypto_hourly(now, days * 24, bq)
    # deeper history: for each product fetch only the hours before ITS first bar in the table and APPEND them (no MERGE:
    # a MERGE per 50k rows over a year of partitions would bill tens of GB). A product not in the table yet stops at the
    # table's first bar. A re-run (e.g. after a crash) picks up each product from its own first bar, so nothing doubles.
    firsts = {r.pair: r.t for r in bq.query(f"SELECT pair, MIN(ts) AS t FROM `{PROJECT}.{TABLES['crypto_hourly']}` "
                                           "WHERE venue = 'coinbase' GROUP BY pair").result()}
    firsts[None] = min(firsts.values())
    start = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0) - dt.timedelta(days=days)
    print(f"  crypto_hourly_history: back to {start:%Y-%m-%d %H:%M} UTC, each product up to its first stored bar "
          f"(table starts {firsts[None]:%Y-%m-%d %H:%M}), append", flush=True)
    return _crypto_hourly(now, 0, bq, end_at=firsts, append=True, start_at=start)


# d) Order-book snapshot for the top 150 Coinbase USD coins by 24h dollar volume -> markets.crypto_orderbook (append),
#    added 2026-10-27 (b242). Takes over from an earlier order-book feed (copied to markets.crypto_orderbook_history):
#    same source (Coinbase Advanced Trade public product book, 50 levels) and the same four measures over the top 20
#    levels - ob_imbalance = (bid size - ask size) / (bid size + ask size), depth_bid / depth_ask in coin units, spread in
#    dollars - plus dollar depth, spread in basis points and the mid price, so coins can be compared with each other.
#    First run of each hour (snapshot a few minutes after the hour), ~150 calls.
OB_TOP_N, OB_LEVELS = 150, 20
CB_MARKET = "https://api.coinbase.com/api/v3/brokerage/market"


def _ob_universe():
    """Top OB_TOP_N online Coinbase USD spot products by 24h dollar volume (public Advanced Trade product list)."""
    d = get(f"{CB_MARKET}/products", params={"product_type": "SPOT", "limit": 2000})
    prods = []
    for p in d.get("products", []):
        if p.get("quote_currency_id") != "USD" or p.get("trading_disabled") or p.get("is_disabled") or p.get("status") not in (None, "online"):
            continue
        if (p.get("base_currency_id") or "") in ("USDT", "USDC", "DAI", "PYUSD", "EURC", "GUSD", "PAX", "USDS"):
            continue   # stablecoins: no price discovery worth tracking
        usd = (f(p.get("volume_24h")) or 0) * (f(p.get("price")) or 0)
        prods.append((usd, p["product_id"], p.get("base_currency_id") or p["product_id"].split("-")[0]))
    prods.sort(reverse=True)
    return prods[:OB_TOP_N]


def _ob_row(pid, base, rank, vol_usd, now):
    for attempt in range(4):
        r = S.get(f"{CB_MARKET}/product_book", params={"product_id": pid, "limit": 50}, timeout=20)
        if r.status_code == 429: time.sleep(2 * (attempt + 1)); continue
        break
    if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code}")
    pb = r.json().get("pricebook") or {}
    bids = [(f(x["price"]), f(x["size"])) for x in pb.get("bids", [])[:OB_LEVELS]]
    asks = [(f(x["price"]), f(x["size"])) for x in pb.get("asks", [])[:OB_LEVELS]]
    if not bids or not asks: raise RuntimeError("empty book")
    bb, ba = bids[0][0], asks[0][0]
    mid = (bb + ba) / 2
    db, da = sum(q for _, q in bids), sum(q for _, q in asks)
    m = re.match(r"(\d{4}-\d\d-\d\d)T(\d\d:\d\d:\d\d)", pb.get("time") or "")   # the book's own time, to the second
    return dict(ts=f"{m.group(1)} {m.group(2)}" if m else now, venue="coinbase", pair=pid, base=base, vol_rank=rank,
                volume_24h_usd=vol_usd, best_bid=bb, best_ask=ba, mid=mid, spread=ba - bb,
                spread_bps=(ba - bb) / mid * 1e4 if mid else None, depth_bid=db, depth_ask=da,
                depth_bid_usd=sum(p * q for p, q in bids), depth_ask_usd=sum(p * q for p, q in asks),
                ob_imbalance=(db - da) / (db + da) if db + da else None, levels=min(len(bids), len(asks)), ingested_at=now)


def pull_crypto_orderbook(now):
    uni, rows, fails = _ob_universe(), [], []
    for rank, (vol, pid, base) in enumerate(uni, 1):
        try:
            rows.append(_ob_row(pid, base, rank, vol, now))
        except Exception as e:
            fails.append(f"{pid}:{type(e).__name__}: {e}"[:120])
        time.sleep(0.1)
    if not uni or len(fails) > 0.25 * len(uni): raise RuntimeError(f"{len(fails)}/{len(uni)} books failed, e.g. {fails[:3]}")
    if "--dry" in sys.argv:
        print(f"  crypto_orderbook: {len(uni)} products, {len(rows)} books, {len(fails)} failed", flush=True)
        for r in rows[:3]: print(f"    {r['pair']} rank {r['vol_rank']} mid {r['mid']} spread {r['spread_bps']:.2f} bps "
                                 f"depth ${r['depth_bid_usd']:,.0f} / ${r['depth_ask_usd']:,.0f} imbalance {r['ob_imbalance']:.3f}", flush=True)
    return {"crypto_orderbook": rows}


# ------------------------------------------------------------------ FINRA short volume and short interest (2026-10-23)
# Free files on FINRA's CDN, no key. Daily short volume (Reg SHO, all US trade reporting facilities consolidated):
# https://cdn.finra.org/equity/regsho/daily/CNMSshvolYYYYMMDD.txt, pipe-delimited, posted the same evening.
# Short interest (twice a month, per settlement date, published about a week later):
# https://cdn.finra.org/equity/otcmarket/biweekly/shrtYYYYMMDD.csv - exchange-listed stocks are included from June 2021,
# OTC only before. Columns are read by name, so a reordered file still loads.
FINRA_SV = "https://cdn.finra.org/equity/regsho/daily/CNMSshvol{d:%Y%m%d}.txt"
FINRA_SI = "https://cdn.finra.org/equity/otcmarket/biweekly/shrt{d:%Y%m%d}.csv"


def _text(url):
    """File text, or None when FINRA has no file for that date (404/403)."""
    for i in range(3):
        try:
            r = S.get(url, timeout=60)
            if r.status_code in (403, 404): return None
            if r.status_code in (429, 502, 503, 504): time.sleep(3 * (i + 1)); continue
            r.raise_for_status(); return r.text
        except requests.RequestException:
            if i == 2: raise
            time.sleep(2 * (i + 1))
    return None


def _table(txt):
    """Header-keyed rows (lower-case keys) from a pipe- or comma-delimited file; FINRA trailer lines are dropped."""
    import csv
    lines = [l for l in txt.splitlines() if l.strip()]
    if not lines: return []
    delim = "|" if lines[0].count("|") >= lines[0].count(",") else ","
    # pipe files have no quoting, but an issue name with a stray " made csv swallow thousands of lines into one
    # field (Mar 2018 short interest); QUOTE_NONE reads every line as its own row
    csv.field_size_limit(10_000_000)
    rd = csv.reader(lines, delimiter=delim, quoting=csv.QUOTE_NONE if delim == "|" else csv.QUOTE_MINIMAL)
    # FINRA started quoting every field in the short interest files from the Sep 15 2026 settlement ("A"|"AA"|...): with
    # QUOTE_NONE the quotes stay in the text, so strip one pair from each header and cell (empty "" becomes '')
    unq = lambda c: c.strip()[1:-1] if len(c.strip()) >= 2 and c.strip()[0] == c.strip()[-1] == '"' else c.strip()
    head = [unq(h).lower() for h in next(rd)]
    return [dict(zip(head, (unq(c) for c in r))) for r in rd if len(r) == len(head)]


def _iso(v, fallback=None):
    v = (v or "").strip()
    if re.fullmatch(r"\d{8}", v): return f"{v[:4]}-{v[4:6]}-{v[6:]}"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v[:10]): return v[:10]
    return fallback


def _short_volume_day(day, now):
    txt = _text(FINRA_SV.format(d=day))
    if not txt: return []
    out = []
    for r in _table(txt):
        sym_ = r.get("symbol")
        if not sym_: continue
        out.append(dict(date=_iso(r.get("date"), day.isoformat()), symbol=sym_, short_volume=f(r.get("shortvolume")),
                        short_exempt_volume=f(r.get("shortexemptvolume")), total_volume=f(r.get("totalvolume")),
                        market=r.get("market"), ingested_at=now))
    return out


def pull_short_volume(now):
    rows, today = [], dt.date.today()
    for k in range(8):
        day = today - dt.timedelta(days=k)
        if day.weekday() < 5: rows += _short_volume_day(day, now)
    return {"short_volume": rows}


def _short_interest_file(day, now):
    txt = _text(FINRA_SI.format(d=day))
    if not txt: return []
    out = []
    for r in _table(txt):
        sym_ = r.get("symbolcode") or r.get("symbol")
        if not sym_: continue
        out.append(dict(settlement_date=_iso(r.get("settlementdate"), day.isoformat()), symbol=sym_,
                        name=r.get("issuename"), market=r.get("issuerservicesgroupexchangecode") or r.get("marketclasscode"),
                        short_qty=f(r.get("currentshortpositionquantity")), prev_short_qty=f(r.get("previousshortpositionquantity")),
                        avg_daily_vol=f(r.get("averagedailyvolumequantity")), days_to_cover=f(r.get("daystocoverquantity")),
                        change_pct=f(r.get("changepercent")), split_flag=r.get("stocksplitflag"),
                        revision=r.get("revisionflag"), source_file=FINRA_SI.format(d=day).rsplit("/", 1)[1], ingested_at=now))
    return out


def pull_short_interest(now):
    """Every short interest file for a settlement date in the last 45 days (a file is replaced when FINRA revises it)."""
    rows, today = [], dt.date.today()
    for k in range(45):
        day = today - dt.timedelta(days=k)
        if day.weekday() < 5: rows += _short_interest_file(day, now)
    return {"short_interest": rows}


def pull_short_volume_history(now, start=dt.date(2018, 8, 1)):
    """One-off: every daily short volume file since start, appended month by month (months already loaded are skipped)."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    have = {r.m for r in bq.query(f"SELECT DISTINCT FORMAT_DATE('%Y-%m', date) AS m FROM `{PROJECT}.{TABLES['short_volume']}`").result()}
    total, day, end = 0, start, dt.date.today() - dt.timedelta(days=8)
    while day <= end:
        m_end = (day.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
        if day.strftime("%Y-%m") not in have:
            rows, d = [], day
            while d <= min(m_end, end):
                if d.weekday() < 5:
                    try: rows += _short_volume_day(d, now)
                    except Exception as e: print(f"  short volume {d}: FAILED {type(e).__name__}: {e}"[:300], flush=True)
                d += dt.timedelta(days=1)
            total += write(bq, "short_volume", rows, append=True)
            print(f"  short volume {day:%Y-%m}: {len(rows)} rows", flush=True)
        day = m_end + dt.timedelta(days=1)
    return {}


def pull_short_interest_history(now, start=dt.date(2014, 1, 1)):
    """One-off: every short interest file since start (tries each weekday; FINRA names files by settlement date)."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    have = {r.f for r in bq.query(f"SELECT DISTINCT source_file AS f FROM `{PROJECT}.{TABLES['short_interest']}`").result()}
    day, end, files = start, dt.date.today() - dt.timedelta(days=45), 0
    while day <= end:
        if day.weekday() < 5 and FINRA_SI.format(d=day).rsplit("/", 1)[1] not in have:
            try:
                rows = _short_interest_file(day, now)
                if rows:
                    write(bq, "short_interest", rows); files += 1
                    print(f"  short interest {day}: {len(rows)} rows", flush=True)
            except Exception as e:   # one bad file is logged and skipped; a rerun retries it
                print(f"  short interest {day}: FAILED {type(e).__name__}: {e}"[:300], flush=True)
        day += dt.timedelta(days=1)
    print(f"short interest history: {files} files loaded")
    return {}


# ------------------------------------------------------------------ U.S. Treasury Daily Treasury Statement (added 2026-10-23, b165)
# Fiscal Data API (no key): https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/<table>
# Published each business day around 4 PM ET for the prior business day; history from Oct 2005. Amounts in $ millions.
# One long table: one row per DTS line (record_date, table_id, line = the row's position that day, in DTS line order). Field names are read loosely so a renamed column
# lands NULL rather than failing the load; every source field is also kept in raw (JSON) for anything not mapped.
DTS_BASE = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/"
DTS_TABLES = {"cash": "operating_cash_balance", "flows": "deposits_withdrawals_operating_cash",
              "debt": "public_debt_transactions", "debt_limit": "debt_subject_to_limit",
              "tax": "federal_tax_deposits"}


def _dts_num(v):
    v = (v or "").strip()
    if v in ("", "null", "*", "-"): return None
    try: return float(v.replace(",", ""))
    except ValueError: return None


def _dts_pick(r, *names):
    for n in names:
        v = r.get(n)
        if v not in (None, "", "null"): return v
    return None


def _dts_pages(table, since, until=None):
    flt = f"record_date:gte:{since}" + (f",record_date:lte:{until}" if until else "")
    page, out = 1, []
    while True:
        js = get(DTS_BASE + table, params={"filter": flt, "sort": "record_date,src_line_nbr", "page[size]": 5000, "page[number]": page})
        out += js.get("data", [])
        if page >= int((js.get("meta") or {}).get("total-pages") or 1): return out
        page += 1


def _dts_rows(table_id, recs, now):
    out, seq = [], {}
    for r in recs:
        d = r.get("record_date")
        k = (d, table_id); seq[k] = seq.get(k, 0) + 1
        out.append(dict(   # line = position within the day (sorted by DTS line number): unique even where old days repeat one
            record_date=d, table_id=table_id, line=seq[k],
            account_type=_dts_pick(r, "account_type"), transaction_type=_dts_pick(r, "transaction_type"),
            category=_dts_pick(r, "transaction_catg", "debt_catg", "security_type", "tax_deposit_type", "tax_catg"),
            category_desc=_dts_pick(r, "transaction_catg_desc", "debt_catg_desc", "security_type_desc", "tax_deposit_type_desc"),
            security_market=_dts_pick(r, "security_market"),
            today_amt=_dts_num(_dts_pick(r, "transaction_today_amt", "tax_deposit_today_amt", "today_amt")),
            mtd_amt=_dts_num(_dts_pick(r, "transaction_mtd_amt", "tax_deposit_mtd_amt", "mtd_amt")),
            fytd_amt=_dts_num(_dts_pick(r, "transaction_fytd_amt", "tax_deposit_fytd_amt", "fytd_amt")),
            open_today_bal=_dts_num(_dts_pick(r, "open_today_bal")), close_today_bal=_dts_num(_dts_pick(r, "close_today_bal")),
            open_month_bal=_dts_num(_dts_pick(r, "open_month_bal")), open_fy_bal=_dts_num(_dts_pick(r, "open_fiscal_year_bal")),
            raw=json.dumps(r, separators=(",", ":")), ingested_at=now))
    return out


def pull_treasury_dts(now):
    """The last 14 days of every DTS table (a revised day is replaced)."""
    since = (dt.date.today() - dt.timedelta(days=14)).isoformat()
    rows = []
    for tid, table in DTS_TABLES.items():
        recs = _dts_pages(table, since)
        if "--dry" in sys.argv and recs: print(f"  dts {table}: {len(recs)} rows; fields: {sorted(recs[0])}", flush=True)
        rows += _dts_rows(tid, recs, now)
    return {"treasury_dts": rows}


def pull_treasury_dts_history(now, start=2005):
    """One-off: every DTS table from Oct 2005, a calendar year at a time (years already loaded for a table are skipped)."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    have = {(r.t, r.y) for r in bq.query(f"SELECT DISTINCT table_id AS t, EXTRACT(YEAR FROM record_date) AS y "
                                         f"FROM `{PROJECT}.{TABLES['treasury_dts']}`").result()}
    this_year = dt.date.today().year
    for tid, table in DTS_TABLES.items():
        for y in range(start, this_year + 1):
            if (tid, y) in have and y < this_year: continue
            try:
                rows = _dts_rows(tid, _dts_pages(table, f"{y}-01-01", f"{y}-12-31"), now)
                write(bq, "treasury_dts", rows)
                print(f"  dts {table} {y}: {len(rows)} rows", flush=True)
            except Exception as e:   # logged and skipped; a rerun retries it
                print(f"  dts {table} {y}: FAILED {type(e).__name__}: {e}"[:300], flush=True)
    return {}


# ------------------------------------------------------------------ EIA grid monitor: US electricity demand and generation (added 2026-10-23, b164)
# EIA API v2 (free key in Secret Manager as eia-api-key, mounted as EIA_API_KEY; on the PC set $env:EIA_API_KEY).
# demand = electricity/rto/region-data (D demand, DF day-ahead forecast, NG net generation, TI net interchange);
# fuel = electricity/rto/fuel-type-data (net generation by fuel: coal, gas, nuclear, wind, solar, hydro, ...). MWh.
# Daily totals for every balancing authority and region from July 2015 (Eastern-time days); hourly only for the 14
# regions (US48 plus the 13 EIA regions), last 3 days rolling. Hourly history is deliberately not kept (~90M rows).
EIA_BASE = "https://api.eia.gov/v2/electricity/rto/"
EIA_SETS = {"demand": "region-data", "fuel": "fuel-type-data"}   # hourly routes; daily totals live under daily-<route>
EIA_REGIONS = ["US48", "CAL", "CAR", "CENT", "FLA", "MIDA", "MIDW", "NE", "NY", "NW", "SE", "SW", "TEN", "TEX"]


def _eia_pages(dataset, frequency, start, end, respondents=None):
    key = os.environ.get("EIA_API_KEY")
    if not key: raise RuntimeError("EIA_API_KEY is not set (secret eia-api-key)")
    params = {"api_key": key, "frequency": frequency, "data[0]": "value", "start": start, "end": end,
              "sort[0][column]": "period", "sort[0][direction]": "asc", "length": 5000}
    if frequency == "daily": params["facets[timezone][]"] = "Eastern"
    if respondents: params["facets[respondent][]"] = respondents
    out, offset = [], 0
    while True:
        route = ("daily-" if frequency == "daily" else "") + EIA_SETS[dataset]
        try:
            js = get(EIA_BASE + route + "/data/", params={**params, "offset": offset})
        except Exception as e:   # the request URL carries the key: keep it out of the run log
            raise RuntimeError(f"EIA {route} {frequency} {start}..{end}: " + str(e).replace(key, "***")[:300]) from None
        resp = js.get("response") or {}
        data = resp.get("data") or []
        out += data
        total = int(resp.get("total") or 0)
        offset += len(data)
        if not data or offset >= total: return out


def _eia_rows(dataset, frequency, recs, now):
    out = []
    for r in recs:
        per = r.get("period") or ""
        typ = r.get("type") or r.get("fueltype")
        if not per or not typ: continue
        ts = f"{per[:10]} {per[11:13]}:00:00" if frequency == "hourly" and len(per) >= 13 else None
        out.append(dict(dataset=dataset, frequency=frequency, period=per, period_date=per[:10], period_ts=ts,
                        respondent=r.get("respondent"), respondent_name=r.get("respondent-name"),
                        type=typ, type_name=r.get("type-name"), value=f(r.get("value")),
                        units=r.get("value-units"), ingested_at=now))
    return out


def pull_eia(now):
    """Daily totals for the last 10 days (all respondents) and hourly for the regions over the last 3 days."""
    today = dt.date.today()
    rows = []
    for ds in EIA_SETS:
        d = _eia_pages(ds, "daily", (today - dt.timedelta(days=10)).isoformat(), today.isoformat())
        h = _eia_pages(ds, "hourly", f"{today - dt.timedelta(days=3)}T00", f"{today}T23", EIA_REGIONS)
        if "--dry" in sys.argv and d: print(f"  eia {ds}: {len(d)} daily, {len(h)} hourly; fields: {sorted(d[0])}", flush=True)
        rows += _eia_rows(ds, "daily", d, now) + _eia_rows(ds, "hourly", h, now)
    return {"eia_grid": rows}


def pull_eia_history(now, start=dt.date(2015, 7, 1)):
    """One-off: daily totals for every respondent since July 2015, a quarter at a time (quarters already loaded skipped)."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    have = {(r.d, r.q) for r in bq.query(f"SELECT DISTINCT dataset AS d, FORMAT_DATE('%Y-%Q', period_date) AS q "
                                         f"FROM `{PROJECT}.{TABLES['eia_grid']}` WHERE frequency = 'daily'").result()}
    end = dt.date.today() - dt.timedelta(days=10)
    for ds in EIA_SETS:
        q0 = start
        while q0 <= end:
            q1 = (q0.replace(day=1) + dt.timedelta(days=95)).replace(day=1)
            while (q1.month - 1) % 3: q1 = (q1 - dt.timedelta(days=1)).replace(day=1)
            tag = f"{q0.year}-{(q0.month - 1) // 3 + 1}"
            if (ds, tag) not in have:
                try:
                    rows = _eia_rows(ds, "daily", _eia_pages(ds, "daily", q0.isoformat(), (q1 - dt.timedelta(days=1)).isoformat()), now)
                    write(bq, "eia_grid", rows)
                    print(f"  eia {ds} {tag}: {len(rows)} rows", flush=True)
                except Exception as e:
                    print(f"  eia {ds} {tag}: FAILED {type(e).__name__}: {e}"[:300], flush=True)
            q0 = q1
    return {}


# ------------------------------------------------------------------ EIA weekly: natural gas storage + petroleum status (added 2026-10-23)
# The two scheduled weekly reports that move energy futures: Weekly Natural Gas Storage (Thursday 10:30 ET, Bcf, lower 48
# and the 5 regions) and the Weekly Petroleum Status Report (Wednesday 10:30 ET: crude / gasoline / distillate stocks,
# Cushing, SPR, production, imports, exports, refinery utilization). period = the week-ending date. Same EIA key.
EIA_WEEKLY = {
    "natural-gas/stor/wkly": None,   # every series on the route (~15: lower 48, regions, salt / non-salt)
    "petroleum/stoc/wstk": ["WCESTUS1", "WCSSTUS1", "WCRSTUS1", "W_EPC0_SAX_YCUOK_MBBL", "WGTSTUS1", "WDISTUS1",
                            "WPRSTUS1", "WKJSTUS1", "WRESTUS1"],
    "petroleum/sum/sndw": ["WCRFPUS2", "WCRIMUS2", "WCREXUS2", "WCRRIUS2", "WPULEUS3", "WRPUPUS2", "WGFUPUS2", "WDIUPUS2"],
}


def _eia_weekly_pages(route, series, start):
    key = os.environ.get("EIA_API_KEY")
    if not key: raise RuntimeError("EIA_API_KEY is not set (secret eia-api-key)")
    params = {"api_key": key, "frequency": "weekly", "data[0]": "value", "start": start,
              "sort[0][column]": "period", "sort[0][direction]": "asc", "length": 5000}
    if series: params["facets[series][]"] = series
    out, offset = [], 0
    while True:
        try:
            js = get(f"https://api.eia.gov/v2/{route}/data/", params={**params, "offset": offset})
        except Exception as e:   # the request URL carries the key: keep it out of the run log
            raise RuntimeError(f"EIA {route} weekly from {start}: " + str(e).replace(key, "***")[:300]) from None
        resp = js.get("response") or {}
        data = resp.get("data") or []
        out += data; offset += len(data)
        if not data or offset >= int(resp.get("total") or 0): return out


def _eia_weekly_rows(route, recs, now):
    out = []
    for r in recs:
        per, ser = (r.get("period") or "")[:10], r.get("series")
        if not per or not ser: continue
        out.append(dict(route=route, series=ser, series_name=r.get("series-description") or r.get("seriesDescription"),
                        area=r.get("area-name") or r.get("duoarea"), product=r.get("product-name") or r.get("process-name"),
                        period=per, value=f(r.get("value")), units=r.get("units"), ingested_at=now))
    return out


def pull_eia_weekly(now, start=None):
    """The last 70 days of every weekly series (EIA revises the prior week); start='2000-01-01' for history."""
    since = start or (dt.date.today() - dt.timedelta(days=70)).isoformat()
    rows = []
    for route, series in EIA_WEEKLY.items():
        recs = _eia_weekly_pages(route, series, since)
        if "--dry" in sys.argv and recs:
            print(f"  eia weekly {route}: {len(recs)} rows, {len({r.get('series') for r in recs})} series; fields: {sorted(recs[0])}", flush=True)
        rows += _eia_weekly_rows(route, recs, now)
    return {"eia_weekly": rows}


def pull_eia_weekly_history(now):
    """One-off: every weekly series since 2000 (~25 years x ~30 series, small)."""
    return pull_eia_weekly(now, start="2000-01-01")


# ------------------------------------------------------------------ NOAA CPC degree days (added 2026-10-23)
# Daily heating / cooling degree days by US census division and CONUS, weighted by population, by homes heating with
# natural gas (the gas-demand series traders watch) and by electric heat. One pipe-delimited file per year and series:
# https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/<year>/<Weights>.<Heating|Cooling>.txt
# (header lines, then "Region|YYYYMMDD|...", one row per region). Each run reloads the current year (CPC revises).
CPC_DD = "https://ftp.cpc.ncep.noaa.gov/htdocs/degree_days/weighted/daily_data/{y}/{w}.{k}.txt"
# (weights label stored, CPC file names to try in order, Heating / Cooling); CPC names: UtilityGas, Electricity, Population
DD_FILES = [("gas", ("UtilityGas", "Gas"), "Heating"), ("population", ("Population",), "Heating"),
            ("population", ("Population",), "Cooling"), ("electric", ("Electricity", "Electric"), "Heating")]
DD_REGIONS = {"1": "New England", "2": "Middle Atlantic", "3": "East North Central", "4": "West North Central",
              "5": "South Atlantic", "6": "East South Central", "7": "West South Central", "8": "Mountain",
              "9": "Pacific", "CONUS": "Contiguous US"}


def _dd_year(year, now):
    rows = []
    for weights, names, kind in DD_FILES:
        txt = None
        for name in names:
            txt = _text(CPC_DD.format(y=year, w=name, k=kind))
            if txt: break
        if not txt:
            print(f"  degree days {year}: no {names[0]}.{kind} file", flush=True); continue
        lines = [l.strip() for l in txt.splitlines() if "|" in l]
        if not lines or not lines[0].lower().startswith("region"): continue
        dates = [_iso(d) for d in lines[0].split("|")[1:]]
        for line in lines[1:]:
            cells = line.split("|")
            reg = cells[0].strip()
            for d, v in zip(dates, cells[1:]):
                val = f(v)
                if d and val is not None and val > -999:
                    rows.append(dict(date=d, region=reg, region_name=DD_REGIONS.get(reg, reg), kind=kind.lower(),
                                     weights=weights, value=val, ingested_at=now))
    return rows


def pull_degree_days(now):
    """This year's file (and last year's in January, when CPC can still revise December)."""
    today = dt.date.today()
    years = [today.year] + ([today.year - 1] if today.month == 1 else [])
    return {"degree_days": [r for y in years for r in _dd_year(y, now)]}


def pull_degree_days_history(now, start=2000):
    """One-off: every year since 2000, a year at a time."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    for y in range(start, dt.date.today().year):
        rows = _dd_year(y, now)
        write(bq, "degree_days", rows)
        print(f"  degree days {y}: {len(rows)} rows", flush=True)
    return {}

# ------------------------------------------------------------------ ETF shares outstanding + NAV (added 2026-10-26, b210)
# Daily dollar flow of an ETF = change in shares outstanding x NAV (creations and redemptions), computed in the
# rrp.etf_flows_daily view. Two free sources, probed 2026-10-26:
#   State Street SPDR: one NAV history file per fund (xlsx, sheet 'navhist': Date | NAV | Shares Outstanding | Total Net
#     Assets, newest first, the fund's whole history). The daily pass keeps the last 10 rows (MERGE fixes revisions).
#   iShares: the US product screener JSON, every iShares ETF in one call with NAV and total net assets as of the last
#     close; shares outstanding = total net assets / NAV. No history file answers, so iShares history starts 2026-10-26.
#   SPDR Gold (GLD): the trust's daily archive (xlsx, Nov 2004 on): shares = total ounces in the trust / ounces per share.
#   VanEck (GDX, GDXJ, SMH): each fund's price-history download (xlsx, newest first, NAV and AUM; weekends repeat Friday's
#     row with no volume and are skipped): shares = AUM / NAV.
# Vanguard (VOX, VIS, VAW) and Invesco (QQQ) gave no usable file; their mapped funds use the secondary ETF.
SSGA_NAVHIST = "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/navhist-us-en-{t}.xlsx"
SSGA_TICKERS = ["XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",          # the 11 sectors
                "XBI", "KBE", "KRE", "KCE", "KIE", "XHB", "XRT", "XOP", "XES", "XME", "XTN", "XTL", "XAR",
                "XPH", "XHE", "XHS", "XSD", "XSW", "SPY", "MDY", "DIA"]
ISHARES_SCREENER = ("https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn?dcrPath=/templatedata/config/"
                    "product-screener-v3/data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true")
GLD_ARCHIVE = "https://api.spdrgoldshares.com/api/v1/historical-archive?product=gld&exchange=NYSE&lang=en"
VANECK = {"GDX": "gold-miners-etf-gdx", "GDXJ": "junior-gold-miners-etf-gdxj", "SMH": "semiconductor-etf-smh"}
VANECK_HIST = "https://www.vaneck.com/us/en/investments/{slug}/downloads/fundhistoprices/"
BROWSER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/129.0 Safari/537.36"}


def _ssga_fund(ticker, now, keep=None):
    """One SPDR NAV history file -> rows (newest first; keep = how many to return, None = all)."""
    import openpyxl
    try:
        r = requests.get(SSGA_NAVHIST.format(t=ticker.lower()), headers=BROWSER_UA, timeout=60)
    except requests.RequestException as e:
        print(f"  {ticker}: {type(e).__name__}", flush=True); return []
    if not r.ok or b"PK" not in r.content[:4]:
        print(f"  {ticker}: no NAV history file ({r.status_code})", flush=True); return []
    wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        name, rows, cols = None, [], None
        for row in ws.iter_rows(values_only=True):
            cells = [c for c in row]
            first = str(cells[0]).strip() if cells and cells[0] is not None else ""
            if first == "Fund Name:" and len(cells) > 1: name = cells[1]
            if cols is None:
                if first == "Date":
                    cols = {str(c).strip().lower(): i for i, c in enumerate(cells) if c is not None}
                continue
            if isinstance(cells[0], (dt.datetime, dt.date)):
                d = cells[0].date() if isinstance(cells[0], dt.datetime) else cells[0]
            else:
                try: d = dt.datetime.strptime(first, "%d-%b-%Y").date()
                except ValueError: break   # footnotes after the table
            g = lambda k: f(cells[cols[k]]) if k in cols and cols[k] < len(cells) else None
            rows.append(dict(date=d.isoformat(), symbol=ticker, issuer="State Street", fund_name=name, nav=g("nav"),
                             shares_out=g("shares outstanding"), total_net_assets=g("total net assets"),
                             source="ssga_navhist", ingested_at=now))
            if keep and len(rows) >= keep: break
    finally:
        wb.close()
    return [x for x in rows if x["shares_out"] and x["nav"]]


def _xlsx_rows(url):
    """All rows of the first data sheet of an xlsx download (None when the answer is not an xlsx)."""
    import openpyxl, warnings
    r = requests.get(url, headers=BROWSER_UA, timeout=90)
    if not r.ok or r.content[:2] != b"PK":
        print(f"  {url}: no xlsx ({r.status_code})", flush=True); return None
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")   # VanEck files carry no default style
        wb = openpyxl.load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    try:
        ws = next((w for w in wb.worksheets if "disclaimer" not in w.title.lower()), wb.worksheets[0])
        return [list(row) for row in ws.iter_rows(values_only=True)]
    finally:
        wb.close()


def _num(v):
    return f(str(v).replace(",", "").replace("$", "").strip()) if v not in (None, "") else None


def _day(v, fmt):
    if isinstance(v, dt.datetime): return v.date()
    if isinstance(v, dt.date): return v
    try: return dt.datetime.strptime(str(v).strip(), fmt).date()
    except ValueError: return None


def _gld(now, keep=None):
    rows = _xlsx_rows(GLD_ARCHIVE) or []
    head = next((i for i, r in enumerate(rows) if r and str(r[0]).strip() == "Date"), None)
    if head is None: return []
    col = {str(c).strip(): i for i, c in enumerate(rows[head]) if c is not None}
    nav_c = next((c for c in col if c.startswith("NAV/Share")), None)
    out = []
    for r in rows[head + 1:]:
        d = _day(r[0], "%d-%b-%Y")
        nav, opps = _num(r[col[nav_c]]) if nav_c else None, _num(r[col["Ounces of Gold per Share"]])
        oz, tna = _num(r[col["Total Ounces of Gold in the Trust"]]), _num(r[col["Total Net Asset Value in the Trust"]])
        if not d or not nav or not opps or not oz: continue   # holidays carry text in every column
        out.append(dict(date=d.isoformat(), symbol="GLD", issuer="SPDR Gold", fund_name="SPDR Gold Shares", nav=nav,
                        shares_out=round(oz / opps), total_net_assets=tna, source="spdr_gold_archive", ingested_at=now))
    return out[-keep:] if keep else out   # oldest first in this file


def _vaneck(ticker, now, keep=None):
    rows = _xlsx_rows(VANECK_HIST.format(slug=VANECK[ticker])) or []
    head = next((i for i, r in enumerate(rows) if r and str(r[0]).strip() == "Date"), None)
    if head is None: return []
    col = {str(c).strip(): i for i, c in enumerate(rows[head]) if c is not None}
    name = str(rows[0][0]).strip() if rows and rows[0] else ticker
    out = []
    for r in rows[head + 1:]:
        d = _day(r[0], "%m/%d/%Y")
        if not d or d.weekday() >= 5: continue   # weekends repeat Friday
        nav, aum = _num(r[col["NAV"]]), _num(r[col["AUM"]])
        if not nav or not aum: continue
        out.append(dict(date=d.isoformat(), symbol=ticker, issuer="VanEck", fund_name=name, nav=nav,
                        shares_out=round(aum / nav), total_net_assets=aum, source="vaneck_history", ingested_at=now))
        if keep and len(out) >= keep: break
    return out


def _ishares(now):
    r = requests.get(ISHARES_SCREENER, headers=BROWSER_UA, timeout=90); r.raise_for_status()
    rows = []
    for rec in r.json().values():
        t = (rec.get("localExchangeTicker") or "").strip().upper()
        nav, tna = rec.get("navAmount") or {}, rec.get("totalNetAssetsFund") or rec.get("totalNetAssets") or {}
        nav_d, tna_d = (rec.get("navAmountAsOf") or {}).get("r"), (rec.get("totalNetAssetsFundAsOf") or {}).get("r")
        nv, tv = f(nav.get("r") if isinstance(nav, dict) else None), f(tna.get("r") if isinstance(tna, dict) else None)
        if not t or not nv or not tv or not nav_d or nav_d != tna_d: continue   # both figures must be the same day
        d = str(nav_d)
        rows.append(dict(date=f"{d[:4]}-{d[4:6]}-{d[6:8]}", symbol=t, issuer="iShares", fund_name=rec.get("fundName"),
                         nav=nv, shares_out=round(tv / nv), total_net_assets=tv, source="ishares_screener", ingested_at=now))
    return rows


def pull_etf_shares(now):
    """Weekday pass: last 10 days of every SPDR in SSGA_TICKERS, plus today's snapshot of every iShares ETF."""
    rows = []
    for t in SSGA_TICKERS:
        rows += _ssga_fund(t, now, keep=10); time.sleep(0.5)
    spdr = len(rows)
    rows += _gld(now, keep=10)
    for t in VANECK: rows += _vaneck(t, now, keep=10)
    other = len(rows) - spdr
    rows += _ishares(now)
    print(f"  etf shares: {spdr} SPDR rows ({len({r['symbol'] for r in rows[:spdr]})} funds), {other} GLD / VanEck rows, "
          f"{len(rows) - spdr - other} iShares funds", flush=True)
    return {"etf_shares": rows}


def pull_etf_shares_history(now):
    """One-off: every SPDR's whole NAV history, a fund at a time."""
    from google.cloud import bigquery
    bq = bigquery.Client(project=PROJECT)
    for t in SSGA_TICKERS:
        rows = _ssga_fund(t, now)
        if rows: write(bq, "etf_shares", rows)
        print(f"  {t}: {len(rows)} days" + (f", {rows[-1]['date']} to {rows[0]['date']}" if rows else ""), flush=True)
        time.sleep(0.5)
    for t, rows in [("GLD", _gld(now))] + [(t, _vaneck(t, now)) for t in VANECK]:
        if rows: write(bq, "etf_shares", rows)
        ds = sorted(r["date"] for r in rows)
        print(f"  {t}: {len(rows)} days" + (f", {ds[0]} to {ds[-1]}" if ds else ""), flush=True)
    return {}


PULLERS = [("kraken_ticker", pull_kraken_ticker), ("coinbase", pull_coinbase), ("coinbase_trending", pull_coinbase_trending),
           ("kraken_ohlc", pull_kraken_ohlc), ("deribit", pull_deribit), ("coinpaprika", pull_global),
           ("fear_greed", pull_fng), ("mempool", pull_fees), ("defillama_stablecoins", pull_stablecoins),
           ("macro_fred", pull_fred), ("macro_treasury", pull_treasury), ("macro_yahoo", pull_yahoo),
           ("crypto_market", pull_crypto_market), ("farside_etf", pull_farside), ("funding", pull_funding),
           ("farside_history", pull_farside_history), ("funding_history", pull_funding_history),
           ("fear_greed_history", pull_fng_history),
           ("universe", pull_universe), ("equities", pull_equities),
           ("short_volume", pull_short_volume), ("short_interest", pull_short_interest),
           ("short_volume_history", pull_short_volume_history), ("short_interest_history", pull_short_interest_history),
           ("treasury_dts", pull_treasury_dts), ("treasury_dts_history", pull_treasury_dts_history),
           ("eia", pull_eia), ("eia_history", pull_eia_history),
           ("eia_weekly", pull_eia_weekly), ("eia_weekly_history", pull_eia_weekly_history),
           ("fred_liquidity_history", pull_fred_liquidity_history),
           ("degree_days", pull_degree_days), ("degree_days_history", pull_degree_days_history),
           ("etf_shares", pull_etf_shares), ("etf_shares_history", pull_etf_shares_history),
           ("crypto_hourly", pull_crypto_hourly), ("crypto_hourly_history", pull_crypto_hourly_history),
           ("crypto_orderbook", pull_crypto_orderbook)]
ON_REQUEST = {"farside_history", "funding_history", "fear_greed_history", "short_volume_history", "short_interest_history", "treasury_dts_history", "eia_history", "eia_weekly_history", "fred_liquidity_history", "degree_days_history", "etf_shares_history", "crypto_hourly_history"}   # only with --only <name> (cef_nav dropped 2026-10-19: CEFConnect never answered)
DAILY = {"funding"}   # once a day, in the 00:15 UTC --crypto-daily execution (1-hour timeout)
CRYPTO_DAILY_ONLY = "--crypto-daily" in sys.argv
EQUITIES_ONLY = "--equities-only" in sys.argv   # the daily stocks/ETFs pass (own scheduler + 1-hour timeout)
EQUITY_PASS = ("universe", "equities", "short_volume", "short_interest", "treasury_dts", "eia", "eia_weekly", "degree_days", "etf_shares")   # the sources that run only in that pass
HOURLY_ONLY = {"crypto_market", "farside_etf", "defillama_stablecoins", "fear_greed", "macro_fred", "macro_treasury", "mempool", "crypto_orderbook"}
# macro_cboe dropped 2026-10-29 (rebuild b252): Yahoo's ^VIX is the same close on 99%+ of days and is now the canonical VIX
# from that date (rrp.macro_daily); the Cboe history stays in markets.macro.
# subject split (rebuild b252, 2026-10-29): rrp-markets runs this file with RRP_SUBJECT=markets (everything but the agency
# data), rrp-gov with RRP_SUBJECT=gov (Treasury, EIA, NOAA degree days), hourly at :25 UTC: there the hourly gates are off
# and the agency daily sources run in the 22:25 UTC weekday run (after the 22:20 slot the health board checks) or with
# --equities. Unset = all sources (the old rrp-market job, one-off runs and backfills).
SUBJECT = os.environ.get("RRP_SUBJECT") or None
GOV_SOURCES = {"macro_treasury", "treasury_dts", "eia", "eia_weekly", "degree_days",
               "treasury_dts_history", "eia_history", "eia_weekly_history", "degree_days_history"}   # slow-moving: first run of each hour only
HALF_HOUR_ONLY = {"crypto_hourly"}   # second run of each hour only: the first run already averages ~160 s of a 600 s limit

# ------------------------------------------------------------------ BigQuery schema
F, I, T, D, B, St = "FLOAT", "INTEGER", "TIMESTAMP", "DATE", "BOOLEAN", "STRING"
SCHEMAS = {
    "spot": [("ts", T), ("source", St), ("symbol", St), ("pair", St), ("last", F), ("bid", F), ("ask", F), ("open_utc_day", F),
             ("high_24h", F), ("low_24h", F), ("volume_24h", F), ("vwap_24h", F), ("trades_24h", I), ("exchange_time", T)],
    "trending": [("ts", T), ("list_rank", I), ("symbol", St), ("name", St), ("market_cap_rank", I), ("market_cap_usd", F),
                 ("price_usd", F), ("volume_24h_usd", F), ("pct_change_hour", F), ("pct_change_day", F), ("pct_change_week", F)],
    "ohlc": [("source", St), ("symbol", St), ("interval_min", I), ("bar_start", T), ("open", F), ("high", F), ("low", F),
             ("close", F), ("vwap", F), ("volume", F), ("trades", I), ("is_open", B), ("ingested_at", T)],
    "indicators_daily": [("symbol", St), ("date", D), ("close", F), ("ma20", F), ("ma50", F), ("rsi14", F)],
    "derivs": [("ts", T), ("venue", St), ("instrument", St), ("symbol", St), ("mark", F), ("index_price", F), ("last", F),
               ("funding_8h", F), ("current_funding", F), ("open_interest_usd", F), ("volume_24h_usd", F), ("high_24h", F),
               ("low_24h", F), ("price_change_24h_pct", F), ("exchange_time", T)],
    "global": [("ts", T), ("source", St), ("market_cap_usd", F), ("volume_24h_usd", F), ("btc_dominance_pct", F),
               ("market_cap_change_24h_pct", F), ("volume_change_24h_pct", F), ("source_updated", T)],
    "fear_greed": [("date", D), ("value", I), ("classification", St), ("ingested_at", T)],
    "btc_fees": [("ts", T), ("fastest", F), ("half_hour", F), ("hour", F), ("economy", F), ("minimum", F)],
    "stablecoins": [("ts", T), ("id", St), ("symbol", St), ("name", St), ("peg_type", St), ("peg_mechanism", St),
                    ("circulating", F), ("prev_day", F), ("prev_week", F), ("prev_month", F)],
    "runs": [("ts", T), ("source", St), ("ok", B), ("row_count", I), ("seconds", F), ("error", St)],
    "etf_flows": [("asset", St), ("date", D), ("fund", St), ("flow_usd_m", F), ("source_url", St), ("ingested_at", T)],
    "funding": [("venue", St), ("instrument", St), ("symbol", St), ("ts", T), ("funding_rate", F), ("relative_funding_rate", F),
                ("ingested_at", T)],
    "macro": [("source", St), ("series", St), ("label", St), ("obs_date", D), ("close", F), ("open", F), ("high", F),
              ("low", F), ("quote_time", St), ("ingested_at", T)],
    "crypto_market": [("ts", T), ("id", St), ("symbol", St), ("name", St), ("rank", I), ("price", F), ("volume_24h", F),
                      ("market_cap", F), ("pct_1h", F), ("pct_24h", F), ("pct_7d", F), ("pct_30d", F), ("pct_1y", F),
                      ("ath_price", F), ("ath_date", D), ("pct_from_ath", F), ("circulating_supply", F), ("total_supply", F),
                      ("max_supply", F), ("first_data_at", D), ("last_updated", T)],
    "crypto_daily": [("venue", St), ("pair", St), ("base", St), ("quote", St), ("date", D), ("open", F), ("high", F),
                     ("low", F), ("close", F), ("vwap", F), ("volume", F), ("trades", I), ("is_open", B), ("ingested_at", T)],
    "crypto_hourly": [("venue", St), ("pair", St), ("base", St), ("ts", T), ("open", F), ("high", F), ("low", F),
                      ("close", F), ("volume", F), ("is_open", B), ("ingested_at", T)],
    "crypto_orderbook": [("ts", T), ("venue", St), ("pair", St), ("base", St), ("vol_rank", I), ("volume_24h_usd", F),
                         ("best_bid", F), ("best_ask", F), ("mid", F), ("spread", F), ("spread_bps", F), ("depth_bid", F),
                         ("depth_ask", F), ("depth_bid_usd", F), ("depth_ask_usd", F), ("ob_imbalance", F), ("levels", I),
                         ("ingested_at", T)],
    "equity_universe": [("as_of", D), ("symbol", St), ("name", St), ("exchange", St), ("market_category", St),
                        ("is_etf", B), ("test_issue", B), ("financial_status", St), ("source_file", St), ("ingested_at", T)],
    "short_volume": [("date", D), ("symbol", St), ("short_volume", F), ("short_exempt_volume", F), ("total_volume", F),
                     ("market", St), ("ingested_at", T)],
    "short_interest": [("settlement_date", D), ("symbol", St), ("name", St), ("market", St), ("short_qty", F),
                       ("prev_short_qty", F), ("avg_daily_vol", F), ("days_to_cover", F), ("change_pct", F), ("split_flag", St),
                       ("revision", St), ("source_file", St), ("ingested_at", T)],
    "treasury_dts": [("record_date", D), ("table_id", St), ("line", I), ("account_type", St), ("transaction_type", St),
                     ("category", St), ("category_desc", St), ("security_market", St), ("today_amt", F), ("mtd_amt", F),
                     ("fytd_amt", F), ("open_today_bal", F), ("close_today_bal", F), ("open_month_bal", F),
                     ("open_fy_bal", F), ("raw", St), ("ingested_at", T)],
    "eia_grid": [("dataset", St), ("frequency", St), ("period", St), ("period_date", D), ("period_ts", T), ("respondent", St),
                 ("respondent_name", St), ("type", St), ("type_name", St), ("value", F), ("units", St), ("ingested_at", T)],
    "degree_days": [("date", D), ("region", St), ("region_name", St), ("kind", St), ("weights", St), ("value", F),
                    ("ingested_at", T)],
    "eia_weekly": [("route", St), ("series", St), ("series_name", St), ("area", St), ("product", St), ("period", D),
                   ("value", F), ("units", St), ("ingested_at", T)],
    "etf_shares": [("date", D), ("symbol", St), ("issuer", St), ("fund_name", St), ("nav", F), ("shares_out", F),
                   ("total_net_assets", F), ("source", St), ("ingested_at", T)],
    "equity_daily": [("symbol", St), ("date", D), ("open", F), ("high", F), ("low", F), ("close", F), ("adj_close", F),
                     ("volume", F), ("currency", St), ("exchange", St), ("instrument_type", St), ("ingested_at", T)],
}
MERGE_KEYS = {"ohlc": ["source", "symbol", "interval_min", "bar_start"], "indicators_daily": ["symbol", "date"],
              "fear_greed": ["date"], "macro": ["source", "series", "obs_date"],
              "equity_daily": ["symbol", "date"], "equity_universe": ["symbol", "as_of"],
              "crypto_daily": ["venue", "pair", "date"], "crypto_hourly": ["venue", "pair", "ts"], "etf_flows": ["asset", "date", "fund"],
              "funding": ["venue", "instrument", "ts"], "short_volume": ["date", "symbol"],
              "short_interest": ["settlement_date", "symbol"], "treasury_dts": ["record_date", "table_id", "line"],
              "eia_grid": ["dataset", "frequency", "respondent", "type", "period_date", "period"],
              "eia_weekly": ["series", "period"], "degree_days": ["date", "region", "kind", "weights"],
              "etf_shares": ["symbol", "date"]}
PARTITION = {"spot": "ts", "trending": "ts", "ohlc": "bar_start", "derivs": "ts", "global": "ts", "btc_fees": "ts",
             "stablecoins": "ts", "runs": "ts", "indicators_daily": "date", "fear_greed": "date", "macro": "obs_date",
             "equity_daily": "date", "equity_universe": "as_of",
             "crypto_market": "ts", "crypto_daily": "date", "crypto_hourly": "ts", "crypto_orderbook": "ts", "etf_flows": "date",
             "funding": "ts", "short_volume": "date", "short_interest": "settlement_date",
             "treasury_dts": "record_date", "eia_grid": "period_date", "eia_weekly": "period", "degree_days": "date",
             "etf_shares": "date"}
CLUSTER = {"spot": ["symbol", "source"], "ohlc": ["symbol", "interval_min"], "indicators_daily": ["symbol"],
           "derivs": ["symbol"], "stablecoins": ["symbol"], "runs": ["source"], "macro": ["label", "source"],
           "equity_daily": ["symbol"], "equity_universe": ["symbol"],
           "crypto_market": ["symbol"], "crypto_daily": ["base", "venue"], "crypto_hourly": ["pair"], "crypto_orderbook": ["pair"],
           "etf_flows": ["asset", "fund"], "funding": ["symbol", "instrument"], "short_volume": ["symbol"],
           "short_interest": ["symbol"], "treasury_dts": ["table_id"], "eia_grid": ["dataset", "respondent"], "eia_weekly": ["series"], "degree_days": ["region", "kind"],
           "etf_shares": ["symbol"]}


def bq_schema(name):
    from google.cloud import bigquery
    return [bigquery.SchemaField(c, t) for c, t in SCHEMAS[name]]


def write(bq, table, rows, append=False):
    """MERGE on MERGE_KEYS (append=True skips the MERGE: only for rows known not to exist yet, e.g. a first backfill)."""
    from google.cloud import bigquery
    if not rows: return 0
    cols = [c for c, _ in SCHEMAS[table]]
    rows = [{c: r.get(c) for c in cols} for r in rows]
    cfg = bigquery.LoadJobConfig(schema=bq_schema(table), source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    if table in MERGE_KEYS and len(rows) > 50000 and not append:
        return sum(write(bq, table, rows[i:i + 50000]) for i in range(0, len(rows), 50000))
    if table in MERGE_KEYS and not append:
        stage = f"{PROJECT}.staging.market_{table}"
        # a fresh table each time: staging tables expire a day after they are CREATED (WRITE_TRUNCATE keeps the old
        # expiry), so a once-a-day load could hit the expiry mid-run - it did on Oct 21 (equities half loaded)
        bq.delete_table(stage, not_found_ok=True)
        cfg.write_disposition = "WRITE_TRUNCATE"
        bq.load_table_from_json(rows, stage, job_config=cfg).result()
        on = " AND ".join(f"t.{k} = s.{k}" for k in MERGE_KEYS[table])
        pc = PARTITION.get(table)   # constant bounds on the partition column, so the MERGE reads only those partitions
        if pc in MERGE_KEYS[table]:
            vals = [str(r[pc]) for r in rows if r.get(pc)]
            if vals:
                kind = "DATE" if dict(SCHEMAS[table])[pc] == D else "TIMESTAMP"
                on += f" AND t.{pc} BETWEEN {kind} '{min(vals)[:19]}' AND {kind} '{max(vals)[:19]}'"
        upd = ", ".join(f"{c} = s.{c}" for c in cols if c not in MERGE_KEYS[table])
        bq.query(f"""MERGE `{PROJECT}.{TABLES[table]}` t USING `{stage}` s ON {on}
                     WHEN MATCHED THEN UPDATE SET {upd}
                     WHEN NOT MATCHED THEN INSERT ({", ".join(cols)}) VALUES ({", ".join("s." + c for c in cols)})""").result()
    else:
        cfg.write_disposition = "WRITE_APPEND"
        bq.load_table_from_json(rows, f"{PROJECT}.{TABLES[table]}", job_config=cfg).result()
    return len(rows)


# ------------------------------------------------------------------ run
def run(dry=False, names=None):
    now_dt = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    now = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    bq = None
    if not dry:
        from google.cloud import bigquery
        bq = bigquery.Client(project=PROJECT)
    health = []
    FULL_OHLC[0] = now_dt.minute < 15 or "--full" in sys.argv
    only = sys.argv[sys.argv.index("--only") + 1] if "--only" in sys.argv else None
    for name, fn in PULLERS:
        if only and name != only: continue
        if name in ON_REQUEST and only != name: continue
        if SUBJECT == "markets" and name in GOV_SOURCES: continue
        if SUBJECT == "gov":
            if name not in GOV_SOURCES: continue
            if name in EQUITY_PASS and only != name and "--equities" not in sys.argv \
                    and not (now_dt.hour == 22 and now_dt.weekday() < 5): continue
        elif names is not None:
            if name not in names: continue
        elif name in DAILY and not only: continue
        if SUBJECT != "gov" and name in HOURLY_ONLY and not only and now_dt.minute >= 15 and "--full" not in sys.argv: continue
        if name in HALF_HOUR_ONLY and not only and now_dt.minute < 15: continue
        if EQUITIES_ONLY and name not in EQUITY_PASS: continue
        if CRYPTO_DAILY_ONLY and names is None: continue
        if SUBJECT != "gov" and name in EQUITY_PASS and not EQUITIES_ONLY and "--equities" not in sys.argv and only != name: continue
        t0 = time.time(); n = 0; err = None
        try:
            out = fn(now)
            for table, rows in out.items():
                n += len(rows) if dry else write(bq, table, rows)
        except Exception as e:
            err = f"{type(e).__name__}: {e}"[:500]
        health.append(dict(ts=now, source=name, ok=err is None, row_count=n, seconds=round(time.time() - t0, 2), error=err))
        print(f"{name}: {'ok' if err is None else 'FAILED ' + err} ({n} rows, {health[-1]['seconds']}s)", flush=True)
    if not dry:
        try: write(bq, "runs", health)
        except Exception as e: print(f"health write FAILED: {e}")
    print(f"market ingest done: {sum(h['ok'] for h in health)}/{len(health)} sources ok")
    if not dry and names is None and SUBJECT in (None, "markets") and (now_dt.minute < 15 or "--alerts" in sys.argv):
        alert_check(bq)


def alert_check(bq):
    """Once an hour: read rrp.alerts (cheap) and write one structured log line. Cloud Monitoring turns
    rrp_alert = "red" lines into a phone push, and the absence of rrp_heartbeat lines for 2 hours into another
    (the check itself stopped). Never raises: a failed check logs rrp_alert = "check_failed" instead."""
    import json as _json
    try:
        rows = [dict(r) for r in bq.query(f"SELECT kind, item, CAST(last_ok AS STRING) AS last_ok, detail FROM `{PROJECT}.rrp.alerts`").result()]
    except Exception as e:
        print(_json.dumps({"severity": "ERROR", "rrp_alert": "check_failed", "rrp_heartbeat": 1,
                           "message": f"RRP alert check failed: {type(e).__name__}: {e}"[:500]}), flush=True)
        return
    if rows:
        msg = "RRP RED: " + "; ".join(f"{r['item']} ({r['detail'][:80]})" for r in rows)
        print(_json.dumps({"severity": "ERROR", "rrp_alert": "red", "rrp_heartbeat": 1, "rrp_red_count": len(rows),
                           "message": msg[:1000], "items": rows}), flush=True)
    else:
        print(_json.dumps({"severity": "INFO", "rrp_alert": "ok", "rrp_heartbeat": 1, "message": "RRP health ok"}), flush=True)


def setup():
    """Create any missing table (partitioned and clustered). Datasets come from warehouse/reorg.py."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    bq = bigquery.Client(project=PROJECT)
    for name in SCHEMAS:
        ref = f"{PROJECT}.{TABLES[name]}"
        try:
            bq.get_table(ref); print(f"  {TABLES[name]} exists"); continue
        except NotFound:
            pass
        t = bigquery.Table(ref, schema=bq_schema(name))
        col = PARTITION.get(name)
        if col:
            t.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH if name in ("ohlc", "indicators_daily", "fear_greed", "macro", "equity_daily", "etf_flows", "funding", "short_volume", "short_interest", "treasury_dts", "eia_grid", "eia_weekly", "degree_days", "etf_shares") else bigquery.TimePartitioningType.DAY, field=col)
        if name in CLUSTER: t.clustering_fields = CLUSTER[name]
        bq.create_table(t); print(f"  created {TABLES[name]}")
    print("setup done")


if __name__ == "__main__":
    if "--setup" in sys.argv: setup()
    elif "--equities-backfill" in sys.argv: equities_backfill()
    elif "--equities-deep-commit" in sys.argv: equities_deep_commit()
    elif "--equities-deep" in sys.argv: equities_deep()
    elif "--macro-backfill" in sys.argv: macro_backfill()
    elif "--fx-backfill" in sys.argv:
        from google.cloud import bigquery
        _now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        _rows = pull_fred(_now, since="2006-01-01", only=FX_SERIES)["macro"]
        print(f"fx backfill: {write(bigquery.Client(project=PROJECT), 'macro', _rows)} rows written")
    elif "--crypto-backfill" in sys.argv: crypto_daily(backfill=True)
    elif "--crypto-daily" in sys.argv:
        crypto_daily(backfill=False)
        run(names=DAILY)   # funding rates ride the daily execution
    else: run(dry="--dry" in sys.argv)
