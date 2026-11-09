"""market main: order-book snapshot rows (b242) and the top-N universe filter."""
from conftest import load

PRODUCTS = {"products": [
    {"product_id": "BTC-USD", "quote_currency_id": "USD", "base_currency_id": "BTC", "volume_24h": "1000", "price": "100000", "status": "online"},
    {"product_id": "USDT-USD", "quote_currency_id": "USD", "base_currency_id": "USDT", "volume_24h": "1e9", "price": "1"},
    {"product_id": "ETH-EUR", "quote_currency_id": "EUR", "base_currency_id": "ETH", "volume_24h": "1", "price": "1"},
    {"product_id": "XYZ-USD", "quote_currency_id": "USD", "base_currency_id": "XYZ", "volume_24h": "5", "price": "1", "trading_disabled": True},
    {"product_id": "ETH-USD", "quote_currency_id": "USD", "base_currency_id": "ETH", "volume_24h": "10000", "price": "4000", "status": "online"}]}
BOOK = {"pricebook": {"bids": [{"price": "99", "size": "2"}, {"price": "98", "size": "1"}],
                      "asks": [{"price": "101", "size": "1"}], "time": "2026-10-27T16:05:01.123456789Z"}}


class Resp:
    status_code = 200
    def json(self): return BOOK


def test_orderbook_rows(monkeypatch):
    m = load("cloud/market/main.py", "market_main")
    monkeypatch.setattr(m, "get", lambda url, **kw: PRODUCTS)
    monkeypatch.setattr(m.S, "get", lambda url, **kw: Resp())
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    rows = m.pull_crypto_orderbook("2026-10-27 16:05:00")["crypto_orderbook"]
    assert [r["pair"] for r in rows] == ["BTC-USD", "ETH-USD"]   # stablecoins, non-USD and disabled products dropped; ranked by $ volume
    r = rows[0]
    assert r["vol_rank"] == 1 and r["volume_24h_usd"] == 100_000_000
    assert (r["best_bid"], r["best_ask"], r["mid"], r["spread"], r["spread_bps"]) == (99, 101, 100, 2, 200)
    assert (r["depth_bid"], r["depth_ask"], r["depth_bid_usd"], r["depth_ask_usd"]) == (3, 1, 296, 101)
    assert r["ob_imbalance"] == 0.5 and r["ts"] == "2026-10-27 16:05:01"
