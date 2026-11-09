-- US spot bitcoin, ether and solana ETF net flows from Farside, US$ millions (negative = outflow), one row per fund per
-- trading day; fund TOTAL is Farside's own daily total. The newest day can change as late funds report; refreshed hourly.
SELECT asset, date, fund, flow_usd_m, ingested_at FROM {{ source('markets', 'etf_flows') }} WHERE fund != 'TOTAL'
