-- FINRA short interest, twice a month (settlement dates mid-month and month end; published about 7 business days later).
-- Every exchange-listed and OTC stock from June 2021; OTC only before that (history from Dec 2017). short_qty = shares sold
-- short and not yet covered at settlement; change_qty / change_pct vs the prior settlement date as FINRA reports it;
-- days_to_cover = short_qty / avg_daily_vol (FINRA's figure). pct_of_shares_out = short_qty over the shares outstanding last
-- reported to the SEC before the settlement date (XBRL cover page; not float: float in shares is not reported).
-- cik comes from the SEC ticker map (latest company for the ticker; reused tickers can map to the newer company).
WITH tk AS (SELECT ticker, ARRAY_AGG(cik ORDER BY last_seen DESC LIMIT 1)[OFFSET(0)] AS cik FROM {{ ref('cik_tickers') }} GROUP BY ticker),
si AS (SELECT s.*, tk.cik FROM {{ source('markets', 'short_interest') }} s LEFT JOIN tk ON tk.ticker = REPLACE(s.symbol, '.', '-')
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY s.settlement_date, s.symbol ORDER BY s.ingested_at DESC) = 1),
sh AS (SELECT si.settlement_date, si.symbol, o.shares, o.period_end
       FROM si JOIN {{ ref('sec_shares_outstanding') }} o
         ON o.cik = si.cik AND o.known_from <= si.settlement_date AND o.period_end >= DATE_SUB(si.settlement_date, INTERVAL 400 DAY)
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY si.settlement_date, si.symbol ORDER BY o.known_from DESC, o.period_end DESC) = 1)
SELECT si.settlement_date, si.symbol, si.name, si.market, si.cik, si.short_qty, si.prev_short_qty,
       si.short_qty - si.prev_short_qty AS change_qty, si.change_pct, si.avg_daily_vol, si.days_to_cover,
       sh.shares AS shares_out, sh.period_end AS shares_as_of,
       ROUND(100 * si.short_qty / NULLIF(sh.shares, 0), 2) AS pct_of_shares_out,
       si.split_flag, si.revision
FROM si LEFT JOIN sh USING (settlement_date, symbol)
