-- daily creations / redemptions for every ETF in markets.etf_shares (SPDR, GLD and VanEck GDX / GDXJ / SMH history from
-- 2005-2006 or inception, every iShares ETF from 2026-10-23): flow_usd = change in shares outstanding x that day's NAV (+ = money in, - = out).
-- prev_date = the fund's previous loaded day; gap_days > 4 means a hole (a missed file), so treat that flow as a
-- multi-day total. flow_pct = flow / the previous day's total net assets x 100. flow_5d / flow_20d = sums over the
-- last 5 / 20 loaded days. split_factor != 1 = a share split that day (NAV moved by a clean multiple overnight, e.g.
-- XES 1-for-10 on 2020-03-30): the previous share count is scaled by it, so a split is not read as a flow. Issuers
-- report shares outstanding a day after the creations settle, so a flow can land one trading day after the trade
-- (KRE's +8.1M shares show on 2026-10-16). Early days of a new fund (50k-share base) give flow_pct in the 100s - real.
-- Mutual funds have no daily flows: rrp.fund_flows maps them onto these ETFs. (The crypto spot-ETF flows from Farside
-- are rrp.etf_flows_daily, a different source.)
WITH s AS (SELECT * FROM {{ source('markets', 'etf_shares') }} WHERE nav > 0 AND shares_out > 0
           QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY ingested_at DESC) = 1),
c AS (SELECT *, LAG(date) OVER w AS prev_date, LAG(shares_out) OVER w AS prev_shares, SAFE_DIVIDE(LAG(nav) OVER w, nav) AS r,
             LAG(total_net_assets) OVER w AS prev_assets
      FROM s WINDOW w AS (PARTITION BY symbol ORDER BY date)),
k AS (SELECT *, CASE WHEN r >= 1.8 THEN ROUND(r) WHEN r <= 0.55 THEN 1 / ROUND(1 / r) ELSE 1 END AS split_factor FROM c),
v AS (SELECT *, shares_out - prev_shares * split_factor AS shares_change,
             ROUND((shares_out - prev_shares * split_factor) * nav) AS flow_usd FROM k WHERE prev_date IS NOT NULL)
SELECT date, symbol, issuer, fund_name, nav, shares_out, total_net_assets, prev_date, DATE_DIFF(date, prev_date, DAY) AS gap_days,
       split_factor, shares_change, flow_usd, ROUND(100 * SAFE_DIVIDE(flow_usd, prev_assets), 3) AS flow_pct,
       SUM(flow_usd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS flow_5d,
       SUM(flow_usd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS flow_20d,
       ROUND(100 * SAFE_DIVIDE(SUM(flow_usd) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                               LAG(total_net_assets, 20) OVER (PARTITION BY symbol ORDER BY date)), 3) AS flow_20d_pct
FROM v
