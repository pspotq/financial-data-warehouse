-- daily ETF flows standing in for each mapped Fidelity fund (rrp.fund_etf_map): etf = the proxy ETF when it has flow
-- data, else the secondary (VOX, VIS, VAW, NXTG, QQQ have no free shares-outstanding file, so FBMPX -> XLC,
-- FSDPX -> XLB, FSLEX -> QCLN when loaded; GDX, SMH and GLD come from VanEck / SPDR Gold). fit / corr_1y say how closely the fund tracks that ETF:
-- read 'weak' rows as context only. Flows are the ETF's, in US$, not the mutual fund's own.
WITH m AS (SELECT * FROM {{ ref('fund_etf_map') }}),
have AS (SELECT DISTINCT symbol FROM {{ source('markets', 'etf_shares') }}),
pick AS (SELECT m.*, CASE WHEN m.proxy_etf IN (SELECT symbol FROM have) THEN m.proxy_etf
                          WHEN m.secondary_etf IN (SELECT symbol FROM have) THEN m.secondary_etf END AS etf
         FROM m)
SELECT p.fund, p.fund_name, p.kind, p.etf, p.etf = p.proxy_etf AS is_proxy, p.fit, p.corr_1y, p.beta_1y,
       f.date, f.flow_usd, f.flow_pct, f.flow_5d, f.flow_20d, f.flow_20d_pct, f.total_net_assets AS etf_assets, f.gap_days
FROM pick p JOIN {{ ref('etf_share_flows') }} f ON f.symbol = p.etf
