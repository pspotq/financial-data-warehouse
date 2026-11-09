-- Fidelity fund -> the ETF whose daily moves it tracks most closely (the proxy_etf), so ETF data (flows, short interest,
-- volume) can stand in for a mutual fund that publishes none. Pairs set by hand on 2026-10-26 and checked against every
-- candidate ETF: the proxy is the ETF with the highest daily-return correlation over the fund's last 252 trading days.
-- corr_1y / beta_1y are recomputed on every read over the trailing 252 days (beta = the fund's move per 1% ETF move;
-- FSAGX ~1x GDX, ~1.4x GLD). fit: strong >= 0.90, moderate 0.80-0.90, weak < 0.80 (proxy flows say little about the
-- fund). secondary_etf = the next-best or larger-flow alternative. Not mapped: FERLX (cash-like) and the non-Fidelity funds.
WITH m AS (SELECT * FROM UNNEST([
    STRUCT('FSUTX' AS fund, 'Select Utilities' AS fund_name, 'sector' AS kind, 'XLU' AS proxy_etf, CAST(NULL AS STRING) AS secondary_etf),
    ('FSENX', 'Select Energy', 'sector', 'XLE', 'XOP'),
    ('FNARX', 'Select Natural Resources', 'sector', 'IGE', 'GUNR'),
    ('FDFAX', 'Select Consumer Staples', 'sector', 'XLP', NULL),
    ('FSCPX', 'Select Consumer Discretionary', 'sector', 'XLY', 'IYC'),
    ('FSRPX', 'Select Retailing', 'sector', 'IYC', 'XRT'),
    ('FSAVX', 'Select Automotive', 'sector', 'XLY', 'IDRV'),
    ('FSHOX', 'Select Construction & Housing', 'sector', 'XHB', 'ITB'),
    ('FSPHX', 'Select Health Care', 'sector', 'IBB', 'XLV'),
    ('FBIOX', 'Select Biotechnology', 'sector', 'XBI', 'IBB'),
    ('FPHAX', 'Select Pharmaceuticals', 'sector', 'IHE', 'XLV'),
    ('FSHCX', 'Select Health Care Services', 'sector', 'IHF', NULL),
    ('FSMEX', 'Select Medical Technology & Devices', 'sector', 'IHI', 'XLV'),
    ('FIDSX', 'Select Financial Services', 'sector', 'XLF', NULL),
    ('FSRBX', 'Select Banking', 'sector', 'KBE', 'KRE'),
    ('FSLBX', 'Select Brokerage & Investment Mgmt', 'sector', 'KCE', 'IAI'),
    ('FSPCX', 'Select Insurance', 'sector', 'KIE', NULL),
    ('FSPTX', 'Select Technology', 'sector', 'XLK', 'QQQ'),
    ('FSELX', 'Select Semiconductors', 'sector', 'SMH', 'SOXX'),
    ('FSCSX', 'Select Software & IT Services', 'sector', 'IGV', NULL),
    ('FBMPX', 'Select Communication Services', 'sector', 'VOX', 'XLC'),
    ('FSTCX', 'Select Telecommunications', 'sector', 'IYZ', 'XTL'),
    ('FWRLX', 'Select Wireless', 'sector', 'NXTG', 'XTL'),
    ('FSDAX', 'Select Defense & Aerospace', 'sector', 'ITA', 'PPA'),
    ('FSRFX', 'Select Transportation', 'sector', 'IYT', 'XTN'),
    ('FSLEX', 'Select Environment & Alternative Energy', 'sector', 'VIS', 'QCLN'),
    ('FSDPX', 'Select Materials', 'sector', 'VAW', 'XLB'),
    ('FSCHX', 'Select Chemicals', 'sector', 'XLB', 'VAW'),
    ('FSAGX', 'Select Gold', 'sector', 'GDX', 'GLD'),
    ('FXAIX', '500 Index', 'broad', 'SPY', NULL),
    ('FBGRX', 'Blue Chip Growth', 'broad', 'IWF', 'QQQ'),
    ('FSMDX', 'Mid Cap Index', 'broad', 'IJH', NULL),
    ('FSSNX', 'Small Cap Index', 'broad', 'IWM', NULL),
    ('FSPSX', 'International Index', 'broad', 'EFA', NULL),
    ('FPADX', 'Emerging Markets Index', 'broad', 'IEMG', 'EEM'),
    ('FXNAX', 'U.S. Bond Index', 'bond', 'AGG', NULL),
    ('FIPDX', 'Inflation-Protected Bond Index', 'bond', 'TIP', NULL),
    ('FUMBX', 'Short-Term Treasury Bond Index', 'bond', 'SHY', 'VGSH')])),
px AS (SELECT symbol, date, SAFE_DIVIDE(adj_close, LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date)) - 1 AS r
       FROM {{ source('markets', 'equity_daily') }}
       WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 400 DAY)
         AND symbol IN (SELECT fund FROM m UNION ALL SELECT proxy_etf FROM m UNION ALL SELECT secondary_etf FROM m WHERE secondary_etf IS NOT NULL)),
pr AS (SELECT m.fund, m.proxy_etf, f.date, f.r AS rf, x.r AS rx FROM m
       JOIN px f ON f.symbol = m.fund JOIN px x ON x.symbol = m.proxy_etf AND x.date = f.date
       WHERE f.r IS NOT NULL AND x.r IS NOT NULL
       QUALIFY ROW_NUMBER() OVER (PARTITION BY m.fund ORDER BY f.date DESC) <= 252),
st AS (SELECT fund, ROUND(CORR(rf, rx), 3) AS corr_1y, ROUND(COVAR_SAMP(rf, rx) / VAR_SAMP(rx), 2) AS beta_1y, COUNT(*) AS days,
              MAX(date) AS through FROM pr GROUP BY fund)
SELECT m.fund, m.fund_name, m.kind, m.proxy_etf, m.secondary_etf, st.corr_1y, st.beta_1y,
       CASE WHEN st.corr_1y >= 0.9 THEN 'strong' WHEN st.corr_1y >= 0.8 THEN 'moderate' ELSE 'weak' END AS fit,
       st.days, st.through
FROM m LEFT JOIN st USING (fund)
