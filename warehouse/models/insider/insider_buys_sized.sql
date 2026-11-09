-- rrp.insider_buys with company size at the time of the buy: shares outstanding last reported before the buy, the
-- close on or before the buy date (the buy's own price before Sep 2021, where equity_daily starts), market cap, the
-- buy as a share of market cap and of shares outstanding, and the first Form 25 / 15 filed after the buy (delisted_on)
WITH b AS (SELECT *, FARM_FINGERPRINT(CONCAT(accession, '|', insider, '|', CAST(tx_date AS STRING), '|', CAST(shares AS STRING), '|',
                                         IFNULL(CAST(price AS STRING), ''))) AS bid FROM {{ ref('insider_buys') }}),
sh AS (SELECT b.bid, s.shares AS shares_out, s.period_end AS shares_as_of
       FROM b JOIN {{ ref('sec_shares_outstanding') }} s
         ON s.cik = b.issuer_cik AND s.known_from <= b.tx_date AND s.period_end >= DATE_SUB(b.tx_date, INTERVAL 400 DAY)
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY b.bid ORDER BY s.known_from DESC, s.period_end DESC) = 1),
px AS (SELECT b.bid, e.close AS close_at_buy, e.date AS close_date
       FROM b JOIN {{ source('markets', 'equity_daily') }} e ON e.symbol = b.ticker AND e.date BETWEEN DATE_SUB(b.tx_date, INTERVAL 7 DAY) AND b.tx_date
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY b.bid ORDER BY e.date DESC) = 1),
dl AS (SELECT b.bid, d.filed AS delisted_on, d.form AS delist_form
       FROM b JOIN {{ ref('sec_delistings') }} d ON d.cik = b.issuer_cik AND d.filed > b.tx_date
       WHERE TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY b.bid ORDER BY d.filed) = 1)
SELECT b.* EXCEPT(bid), sh.shares_out, sh.shares_as_of, px.close_at_buy, px.close_date,
  ROUND(sh.shares_out * COALESCE(px.close_at_buy, b.price) / 1e6, 2) AS mkt_cap_usd_m,
  ROUND(100 * b.value / NULLIF(sh.shares_out * COALESCE(px.close_at_buy, b.price), 0), 4) AS buy_pct_of_mkt_cap,
  ROUND(100 * b.shares / NULLIF(sh.shares_out, 0), 4) AS buy_pct_of_shares,
  dl.delisted_on, dl.delist_form
FROM b LEFT JOIN sh USING (bid) LEFT JOIN px USING (bid) LEFT JOIN dl USING (bid)
