-- CFTC Commitments of Traders since 2006, one row per market per week, with net positions and week-over-week change.
-- is_major (2026-10-26): the ~41 benchmark markets readers know (Treasuries, SOFR / fed funds, S&P / Nasdaq / Russell,
-- VIX, major currencies, CME bitcoin / ether, WTI / Brent / gas / RBOB / ULSD, gold / silver / copper / platinum /
-- palladium, grains, softs, livestock). Every market is loaded; the rest (basis swaps, regional power, RECs) are niche.
SELECT report, report_date, market_code, market_name, open_interest_all AS open_interest,
       REGEXP_CONTAINS(market_name, r'^(E-MINI S&P 500 -|NASDAQ MINI -|RUSSELL E-MINI -|UST (2Y|5Y|10Y) NOTE -|UST BOND -|ULTRA UST (10Y|BOND) -|EURO FX -|JAPANESE YEN -|BRITISH POUND -|CANADIAN DOLLAR -|AUSTRALIAN DOLLAR -|MEXICAN PESO -|SWISS FRANC -|VIX FUTURES -|BITCOIN - CHICAGO|ETHER CASH SETTLED - CHICAGO|SOFR-3M - CHICAGO|FED FUNDS -|CRUDE OIL, LIGHT SWEET-WTI -|WTI-PHYSICAL -|NAT GAS NYME -|GOLD - COMMODITY|SILVER - COMMODITY|COPPER- #1 -|CORN - CHICAGO|SOYBEANS - CHICAGO|WHEAT-SRW -|GASOLINE RBOB -|NY HARBOR ULSD -|BRENT LAST DAY -|SUGAR NO. 11 -|COFFEE C -|COCOA - ICE|COTTON NO. 2 -|LIVE CATTLE -|LEAN HOGS -|PLATINUM -|PALLADIUM -)') AS is_major,
       lev_money_positions_long - lev_money_positions_short AS lev_funds_net,
       asset_mgr_positions_long - asset_mgr_positions_short AS asset_mgr_net,
       dealer_positions_long_all - dealer_positions_short_all AS dealer_net,
       m_money_positions_long_all - m_money_positions_short_all AS managed_money_net,
       lev_money_positions_long - lev_money_positions_short
         - LAG(lev_money_positions_long - lev_money_positions_short) OVER w AS lev_funds_net_chg,
       m_money_positions_long_all - m_money_positions_short_all
         - LAG(m_money_positions_long_all - m_money_positions_short_all) OVER w AS managed_money_net_chg,
       open_interest_all - LAG(open_interest_all) OVER w AS open_interest_chg,
       * EXCEPT(report, report_date, market_code, market_name, raw_json, ingested_at)
FROM {{ source('gov', 'cot') }} WINDOW w AS (PARTITION BY market_code ORDER BY report_date)
