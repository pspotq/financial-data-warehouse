-- US electricity per day and balancing authority / region (EIA Form 930), Jan 2019 on, MWh, Eastern-time days.
-- demand, day-ahead forecast, net generation and total interchange (+ = net exports). is_region = US48 (lower 48 total)
-- or one of the 13 EIA regions (CAL, CAR, CENT, FLA, MIDA, MIDW, NE, NY, NW, SE, SW, TEN, TEX); else a single
-- balancing authority (PJM, ERCO, MISO, CISO, ...). demand_yoy_pct = vs the same weekday 52 weeks earlier. The latest day
-- can be revised by EIA for several days.
WITH d AS (
  SELECT period_date AS date, respondent, ANY_VALUE(respondent_name) AS respondent_name,
         MAX(IF(type = 'D', value, NULL)) AS demand_mwh, MAX(IF(type = 'DF', value, NULL)) AS forecast_mwh,
         MAX(IF(type = 'NG', value, NULL)) AS net_generation_mwh, MAX(IF(type = 'TI', value, NULL)) AS interchange_mwh
  FROM {{ source('gov', 'eia_grid') }} WHERE dataset = 'demand' AND frequency = 'daily' GROUP BY 1, 2)
SELECT d.*, d.respondent IN ('US48', 'CAL', 'CAR', 'CENT', 'FLA', 'MIDA', 'MIDW', 'NE', 'NY', 'NW', 'SE', 'SW', 'TEN', 'TEX') AS is_region,
       ROUND(100 * SAFE_DIVIDE(d.demand_mwh - d.forecast_mwh, d.forecast_mwh), 2) AS forecast_miss_pct,
       ROUND(100 * SAFE_DIVIDE(d.demand_mwh - y.demand_mwh, y.demand_mwh), 2) AS demand_yoy_pct
FROM d LEFT JOIN d y ON y.respondent = d.respondent AND y.date = DATE_SUB(d.date, INTERVAL 364 DAY)
