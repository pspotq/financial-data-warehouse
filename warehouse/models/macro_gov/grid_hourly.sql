-- hourly US electricity for US48 and the 13 EIA regions, last ~3 days only (rolling; hourly history is not kept).
-- hour_utc = the hour's start in UTC. kind: demand (D demand, DF forecast, NG net generation, TI interchange) or fuel
-- (type = fuel code: NG gas, COL coal, NUC nuclear, SUN solar, WND wind, WAT hydro, BAT battery, OIL, OTH ...). MWh.
SELECT period_ts AS hour_utc, respondent, respondent_name, dataset AS kind, type, type_name, value AS mwh
FROM {{ source('gov', 'eia_grid') }} WHERE frequency = 'hourly'
