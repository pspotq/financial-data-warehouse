-- coins that track a dollar or gold price: left out of every movers list and momentum read. kind = usd / eur / gold
SELECT symbol, kind FROM UNNEST([
  STRUCT('USDT' AS symbol, 'usd' AS kind), ('USDC', 'usd'), ('USDE', 'usd'), ('DAI', 'usd'), ('FDUSD', 'usd'), ('PYUSD', 'usd'),
  ('USD1', 'usd'), ('USDS', 'usd'), ('TUSD', 'usd'), ('USDP', 'usd'), ('GUSD', 'usd'), ('RLUSD', 'usd'), ('USDD', 'usd'),
  ('BUSD', 'usd'), ('FRAX', 'usd'), ('LUSD', 'usd'), ('USDG', 'usd'), ('USDTB', 'usd'), ('USD0', 'usd'), ('BUIDL', 'usd'),
  ('USYC', 'usd'), ('SUSDE', 'usd'), ('SUSDS', 'usd'), ('GHO', 'usd'), ('CRVUSD', 'usd'), ('USDF', 'usd'), ('USDX', 'usd'),
  ('EURC', 'eur'), ('EURS', 'eur'), ('PAXG', 'gold'), ('XAUT', 'gold')])
