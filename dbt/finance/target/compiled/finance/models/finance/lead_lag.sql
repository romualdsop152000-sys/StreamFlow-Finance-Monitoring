with lead_lag as (
  COPY *
  FROM '../../../../data/usage/finance/lead_lag_analysis/dt=2026-02-21/data.parquet'
)

SELECT * 
FROM lead_lag