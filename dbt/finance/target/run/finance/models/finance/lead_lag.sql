
  create view "datalake"."lead_lag_analysis"."lead_lag__dbt_tmp"
    
    
  as (
    with lead_lag as (
  COPY *
  FROM '../../../../data/usage/finance/lead_lag_analysis/dt=2026-02-21/data.parquet'
)

SELECT * 
FROM lead_lag
  );