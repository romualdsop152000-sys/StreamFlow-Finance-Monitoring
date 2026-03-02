{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('mart_btc_ndx_5m_aligned') }}
),

enriched as (
    select
        *,
        -- BTC engineered features
        (btc_high - btc_low) as btc_price_range_calc,

        lag(btc_close, 1) over (order by ts_minute_utc) as btc_close_lag_1_calc,
        lead(btc_close, 1) over (order by ts_minute_utc) as btc_close_lead_1_calc,

        -- NDX engineered features
        (ndaq_high - ndaq_low) as ndaq_price_range_calc,

        lag(ndaq_close, 1) over (order by ts_minute_utc) as ndaq_close_lag_1_calc,
        lead(ndaq_close, 1) over (order by ts_minute_utc) as ndaq_close_lead_1_calc

    from base
)

select
    ts_minute_utc,
    dt,
    btc_high, btc_low, btc_close, btc_volume,
    ndaq_high, ndaq_low, ndaq_close, ndaq_volume,

    -- prefer computed values (dbt) over spark columns if spark ones are null
    coalesce(btc_close_lag_1, btc_close_lag_1_calc) as btc_close_lag_1,
    coalesce(btc_close_lead_1, btc_close_lead_1_calc) as btc_close_lead_1,

    coalesce(ndaq_close_lag_1, ndaq_close_lag_1_calc) as ndaq_close_lag_1,
    coalesce(ndaq_close_lead_1, ndaq_close_lead_1_calc) as ndaq_close_lead_1

from enriched
order by ts_minute_utc