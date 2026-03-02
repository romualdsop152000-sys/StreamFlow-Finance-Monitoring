with source as (

    select *
    from {{ source('datalake', 'btc_nasdaq') }}

),

final as (

    select
        cast(ts_minute_utc as timestamp) as ts_minute_utc,
        cast(dt as date) as dt,

        -- BTC
        -- cast(btc_open as double precision) as btc_open,
        cast(btc_high as double precision) as btc_high,
        cast(btc_low as double precision) as btc_low,
        cast(btc_close as double precision) as btc_close,
        cast(btc_volume as double precision) as btc_volume,
        cast(btc_change_pct as double precision) as btc_change_pct,
        cast(btc_close_lag_1 as double precision) as btc_close_lag_1,
        cast(btc_close_lead_1 as double precision) as btc_close_lead_1,
        cast(btc_return_1m as double precision) as btc_return_1m,

        -- NDAQ
        cast(ndaq_high as double precision) as ndaq_high,
        cast(ndaq_low as double precision) as ndaq_low,
        cast(ndaq_close as double precision) as ndaq_close,
        cast(ndaq_volume as double precision) as ndaq_volume,
        cast(ndaq_close_lag_1 as double precision) as ndaq_close_lag_1,
        cast(ndaq_close_lead_1 as double precision) as ndaq_close_lead_1,
        cast(ndaq_return_1m as double precision) as ndaq_return_1m

    from source
)

select *
from final