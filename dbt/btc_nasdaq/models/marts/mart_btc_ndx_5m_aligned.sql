select *
from {{ ref('stg_btc_ndx_features_5m_dbt') }}
where ndaq_close is not null