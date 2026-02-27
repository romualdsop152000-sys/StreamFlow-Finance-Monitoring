-- Use the `ref` function to select from other models

select *
from "datalake"."lead_lag_analysis"."my_first_dbt_model"
where id = 1