{{ config(materialized='incremental',on_schema_change ='append_new_columns') }}

with stg_charges as (

    select
        Cast(D_ID as integer ) as D_ID,
        Cast(Currency as char(3) ) as Currency,
        Cast(Charge_Value as integer ) as Charge_Value,
        row_number() over (partition by d_id, currency order by Charge_Value )as row_num
    from raw.charges
    
    
)

select *
from stg_charges
where row_num = 1
  