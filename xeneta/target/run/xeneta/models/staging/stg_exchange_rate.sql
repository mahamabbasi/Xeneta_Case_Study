
  
    
    

    create  table
      "dev"."main_staging"."stg_exchange_rate__dbt_tmp"
  
    as (
      

with stg_exchange_rate as (

    select
        Cast(Day as DATE ) as Day,
        Cast(Currency as Char(3) ) as Currency,
        Cast(Rate as decimal(18,10) ) as Rate,
        -- ROW_NUMBER() OVER (PARTITION BY PID ORDER BY PID) AS row_num
    from raw.exchange_rate
    
)

select *
from stg_exchange_rate
    );
  
  