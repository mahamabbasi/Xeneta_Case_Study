{{ config(
    materialized='table')}}

with datapoints as (
    select * from {{ ref('stg_datapoints') }}
),
charges as (
    select * from {{ ref('stg_charges') }}
),
exchange_rates as (
    select * from {{ ref('stg_exchange_rate') }}
),

datapoint_charges as (
    select 
        dp.d_id,
        dp.valid_from,
        dp.valid_to,
        dp.equipment_id,
        dp.Origin_PID,
        dp.Destination_PID,
        dp.company_id,
        dp.supplier_id,
        c.currency,
        c.Charge_Value,
        er.rate,
        (c.Charge_Value / er.rate) as usd_value
    from datapoints dp
    join charges c on dp.d_id = c.d_id
    join exchange_rates er on er.currency = c.currency
    where er.day = dp.valid_from
),

daily_prices as (
    select 
        dpc.valid_from + (cast(seq.seqnum as integer) - 1) as day,
        dpc.equipment_id,
        dpc.Origin_PID,
        dpc.Destination_PID,
        sum(dpc.usd_value) as daily_price,
        dpc.company_id,
        dpc.supplier_id
    from datapoint_charges dpc
    join (select row_number() over() as seqnum from range(10000)) seq
    on seq.seqnum <= (dpc.valid_to - dpc.valid_from)
    group by 1, 2, 3, 4, 6, 7
),

lane_prices as (
    select 
        day,
        equipment_id,
        Origin_PID,
        Destination_PID,
        avg(daily_price) as avg_daily_price,
        median(daily_price) as median_daily_price,
        count(distinct company_id) as company_count,
        count(distinct supplier_id) as supplier_count,
        case 
            when count(distinct company_id) >= 5 and count(distinct supplier_id) >= 2 then 'True'
            else 'False'
        end as dq_ok
    from daily_prices
    group by 1, 2, 3, 4
)

select *
from lane_prices