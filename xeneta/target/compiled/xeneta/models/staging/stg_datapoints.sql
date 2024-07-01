

with stg_datapoints as (

    select
        Cast(D_ID as integer ) as D_ID,
        Cast(Created as timestamp ) as Created,
        Cast(Origin_PID as integer ) as Origin_PID,
        Cast(Destination_PID as integer )as Destination_PID,
        Cast(Valid_From as date ) as Valid_From,
        Cast(Valid_To as date ) as Valid_To,
        Cast(Company_ID as integer ) as Company_ID,
        Cast(Supplier_ID as integer ) as Supplier_ID,
        Cast(Equipment_ID as integer ) as Equipment_ID,
        row_number() over (partition by d_id order by created )as row_num
    from raw.datapoints

    
)

select *
from stg_datapoints
where row_num = 1