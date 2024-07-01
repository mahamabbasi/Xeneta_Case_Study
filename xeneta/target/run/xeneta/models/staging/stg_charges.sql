
  
    
    
      
    

    create  table
      "dev"."main_staging"."stg_charges"
  
  (
    D_ID integer not null,
    Currency char(3),
    Charge_Value integer,
    row_num BIGINT
    
    )
 ;
    insert into "dev"."main_staging"."stg_charges" 
  (
    
      
      D_ID ,
    
      
      Currency ,
    
      
      Charge_Value ,
    
      
      row_num 
    
  )
 (
      
    select D_ID, Currency, Charge_Value, row_num
    from (
        

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
    ) as model_subq
    );
  
  
  