

with stg_regions as (

    select
        
        Cast(Slug as varchar(80) ) as Slug,
        Cast(Name as varchar(80) ) as Name,
        Cast(Parent as varchar(80) ) as Parent,
        
    from raw.regions
    
)

select *
from stg_regions