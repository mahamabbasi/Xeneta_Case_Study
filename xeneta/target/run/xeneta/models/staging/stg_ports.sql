
  
    
    
      
    

    create  table
      "dev"."main_staging"."stg_ports__dbt_tmp"
  
  (
    PID integer not null,
    Code char(5),
    Slug varchar,
    Name varchar,
    Country varchar,
    Country_Code char(2),
    
    primary key (PID)
    )
 ;
    insert into "dev"."main_staging"."stg_ports__dbt_tmp" 
  (
    
      
      PID ,
    
      
      Code ,
    
      
      Slug ,
    
      
      Name ,
    
      
      Country ,
    
      
      Country_Code 
    
  )
 (
      
    select PID, Code, Slug, Name, Country, Country_Code
    from (
        

with stg_ports as (

    select
        Cast(PID as integer ) as PID,
        Cast(Code as char(5) ) as Code,
        Cast(Slug as varchar ) as Slug,
        Cast(Name as varchar )as Name,
        Cast(Country as varchar ) as Country,
        Cast(Country_Code as char(2) ) as Country_Code,
        ROW_NUMBER() OVER (PARTITION BY PID ORDER BY PID) AS row_num
    from raw.ports
    
)

select
PID,
Code,
Slug,
Name,
Country,
Country_Code
from stg_ports
where row_num = 1
    ) as model_subq
    );
  
  