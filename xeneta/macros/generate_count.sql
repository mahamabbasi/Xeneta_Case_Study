{% macro generate_count() -%}

{%- set target_relation = adapter.get_relation(
      database=this.database,
      schema=this.schema,
      identifier=this.name) -%}
{%- set table_exists=target_relation is not none -%}
{%- if table_exists -%}
  CREATE OR REPLACE table prior_count as
    ( SELECT day, Equipment_ID, Origin_PID,Destination_PID,dq_ok, count(*) as count 
        from {{this}}             
        WHERE dq_ok ='True'
        group by 1,2,3,4,5
        order by count desc )

{%- endif -%}
{%- endmacro %}