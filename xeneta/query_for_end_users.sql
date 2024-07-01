-- Provide a few example queries to the data users. 
-- For example, how can they get the average container shipping price of equipment type _2_ from China East Main region to US West Coast region?

Select avg_daily_price, median_daily_price
from main_final.aggregations
where equipment_id = 2
and Origin_PID in (select PID from main_staging.stg_ports where slug = 'china_east_main')
and Destination_PID in (select PID from main_staging.stg_ports where slug = 'us_west_coast')