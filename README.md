# Xeneta_Case_Study

**#Overall Solution**
The xeneta folder contains two .py files, first_run.py and incremental_run.py.

#Running the first_run.py file
Creates raw schema and the tables, loads data from .csv into the raw tables. Then loads data from raw tables to staging tables after initial data validations using dbt models that reside in the models/staging/ folder. Once data is loaded, then models/final/aggregations model is run that would create the report for the end user. I have executed all logic in the aggreagtions model.

#Running the incremental_run.py file
Would load datapoints and charges with new files and then re-run the aggregations model based on the new data loaded. Initially I used merge incremental startegy for both, datapoints and charges but duckdb adapter wasn't compatible hence had to go with append.

A query_for_end_users.sql is present in the folder that contains the query to help the end users in quering the aggregations model

**#Time Distribution**
I hadn't worked on dbt and also with duckdb before, hence it was trial and error with certain things but good thing that I learnt in the process specially w.r.t dbt.

Dbt and Duckdb installation and integration: 1 hour
Initial loading of data from .csv to raw tables : 30 minutes
Working on dbt models to load data and generate reports: 10 - 12 hours 


