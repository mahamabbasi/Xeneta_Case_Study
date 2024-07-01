import duckdb
from dbt.cli.main import dbtRunner, dbtRunnerResult

con = duckdb.connect(database='./dev.duckdb', read_only=False)

# create 'raw' schema
con.sql("CREATE SCHEMA IF NOT EXISTS raw;")

# create tables in raw schema
con.sql("Drop table if exists raw.ports")
con.sql("CREATE TABLE raw.ports (PID VARCHAR(256), Code VARCHAR(256), Slug VARCHAR(256), Name VARCHAR(256), Country VARCHAR(256),Country_code VARCHAR(256) ) ");
con.sql("COPY raw.ports FROM './input_files/DE_casestudy_ports.csv'(DELIMITER ',',HEADER)");

con.sql("Drop table if exists raw.regions")
con.sql("CREATE TABLE raw.regions (Slug VARCHAR(256), Name VARCHAR(256), Parent VARCHAR(256) )");
con.sql("COPY raw.regions FROM './input_files/DE_casestudy_regions.csv'(DELIMITER ',',HEADER)");

con.sql("Drop table if exists raw.exchange_rate")
con.sql("CREATE TABLE raw.exchange_rate (Day VARCHAR(256), Currency VARCHAR(256), Rate VARCHAR(256) )");
con.sql("COPY raw.exchange_rate FROM './input_files/DE_casestudy_exchange_rates.csv'(DELIMITER ',',HEADER)");

con.sql("Drop table if exists raw.datapoints")
con.sql("CREATE TABLE raw.datapoints (D_ID VARCHAR(256), Created VARCHAR(256), Origin_PID VARCHAR(256), Destination_PID VARCHAR(256), Valid_From VARCHAR(256),Valid_To VARCHAR(256),Company_ID VARCHAR(256), Supplier_ID VARCHAR(256),Equipment_ID VARCHAR(256)) ");
con.sql("COPY raw.datapoints FROM './input_files/DE_casestudy_datapoints_1.csv'(DELIMITER ',',HEADER)");

con.sql("Drop table if exists raw.charges")
con.sql("CREATE TABLE raw.charges (D_ID VARCHAR(256), Currency VARCHAR(256), Charge_Value VARCHAR(256)) ");
con.sql("COPY raw.charges FROM './input_files/DE_casestudy_charges_1.csv'(DELIMITER ',',HEADER)");

con.sql("Drop table if exists main_staging.stg_datapoints")
con.sql("Drop table if exists main_staging.stg_charges")

con.close()
# loading data from raw to staging using dbt models
dbt = dbtRunner()

cli_args1 = ["run", "--select", "stg_ports"]
cli_args2 = ["run", "--select", "stg_regions"]
cli_args3 = ["run", "--select", "stg_exchange_rate"]
cli_args4 = ["run", "--select", "stg_datapoints"]
cli_args5 = ["run", "--select", "stg_charges"]

res: dbtRunnerResult = dbt.invoke(cli_args1)
res: dbtRunnerResult = dbt.invoke(cli_args2)
res: dbtRunnerResult = dbt.invoke(cli_args3)
res: dbtRunnerResult = dbt.invoke(cli_args4)
res: dbtRunnerResult = dbt.invoke(cli_args5)

# running the aggregations model to create report for the end user
cli_args6 = ["run", "--select", "aggregations"]
res: dbtRunnerResult = dbt.invoke(cli_args6)


