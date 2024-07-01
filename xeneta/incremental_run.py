import duckdb
from dbt.cli.main import dbtRunner, dbtRunnerResult

con = duckdb.connect(database='./dev.duckdb', read_only=False)

con.sql("Drop table if exists raw.datapoints")
con.sql("CREATE TABLE raw.datapoints (D_ID VARCHAR(256), Created VARCHAR(256), Origin_PID VARCHAR(256), Destination_PID VARCHAR(256), Valid_From VARCHAR(256),Valid_To VARCHAR(256),Company_ID VARCHAR(256), Supplier_ID VARCHAR(256),Equipment_ID VARCHAR(256)) ");
con.sql("COPY raw.datapoints FROM './input_files/DE_casestudy_datapoints_2.csv'(DELIMITER ',',HEADER)");

con.sql("Drop table if exists raw.charges")
con.sql("CREATE TABLE raw.charges (D_ID VARCHAR(256), Currency VARCHAR(256), Charge_Value VARCHAR(256)) ");
con.sql("COPY raw.charges FROM './input_files/DE_casestudy_charges_2.csv'(DELIMITER ',',HEADER)");

con.close()

dbt = dbtRunner()

cli_args4 = ["run", "--select", "stg_datapoints"]
cli_args5 = ["run", "--select", "stg_charges"]
res: dbtRunnerResult = dbt.invoke(cli_args4)
res: dbtRunnerResult = dbt.invoke(cli_args5)

# run aggreagtions model again on the incremental data
cli_args6 = ["run", "--select", "aggregations"]
res: dbtRunnerResult = dbt.invoke(cli_args6)