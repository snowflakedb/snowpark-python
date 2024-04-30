from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, contains

connection_parameters = {
  "host": "snowflake.dev.local",
  "port": 53100,
  "protocol": "http",
  "account": "SNOWFLAKE",
  "user": "admin",
  "password": "test",
  "role": "ACCOUNTADMIN",
  "warehouse": "TESTWH_SNOWPANDAS",
  "database": "TESTDB_SNOWPANDAS",
  "schema": "public"
}

session = Session.builder.configs(connection_parameters).create()
df = session.table('test_table')
df = df.filter("STR LIKE '%e%'")
df.show()
