## download the following JARs and put into the jdbc_drivers folder:

1. (required for mssql) mssql-jdbc-12.8.1.jre11.jar: https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc/12.8.1.jre11
2. (required for oracle) ojdbc11.jar: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
3. parquet-avro-1.10.1.jar: https://mvnrepository.com/artifact/org.apache.parquet/parquet-avro/1.10.1
4. snowflake-jdbc-3.19.0.jar: https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.19.0
5. spark-snowflake_2.12-3.1.0.jar: https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.13/3.1.0

## known issues:

### Oracle
1. spark-snowflake_2.12-3.1.1 doesn't work with oracledb blob type data, should be binary data but it want binary data
solution: spark-snowflake_2.12-3.1.0 works
2. spark can not handle oracle database timestamp with time zone data type

### MSSQL
1. SQL_VARIANT type is not supported by spark when loading
2. date type gets warning LogicalTypes: Ignoring invalid logical type for name: date
