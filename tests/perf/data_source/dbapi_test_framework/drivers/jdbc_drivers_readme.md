# JDBC Drivers Directory

Place JDBC driver JAR files here for testing.

## Required Drivers

Download from official sources:

### MySQL
- **Download**: https://dev.mysql.com/downloads/connector/j/
- Select "Platform Independent" and download the ZIP/TAR archive

### PostgreSQL
- **Download**: https://jdbc.postgresql.org/download/

### SQL Server
- **Download**: https://learn.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server

### Oracle
- **Download**: https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html

### Databricks
- **Download**: https://www.databricks.com/spark/jdbc-drivers-download

## Snowflake Components (Required for PySpark Method)

For the `pyspark` ingestion method, you need **both** of these JARs:

### 1. Snowflake JDBC Driver
- **snowflake-jdbc-3.19.0.jar** or later
  - Download: https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc/3.19.0
  - Required for Snowflake connectivity

### 2. Snowflake Spark Connector
- **spark-snowflake_2.13-3.1.0.jar** (Scala 2.13, recommended)
  - Download: https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.13/3.1.0
  
- **OR spark-snowflake_2.12-3.1.0.jar** (Scala 2.12, if using older Spark)
  - Download: https://mvnrepository.com/artifact/net.snowflake/spark-snowflake_2.12/3.1.0

**Important Notes**:
- Spark connector version 3.1.0 is recommended - version 3.1.1 has known issues with Oracle BLOB types
- Both JARs must be in the `drivers/` directory for PySpark to work

## Automatic Upload

These drivers will be **automatically uploaded** to your Snowflake stage when you run JDBC tests. 

- Upload happens only once per session
- Subsequent runs skip the upload if the driver already exists on the stage
- Each test only uploads the specific driver it needs

## Configuration

If you use different driver versions, update the filenames in:
```
tests/perf/data_source/dbapi_test_framework/config.py
```

Look for the `JDBC_DRIVER_JARS` dictionary.

## Known Issues

### Oracle

1. **BLOB Type with Spark-Snowflake 3.1.1+**
   - `spark-snowflake_2.12-3.1.1` and later versions don't work correctly with Oracle BLOB type data
   - The connector expects binary data but receives different format
   - **Solution**: Use `spark-snowflake_2.12-3.1.0` or `spark-snowflake_2.13-3.1.0`

2. **TIMESTAMP WITH TIME ZONE**
   - PySpark cannot handle Oracle's `TIMESTAMP WITH TIME ZONE` data type
   - **Workaround**: Convert to string in query: `TO_CHAR(TIMESTAMP_TZ_COL)` or use `TIMESTAMP WITH LOCAL TIME ZONE`

### SQL Server

1. **SQL_VARIANT Type**
   - `SQL_VARIANT` type is not supported by PySpark when loading via JDBC
   - **Workaround**: Cast to specific type in query or exclude from selection

2. **DATE Type Warning**
   - DATE type generates warning: `LogicalTypes: Ignoring invalid logical type for name: date`
   - Data loads correctly despite the warning
   - Can be safely ignored or suppress with log level configuration

### General PySpark/JDBC

1. **Java Version Compatibility**
   - PySpark requires Java 8, 11, or 17
   - Java 25+ has breaking changes that cause `getSubject is not supported` errors
   - **Solution**: Install Java 17 (LTS): `brew install openjdk@17`

2. **Query Subquery Syntax**
   - Oracle JDBC doesn't support alias syntax in `dbtable` option
   - Use `(SELECT ...) ` without `as tmp` for Oracle
   - Other databases work with `(SELECT ...) as tmp`
