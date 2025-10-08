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
