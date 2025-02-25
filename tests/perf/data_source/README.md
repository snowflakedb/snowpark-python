## snowpark python setup
```bash
git clone https://github.com/snowflakedb/snowpark-python.git
cd snowpark-python
pip install ".[pandas]"
```

# oracledb setup instruction for test

## local oracle db setup
```bash
docker pull container-registry.oracle.com/database/free:latest
docker run -d --name oracledb -e ORACLE_PWD=test -p 1521:1521 -p 5500:5500 container-registry.oracle.com/database/free:latest
```
## python env setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install oracledb
python tests/perf/data_source/scripts/oracledb_resource_setup.py
```

# sqlserver setup instruction for test

## local sqlserver db setup
```bash
docker pull mcr.microsoft.com/mssql/server:2022-latest
docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=Test12345()" \
   -p 1433:1433 --name sqlserverdb \
   -d mcr.microsoft.com/mssql/server:2022-latest
```

## sqlserver odbc driver setup
https://learn.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver16

## python env setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install pyodbc
python tests/perf/data_source/scripts/sqlserver_resource_setup.py
```

# run test

**Steps**

1. create `tests/perf/data_source/scripts/parameters.py` and fill in the snowflake credential information

2. in `tests/perf/data_source/scripts/e2etest.py`, construct the test configuration and test case

3. run the script
```bash
python tests/perf/data_source/e2etest.py
```

4. the reports will be generated in the `tests/perf/data_source/reports/performance_results.csv`
