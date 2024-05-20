from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, contains

CONNECTION_PARAMETERS = {
    'host': "snowflake.dev.local",
    'protocol': "http",
    'port': 53200,
    'account': 'snowflake',
    'user': "admin",
    'password': "test",
    'schema': 'PUBLIC',
    'database': 'TESTDB',
    'warehouse': 'REGRESS',
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()
df = session.table('testtab')
df = df.filter("C2 < 40")
df.show()
