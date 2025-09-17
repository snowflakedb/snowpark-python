from snowflake.snowpark import Session
import requests
CONNECTION_PARAMETERS={
    "account": "",
    "user": "",
    "password": "",
    "role": "",
    "database": "",
    "schema": "",
    "warehouse": "",
}

session = Session.builder.configs(CONNECTION_PARAMETERS).create()

res = session.connection.rest.request(
    "/observability/event-table/hostname",
    method="get",
    # timeout=5,
)


print(res)
session.close()