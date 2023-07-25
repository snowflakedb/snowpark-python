from os import environ
from snowflake.snowpark.session import Session
from snowflake.snowpark.mock.connection import MockServerConnection
from project.local import get_env_var_config

def before_all(context):
    if environ.get('SNOWPARK_LOCAL') == '1': 
        print('Using local Snowpark session')
        context.session = Session(MockServerConnection())
    else:
        print('Using live connection to Snowflake')
        context.session = Session.builder.configs(get_env_var_config()).create()
