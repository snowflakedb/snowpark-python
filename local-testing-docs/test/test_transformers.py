import pytest
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType
from snowflake.snowpark.mock.connection import MockServerConnection

from project.transformers import add_rider_age
from project.local import get_env_var_config


@pytest.fixture
def session(request) -> Session:
    if request.config.getoption('--snowflake-session') == 'local':  # parse the option from conftest.py
        return Session(MockServerConnection())
    else:
        return Session.builder.configs(get_env_var_config()).create()

def test_add_distance(session: Session):
    input = session.create_dataframe(
        [
            [1980], 
            [1995], 
            [2000]
        ], 
        schema=StructType([StructField("BIRTH_YEAR", IntegerType())])
    )

    expected = session.create_dataframe(
        [
            [1980, 43], 
            [1995, 28], 
            [2000, 23]
        ],
        schema=StructType([StructField("BIRTH_YEAR", IntegerType()), StructField("RIDER_AGE", IntegerType())])
    )
    
    actual = add_rider_age(input)
    assert expected.collect() == actual.collect()