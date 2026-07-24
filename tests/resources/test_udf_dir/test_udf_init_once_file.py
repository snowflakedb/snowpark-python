try:
    from _snowflake import udf_init_once
except ModuleNotFoundError:
    from snowflake.snowpark.functions import udf_init_once


_multiplier = 1


@udf_init_once
def setup():
    global _multiplier
    _multiplier = 10


def multiply(x):
    return x * _multiplier
