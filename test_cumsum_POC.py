# Setup
from tests.parameters import CONNECTION_PARAMETERS
from snowflake.snowpark import Session
import modin.pandas as pd
from modin.config import context as config_context

from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    HYBRID_SWITCH_FOR_UNSUPPORTED_PARAMS,
)

session = Session.builder.configs(CONNECTION_PARAMETERS).create()
pd.session = session

# Verify rule registration
cumsum_rule = HYBRID_SWITCH_FOR_UNSUPPORTED_PARAMS.get(("BasePandasDataset", "cumsum"))
# print(cumsum_rule)

# Test autoswitch = False, should raise NotImplementedError
df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]})
snow_df = df.move_to("Snowflake")
with config_context(AutoSwitchBackend=False):
    try:
        snow_df.cumsum(axis=1)
        raise AssertionError("Should have raised NotImplementedError")
    except NotImplementedError as e:
        # print(e)
        assert "requires pandas backend" in str(
            e
        ), "Should mention 'requires pandas backend'"

# Test autoswitch = True, should switch to Pandas
with config_context(AutoSwitchBackend=True):
    result = snow_df.cumsum(axis=1)
    # print(result.get_backend())
    # print(result)
