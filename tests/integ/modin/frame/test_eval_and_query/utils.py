#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from pytest import param

ENGINE_IGNORED_MESSAGE = (
    "The argument `engine` of `eval` has been ignored by Snowpark pandas "
    + "API:\nSnowpark pandas always uses the python engine in favor of "
    + "the numexpr engine, even if the numexpr engine is available."
)


engine_parameters = pytest.mark.parametrize(
    "engine_kwargs",
    [
        param({"engine": "python"}, id="engine_python"),
        param({"engine": "numexpr"}, id="engine_numexpr"),
        param({}, id="default_engine"),
    ],
)
