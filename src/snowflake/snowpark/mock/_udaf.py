#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# TODO: Implement mock udaf for local testing

from typing import Dict

from snowflake.snowpark.udaf import UDAFRegistration, UserDefinedAggregateFunction


class MockUserDefinedAggregateFunction(UserDefinedAggregateFunction):
    def __init__(self, *args, strict=False, use_session_imports=True, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.strict = strict
        self._imports = set()
        self.use_session_imports = use_session_imports

    def add_import(self, absolute_module_path: str) -> None:
        self.use_session_imports = False
        self._imports.add(absolute_module_path)


class MockUDAFRegistration(UDAFRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[
            str, MockUserDefinedAggregateFunction
        ] = (
            dict()
        )  # maps udf name to either the callable or a pair of str (module_name, callable_name)
        self._session_level_imports = set()
