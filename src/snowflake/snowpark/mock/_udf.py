#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from types import ModuleType
from typing import Callable, Dict, List, Optional, Tuple, Union

from snowflake.snowpark._internal.udf_utils import (
    check_python_runtime_version,
    process_registration_inputs,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.types import DataType
from snowflake.snowpark.udf import UDFRegistration, UserDefinedFunction


class MockUDFRegistration(UDFRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[str, Callable] = dict()

    def register_from_file(self, *_, **__) -> UserDefinedFunction:
        raise NotImplementedError(
            "[Local Testing] Registering UDF from file is not currently supported."
        )

    def _do_register_udf(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: Optional[DataType],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        max_batch_size: Optional[int] = None,
        from_pandas_udf_function: bool = False,
        strict: bool = False,
        secure: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        immutable: bool = False,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
    ) -> UserDefinedFunction:
        if is_permanent:
            raise NotImplementedError(
                "[Local Testing] Registering permanent UDF is not currently supported."
            )

        # get the udf name, return and input types
        (
            udf_name,
            is_pandas_udf,
            is_dataframe_input,
            return_type,
            input_types,
        ) = process_registration_inputs(
            self._session, TempObjectType.FUNCTION, func, return_type, input_types, name
        )

        # allow registering pandas UDF from udf(),
        # but not allow registering non-pandas UDF from pandas_udf()
        if from_pandas_udf_function and not is_pandas_udf:
            raise ValueError(
                "You cannot create a non-vectorized UDF using pandas_udf(). "
                "Use udf() instead."
            )

        if packages or imports:
            raise NotImplementedError(
                "[Local Testing] Uploading imports and packages not currently supported."
            )

        custom_python_runtime_version_allowed = False

        if not custom_python_runtime_version_allowed:
            check_python_runtime_version(
                self._session._runtime_version_from_requirement
            )

        if udf_name in self._registry:
            raise SnowparkSQLException(
                f"002002 (42710): SQL compilation error: \nObject '{udf_name}' already exists.",
                error_code="1304",
            )

        self._registry[udf_name] = func

        return UserDefinedFunction(func, return_type, input_types, udf_name)
