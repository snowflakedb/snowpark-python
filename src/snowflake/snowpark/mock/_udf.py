#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Callable, Dict, Optional

from snowflake.snowpark._internal.udf_utils import (
    CallableProperties,
    check_python_runtime_version,
    process_registration_inputs,
)
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.udf import UDFRegistration, UserDefinedFunction


class MockUDFRegistration(UDFRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[str, Callable] = dict()

    def register_from_file(self, *_, **__) -> UserDefinedFunction:
        self._session._conn.log_not_supported_error(
            external_feature_name="udf",
            error_message="Registering UDF from file is not currently supported.",
            raise_error=NotImplementedError,
        )

    def _do_register_udf(
        self,
        callableProperties: CallableProperties,
        *,
        api_call_source: str,
        statement_params: Optional[Dict[str, str]] = None,
        from_pandas_udf_function: bool = False,
        skip_upload_on_content_match: bool = False,
    ) -> UserDefinedFunction:
        if callableProperties.is_permanent:
            self._session._conn.log_not_supported_error(
                external_feature_name="udf",
                error_message="Registering permanent UDF is not currently supported.",
                raise_error=NotImplementedError,
            )

        # get the udf name, return and input types
        (
            udf_name,
            is_pandas_udf,
            is_dataframe_input,
            return_type,
            input_types,
        ) = process_registration_inputs(
            session=self._session,
            object_type=callableProperties.object_type,
            func=callableProperties.func,
            return_type=callableProperties.raw_return_type,
            input_types=callableProperties.raw_input_types,
            name=callableProperties.raw_name,
        )

        callableProperties.set_validated_object_name(udf_name)
        callableProperties.set_validated_return_type(return_type)
        callableProperties.set_validated_input_types(input_types)

        # allow registering pandas UDF from udf(),
        # but not allow registering non-pandas UDF from pandas_udf()
        if from_pandas_udf_function and not is_pandas_udf:
            raise ValueError(
                "You cannot create a non-vectorized UDF using pandas_udf(). "
                "Use udf() instead."
            )

        if callableProperties.raw_packages or callableProperties.raw_imports:
            self._session._conn.log_not_supported_error(
                external_feature_name="udf",
                error_message="Uploading imports and packages not currently supported.",
                raise_error=NotImplementedError,
            )

        custom_python_runtime_version_allowed = False

        if not custom_python_runtime_version_allowed:
            check_python_runtime_version(
                self._session._runtime_version_from_requirement
            )

        if udf_name in self._registry and not callableProperties.replace:
            raise SnowparkSQLException(
                f"002002 (42710): SQL compilation error: \nObject '{udf_name}' already exists.",
                error_code="1304",
            )

        self._registry[udf_name] = callableProperties.func

        return UserDefinedFunction(
            func=callableProperties.func,
            name=callableProperties.validated_object_name,
            _return_type=callableProperties.validated_return_type,
            _input_types=callableProperties.validated_input_types,
        )
