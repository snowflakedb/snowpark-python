#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
import sys
import typing
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.udf_utils import (
    check_python_runtime_version,
    process_registration_inputs,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock import CUSTOM_JSON_ENCODER
from snowflake.snowpark.mock._plan import calculate_expression
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator
from snowflake.snowpark.stored_procedure import (
    StoredProcedure,
    StoredProcedureRegistration,
)
from snowflake.snowpark.types import (
    ArrayType,
    DataType,
    MapType,
    StructType,
    _FractionalType,
    _IntegralType,
)

from ._telemetry import LocalTestOOBTelemetryService

if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


def sproc_types_are_compatible(x, y):
    if (
        isinstance(x, type(y))
        or isinstance(x, _IntegralType)
        and isinstance(y, _IntegralType)
        or isinstance(x, _FractionalType)
        and isinstance(y, _FractionalType)
    ):
        return True
    return False


class MockStoredProcedure(StoredProcedure):
    def __call__(
        self,
        *args: Any,
        session: Optional["snowflake.snowpark.session.Session"] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Any:
        args, session = self._validate_call(args, session)

        # Unpack columns if passed
        parsed_args = []
        for arg, expected_type in zip(args, self._input_types):
            if isinstance(arg, Column):
                expr = arg._expression

                # If expression does not define its datatype we cannot verify it's compatibale.
                # This is potentially unsafe.
                if expr.datatype and not sproc_types_are_compatible(
                    expr.datatype, expected_type
                ):
                    raise ValueError(
                        f"Unexpected type {expr.datatype} for sproc argument of type {expected_type}"
                    )

                # Expression may be a nested expression. Expression should not need any input data
                # and should only return one value so that it can be passed as a literal value.
                # We pass in a single None value so that the expression evaluator has some data to
                # pass to the expressions.
                resolved_expr = calculate_expression(
                    expr,
                    ColumnEmulator(data=[None]),
                    session._analyzer,
                    {},
                )

                # If the length of the resolved expression is not a single value we cannot pass it as a literal.
                if len(resolved_expr) != 1:
                    raise ValueError(
                        "[Local Testing] Unexpected argument type {expr.__class__.__name__} for call to sproc"
                    )
                parsed_args.append(resolved_expr[0])
            else:
                parsed_args.append(arg)

        result = self.func(session, *parsed_args)

        # Semi-structured types are serialized in json
        if isinstance(
            self._return_type,
            (
                ArrayType,
                MapType,
                StructType,
            ),
        ) and not isinstance(result, DataFrame):
            result = json.dumps(result, indent=2, cls=CUSTOM_JSON_ENCODER)

        return result


class MockStoredProcedureRegistration(StoredProcedureRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[str, Callable] = dict()

    def register_from_file(
        self,
        file_path: str,
        func_name: str,
        return_type: Optional[DataType] = None,
        input_types: Optional[List[DataType]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        is_permanent: bool = False,
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        execute_as: typing.Literal["caller", "owner"] = "owner",
        strict: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        skip_upload_on_content_match: bool = False,
    ) -> StoredProcedure:
        LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
            external_feature_name="register sproc from file",
            internal_feature_name="MockStoredProcedureRegistration.register_from_file",
            parameters_info={},
            raise_error=NotImplementedError,
        )

    def _do_register_sp(
        self,
        func: Union[Callable, Tuple[str, str]],
        return_type: DataType,
        input_types: List[DataType],
        sp_name: str,
        stage_location: Optional[str],
        imports: Optional[List[Union[str, Tuple[str, str]]]],
        packages: Optional[List[Union[str, ModuleType]]],
        replace: bool,
        if_not_exists: bool,
        parallel: int,
        strict: bool,
        *,
        source_code_display: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
        execute_as: typing.Literal["caller", "owner"] = "owner",
        anonymous: bool = False,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        force_inline_code: bool = False,
    ) -> StoredProcedure:
        (
            udf_name,
            is_pandas_udf,
            is_dataframe_input,
            return_type,
            input_types,
        ) = process_registration_inputs(
            self._session,
            TempObjectType.PROCEDURE,
            func,
            return_type,
            input_types,
            sp_name,
            anonymous,
        )

        if is_pandas_udf:
            raise TypeError("pandas stored procedure is not supported")

        if packages or imports:
            LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                external_feature_name="uploading imports and packages for sprocs",
                internal_feature_name="MockStoredProcedureRegistration._do_register_sp",
                parameters_info={},
                raise_error=NotImplementedError,
            )

        check_python_runtime_version(self._session._runtime_version_from_requirement)

        if udf_name in self._registry and not replace:
            raise SnowparkSQLException(
                f"002002 (42710): SQL compilation error: \nObject '{udf_name}' already exists.",
                error_code="1304",
            )

        sproc = MockStoredProcedure(
            func,
            return_type,
            input_types,
            udf_name,
            execute_as=execute_as,
        )

        self._registry[udf_name] = sproc

        return sproc

    def call(
        self,
        sproc_name: str,
        *args: Any,
        session: Optional["snowflake.snowpark.session.Session"] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ):

        if sproc_name not in self._registry:
            raise SnowparkSQLException(
                f"[Local Testing] sproc {sproc_name} does not exist."
            )

        return self._registry[sproc_name](
            *args, session=session, statement_params=statement_params
        )
