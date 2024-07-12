#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
import typing
from copy import copy
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.type_utils import infer_type
from snowflake.snowpark._internal.udf_utils import (
    check_python_runtime_version,
    process_registration_inputs,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.mock import CUSTOM_JSON_ENCODER
from snowflake.snowpark.mock._plan import calculate_expression
from snowflake.snowpark.mock._snowflake_data_type import ColumnEmulator
from snowflake.snowpark.mock._udf_utils import (
    extract_import_dir_and_module_name,
    types_are_compatible,
)
from snowflake.snowpark.mock._util import ImportContext, get_fully_qualified_name
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.stored_procedure import (
    StoredProcedure,
    StoredProcedureRegistration,
)
from snowflake.snowpark.types import ArrayType, DataType, MapType, StructType


class MockStoredProcedure(StoredProcedure):
    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        imports: Set[str],
        execute_as: typing.Literal["caller", "owner"] = "owner",
        anonymous_sp_sql: Optional[str] = None,
        strict=False,
    ) -> None:
        self.imports = imports
        self.strict = strict
        super().__init__(
            func,
            return_type,
            input_types,
            name,
            execute_as=execute_as,
            anonymous_sp_sql=anonymous_sp_sql,
        )

    def __call__(
        self,
        *args: Any,
        session: Optional["snowflake.snowpark.session.Session"] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Any:
        args, session = self._validate_call(args, session)
        if self.strict and any([arg is None for arg in args]):
            return None

        # Unpack columns if passed
        parsed_args = []
        for arg, expected_type in zip(args, self._input_types):
            if isinstance(arg, Column):
                expr = arg._expression

                # If expression does not define its datatype we cannot verify it's compatibale.
                # This is potentially unsafe.
                if expr.datatype and not types_are_compatible(
                    expr.datatype, expected_type
                ):
                    raise SnowparkLocalTestingException(
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
                    raise SnowparkLocalTestingException(
                        f"Unexpected type {expr.__class__.__name__} for sproc argument of type {expected_type}"
                    )

                if not types_are_compatible(
                    resolved_expr.sf_type.datatype, expected_type
                ):
                    raise SnowparkLocalTestingException(
                        f"Unexpected type {resolved_expr.sf_type.datatype} for sproc argument of type {expected_type}"
                    )

                parsed_args.append(resolved_expr[0])
            else:
                inferred_type = infer_type(arg)
                if not types_are_compatible(expected_type, inferred_type):
                    raise SnowparkLocalTestingException(
                        f"Unexpected type {inferred_type} for sproc argument of type {expected_type}"
                    )
                parsed_args.append(arg)

        with ImportContext(self.imports):
            # Resolve handler callable
            if type(self.func) is tuple:
                module_name, handler_name = self.func
                exec(f"from {module_name} import {handler_name}")
                sproc_handler = eval(handler_name)
            else:
                sproc_handler = self.func

            try:
                result = sproc_handler(session, *parsed_args)
            except Exception as err:
                SnowparkLocalTestingException.raise_from_error(
                    err, error_message=f"Python Interpreter Error: {err}"
                )

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
        self._registry: Dict[
            str, Union[Callable, Tuple[str, str]]
        ] = (
            dict()
        )  # maps name to either the callable or a pair of str (module_name, callable_name)
        self._sproc_level_imports = dict()  # maps name to a set of file paths
        self._session_level_imports = set()

    def _clear_session_imports(self):
        self._session_level_imports.clear()

    def _import_file(
        self,
        file_path: str,
        import_path: Optional[str] = None,
        sproc_name: Optional[str] = None,
    ) -> str:
        """
        Imports a python file or a directory of python module structure or a zip of the former.
        Returns the name of the Python module to be imported.
        When sproc_name is not None, the import is added to the sproc associated with the name;
        Otherwise, it is a session level import and will be added to any sproc with no sproc level
        imports specified.
        """

        absolute_module_path, module_name = extract_import_dir_and_module_name(
            file_path, self._session._conn.stage_registry, import_path
        )

        if sproc_name:
            self._sproc_level_imports[sproc_name].add(absolute_module_path)
        else:
            self._session_level_imports.add(absolute_module_path)

        return module_name

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
        comment: Optional[str] = None,
        native_app_params: Optional[Dict[str, Any]] = None,
    ) -> StoredProcedure:

        if is_permanent:
            self._session._conn.log_not_supported_error(
                external_feature_name="sproc",
                error_message="Registering permanent sproc is not currently supported.",
                raise_error=NotImplementedError,
            )

        if anonymous:
            self._session._conn.log_not_supported_error(
                external_feature_name="sproc",
                error_message="Registering anonymous sproc is not currently supported.",
                raise_error=NotImplementedError,
            )
        (
            sproc_name,
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

        current_schema = self._session.get_current_schema()
        current_database = self._session.get_current_database()
        sproc_name = get_fully_qualified_name(
            sproc_name, current_schema, current_database
        )

        check_python_runtime_version(self._session._runtime_version_from_requirement)

        if replace and if_not_exists:
            raise ValueError("options replace and if_not_exists are incompatible")

        if sproc_name in self._registry and if_not_exists:
            return self._registry[sproc_name]

        if sproc_name in self._registry and not replace:
            raise SnowparkLocalTestingException(
                f"002002 (42710): SQL compilation error: \nObject '{sproc_name}' already exists.",
                error_code="1304",
            )

        if is_pandas_udf:
            raise TypeError("pandas stored procedure is not supported")

        if packages:
            pass  # NO-OP

        if imports is not None or type(func) is tuple:
            self._sproc_level_imports[sproc_name] = set()

        if imports is not None:
            for _import in imports:
                if isinstance(_import, str):
                    self._import_file(_import, sproc_name=sproc_name)
                elif isinstance(_import, tuple) and all(
                    isinstance(item, str) for item in _import
                ):
                    local_path, import_path = _import
                    self._import_file(local_path, import_path, sproc_name=sproc_name)
                else:
                    raise TypeError(
                        "stored-proc-level import can only be a file path (str) or a tuple of the file path (str) and the import path (str)"
                    )

        if type(func) is tuple:  # register from file
            if sproc_name not in self._sproc_level_imports:
                self._sproc_level_imports[sproc_name] = set()
            module_name = self._import_file(func[0], sproc_name=sproc_name)
            func = (module_name, func[1])

        if sproc_name in self._sproc_level_imports:
            sproc_imports = self._sproc_level_imports[sproc_name]
        else:
            sproc_imports = copy(self._session_level_imports)

        sproc = MockStoredProcedure(
            func,
            return_type,
            input_types,
            sproc_name,
            sproc_imports,
            execute_as=execute_as,
            strict=strict,
        )

        self._registry[sproc_name] = sproc

        return sproc

    def call(
        self,
        sproc_name: str,
        *args: Any,
        session: Optional["snowflake.snowpark.session.Session"] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ):
        current_schema = self._session.get_current_schema()
        current_database = self._session.get_current_database()
        sproc_name = get_fully_qualified_name(
            sproc_name, current_schema, current_database
        )

        if sproc_name not in self._registry:
            raise SnowparkLocalTestingException(
                f"Unknown function {sproc_name}. Stored procedure by that name does not exist."
            )

        return self._registry[sproc_name](
            *args, session=session, statement_params=statement_params
        )
