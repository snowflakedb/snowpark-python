#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from snowflake.snowpark._internal.udf_utils import (
    check_python_runtime_version,
    process_registration_inputs,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock._udf_utils import extract_import_dir_and_module_name
from snowflake.snowpark.mock._util import get_fully_qualified_name
from snowflake.snowpark.types import DataType
from snowflake.snowpark.udf import UDFRegistration, UserDefinedFunction


class MockUDFRegistration(UDFRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[
            str, Union[Callable, Tuple[str, str]]
        ] = (
            dict()
        )  # maps udf name to either the callable or a pair of str (module_name, callable_name)
        self._udf_level_imports = dict()  # maps udf name to a set of file paths
        self._session_level_imports = set()

    def _clear_session_imports(self):
        self._session_level_imports.clear()

    def _import_file(
        self,
        file_path: str,
        import_path: Optional[str] = None,
        udf_name: Optional[str] = None,
    ) -> str:
        """
        Imports a python file or a directory of python module structure or a zip of the former.
        Returns the name of the Python module to be imported.
        When udf_name is not None, the import is added to the UDF associated with the name;
        Otherwise, it is a session level import and will be used if no UDF-level imports are specified.
        """
        absolute_module_path, module_name = extract_import_dir_and_module_name(
            file_path, self._session._conn.stage_registry, import_path
        )
        if udf_name:
            self._udf_level_imports[udf_name].add(absolute_module_path)
        else:
            self._session_level_imports.add(absolute_module_path)

        return module_name

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
        comment: Optional[str] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
        native_app_params: Optional[Dict[str, Any]] = None,
    ) -> UserDefinedFunction:
        if is_permanent:
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
            self._session, TempObjectType.FUNCTION, func, return_type, input_types, name
        )

        current_schema = self._session.get_current_schema()
        current_database = self._session.get_current_database()
        udf_name = get_fully_qualified_name(udf_name, current_schema, current_database)

        # allow registering pandas UDF from udf(),
        # but not allow registering non-pandas UDF from pandas_udf()
        if from_pandas_udf_function and not is_pandas_udf:
            raise ValueError(
                "You cannot create a non-vectorized UDF using pandas_udf(). "
                "Use udf() instead."
            )

        custom_python_runtime_version_allowed = False

        if not custom_python_runtime_version_allowed:
            check_python_runtime_version(
                self._session._runtime_version_from_requirement
            )

        if udf_name in self._registry and not replace:
            raise SnowparkSQLException(
                f"002002 (42710): SQL compilation error: \nObject '{udf_name}' already exists.",
                error_code="1304",
            )

        if packages:
            pass  # NO-OP

        if imports is not None or type(func) is tuple:
            self._udf_level_imports[udf_name] = set()

        if imports is not None:
            for _import in imports:
                if type(_import) is str:
                    self._import_file(_import, udf_name=udf_name)
                else:
                    local_path, import_path = _import
                    self._import_file(local_path, import_path, udf_name=udf_name)

        if type(func) is tuple:  # register from file
            module_name = self._import_file(func[0], udf_name=udf_name)
            self._registry[udf_name] = (module_name, func[1])
        else:
            # register from callable
            self._registry[udf_name] = func

        return UserDefinedFunction(
            func, return_type, input_types, udf_name, packages=packages
        )
