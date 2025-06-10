#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from snowflake.snowpark._internal.ast.utils import build_udf, with_src_position
from snowflake.snowpark._internal.udf_utils import (
    check_python_runtime_version,
    process_registration_inputs,
)
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.mock._udf_utils import extract_import_dir_and_module_name
from snowflake.snowpark.mock._util import get_fully_qualified_name
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import DataType
from snowflake.snowpark.udf import UDFRegistration, UserDefinedFunction


class MockUserDefinedFunction(UserDefinedFunction):
    def __init__(self, *args, strict=False, use_session_imports=True, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.strict = strict
        self._imports = set()
        self.use_session_imports = use_session_imports

    def add_import(self, absolute_module_path: str) -> None:
        self.use_session_imports = False
        self._imports.add(absolute_module_path)


class MockUDFRegistration(UDFRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[
            str, MockUserDefinedFunction
        ] = (
            dict()
        )  # maps udf name to either the callable or a pair of str (module_name, callable_name)
        self._session_level_imports = set()
        self._lock = self._session._conn.get_lock()

    def _clear_session_imports(self):
        with self._lock:
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
        with self._lock:
            absolute_module_path, module_name = extract_import_dir_and_module_name(
                file_path, self._session._conn.stage_registry, import_path
            )
            if udf_name:
                self._registry[udf_name].add_import(absolute_module_path)
            else:
                self._session_level_imports.add(absolute_module_path)

            return module_name

    def get_udf(self, udf_name: str) -> MockUserDefinedFunction:
        with self._lock:
            if udf_name not in self._registry:
                raise SnowparkLocalTestingException(f"udf {udf_name} does not exist.")
            return self._registry[udf_name]

    def get_udf_imports(self, udf_name: str) -> Set[str]:
        with self._lock:
            udf = self._registry.get(udf_name)
            if not udf:
                return set()
            elif udf.use_session_imports:
                return self._session_level_imports
            else:
                return udf._imports

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
        copy_grants: bool = False,
        _emit_ast: bool = True,
        **kwargs,
    ) -> UserDefinedFunction:
        ast, ast_id = None, None
        if kwargs.get("_registered_object_name") is not None:
            if _emit_ast:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.udf, stmt)
                ast_id = stmt.uid

            object_name = kwargs["_registered_object_name"]
            udf = MockUserDefinedFunction(
                func,
                return_type,
                input_types,
                object_name,
                strict=strict,
                packages=packages,
                use_session_imports=imports is None,
                _ast=ast,
                _ast_id=ast_id,
            )
            self._registry[object_name] = udf
            return udf

        if is_permanent:
            self._session._conn.log_not_supported_error(
                external_feature_name="udf",
                error_message="Registering permanent UDF is not currently supported.",
                raise_error=NotImplementedError,
            )

        with self._lock:
            # Retrieve the UDF name, return and input types.
            (
                udf_name,
                is_pandas_udf,
                is_dataframe_input,
                return_type,
                input_types,
                opt_arg_defaults,
            ) = process_registration_inputs(
                self._session,
                TempObjectType.FUNCTION,
                func,
                return_type,
                input_types,
                name,
            )

            current_schema = self._session.get_current_schema()
            current_database = self._session.get_current_database()
            udf_name = get_fully_qualified_name(
                udf_name, current_schema, current_database
            )

            if _emit_ast:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.udf, stmt)
                ast_id = stmt.uid
                build_udf(
                    ast,
                    func,
                    return_type,
                    input_types,
                    name,
                    stage_location,
                    imports,
                    packages,
                    replace,
                    if_not_exists,
                    parallel,
                    max_batch_size,
                    strict,
                    secure,
                    external_access_integrations,
                    secrets,
                    immutable,
                    comment,
                    statement_params=statement_params,
                    source_code_display=source_code_display,
                    is_permanent=is_permanent,
                    session=self._session,
                    _registered_object_name=udf_name,
                    **kwargs,
                )

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

            if replace and if_not_exists:
                raise ValueError("options replace and if_not_exists are incompatible")

            if udf_name in self._registry and if_not_exists:
                ans = self._registry[udf_name]
                ans._ast = ast
                ans._ast_id = ast_id
                return ans

            if udf_name in self._registry and not replace:
                raise SnowparkSQLException(
                    f"002002 (42710): SQL compilation error: \nObject '{udf_name}' already exists.",
                    error_code="1304",
                )

            if packages:
                pass  # NO-OP

            # register
            self._registry[udf_name] = MockUserDefinedFunction(
                func,
                return_type,
                input_types,
                udf_name,
                strict=strict,
                packages=packages,
                use_session_imports=imports is None,
                _ast=ast,
                _ast_id=ast_id,
            )

            if type(func) is tuple:  # update file registration
                module_name = self._import_file(func[0], udf_name=udf_name)
                self._registry[udf_name].func = (module_name, func[1])

            if imports is not None:
                for _import in imports:
                    if type(_import) is str:
                        self._import_file(_import, udf_name=udf_name)
                    else:
                        local_path, import_path = _import
                        self._import_file(local_path, import_path, udf_name=udf_name)

            return self._registry[udf_name]
