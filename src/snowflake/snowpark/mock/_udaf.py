#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from types import ModuleType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from snowflake.snowpark._internal.ast.utils import build_udaf, with_src_position
from snowflake.snowpark._internal.udf_utils import process_registration_inputs
from snowflake.snowpark._internal.utils import TempObjectType, check_imports_type
from snowflake.snowpark.types import DataType
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

    def get_udaf(self, name: str) -> UserDefinedAggregateFunction:
        return self._registry[name]

    def get_udaf_imports(self, name: str) -> List[Any]:
        # TODO: implement this fully.
        return []

    def _do_register_udaf(
        self,
        handler: Union[Callable, Tuple[str, str]],
        return_type: Optional[DataType],
        input_types: Optional[List[DataType]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        *,
        native_app_params: Optional[Dict[str, Any]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        source_code_display: bool = True,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
        immutable: bool = False,
        _emit_ast: bool = True,
        **kwargs
    ) -> UserDefinedAggregateFunction:
        ast, ast_id = None, None
        if kwargs.get("_registered_object_name") is not None:
            if _emit_ast:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.udaf, stmt)
                ast_id = stmt.uid

            object_name = kwargs["_registered_object_name"]
            udaf = MockUserDefinedAggregateFunction(
                handler,
                object_name,
                return_type,
                input_types,
                packages=packages,
                _ast=ast,
                _ast_id=ast,
            )
            self._registry[object_name] = udaf
            return udaf

        check_imports_type(imports)

        # Retrieve the UDAF name, return and input types.
        (
            object_name,
            _,
            _,
            return_type,
            input_types,
            opt_arg_defaults,
        ) = process_registration_inputs(
            self._session,
            TempObjectType.AGGREGATE_FUNCTION,
            handler,
            return_type,
            input_types,
            name,
        )

        # Capture original parameters.
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.udaf, stmt)
            ast_id = stmt.uid
            build_udaf(
                ast,
                handler,
                return_type=return_type,
                input_types=input_types,
                name=name,
                stage_location=stage_location,
                imports=imports,
                packages=packages,
                replace=replace,
                if_not_exists=if_not_exists,
                parallel=parallel,
                external_access_integrations=external_access_integrations,
                secrets=secrets,
                immutable=immutable,
                comment=comment,
                statement_params=statement_params,
                is_permanent=is_permanent,
                session=self._session,
                _registered_object_name=object_name,
                **kwargs,
            )

        udaf = MockUserDefinedAggregateFunction(
            handler,
            object_name,
            return_type,
            input_types,
            packages=packages,
            _ast=ast,
            _ast_id=ast_id,
        )

        self._registry[object_name] = udaf

        return udaf
