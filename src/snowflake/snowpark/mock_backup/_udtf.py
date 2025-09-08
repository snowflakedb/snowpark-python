#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# TODO SNOW-1800512: Implement mock udtf for local testing.
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from snowflake.snowpark._internal.ast.utils import build_udtf, with_src_position
from snowflake.snowpark._internal.udf_utils import process_registration_inputs
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import DataType, PandasDataFrameType, StructType
from snowflake.snowpark.udtf import (
    UDTFRegistration,
    UserDefinedTableFunction,
    _validate_output_schema_names,
)


class MockUserDefinedTableFunction(UserDefinedTableFunction):
    def __init__(self, *args, strict=False, use_session_imports=True, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.strict = strict
        self._imports = set()
        self.use_session_imports = use_session_imports

    def add_import(self, absolute_module_path: str) -> None:
        self.use_session_imports = False
        self._imports.add(absolute_module_path)


class MockUDTFRegistration(UDTFRegistration):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._registry: Dict[
            str, MockUserDefinedTableFunction
        ] = (
            dict()
        )  # maps udf name to either the callable or a pair of str (module_name, callable_name)
        self._session_level_imports = set()

    def get_udtf(self, name: str) -> UserDefinedTableFunction:
        return self._registry[name]

    def _do_register_udtf(
        self,
        handler: Union[Callable, Tuple[str, str]],
        output_schema: Union[StructType, Iterable[str], "PandasDataFrameType"],
        input_types: Optional[List[DataType]],
        input_names: Optional[List[str]],
        name: Optional[str],
        stage_location: Optional[str] = None,
        imports: Optional[List[Union[str, Tuple[str, str]]]] = None,
        packages: Optional[List[Union[str, ModuleType]]] = None,
        replace: bool = False,
        if_not_exists: bool = False,
        parallel: int = 4,
        strict: bool = False,
        secure: bool = False,
        external_access_integrations: Optional[List[str]] = None,
        secrets: Optional[Dict[str, str]] = None,
        immutable: bool = False,
        max_batch_size: Optional[int] = None,
        comment: Optional[str] = None,
        *,
        native_app_params: Optional[Dict[str, Any]] = None,
        statement_params: Optional[Dict[str, str]] = None,
        api_call_source: str,
        skip_upload_on_content_match: bool = False,
        is_permanent: bool = False,
        _emit_ast: bool = True,
        **kwargs,
    ) -> UserDefinedTableFunction:
        ast, ast_id = None, None
        if kwargs.get("_registered_object_name") is not None:
            if _emit_ast:
                stmt = self._session._ast_batch.bind()
                ast = with_src_position(stmt.expr.udtf, stmt)
                ast_id = stmt.uid

            object_name = kwargs["_registered_object_name"]
            udtf = MockUserDefinedTableFunction(
                handler,
                output_schema,
                input_types,
                object_name,
                _ast=ast,
                _ast_id=ast_id,
            )
            # Add to registry to MockPlan can execute.
            self._registry[object_name] = udtf
            return udtf

        if isinstance(output_schema, StructType):
            _validate_output_schema_names(output_schema.names)
            return_type = output_schema
            output_schema = None
        elif isinstance(output_schema, PandasDataFrameType):
            _validate_output_schema_names(output_schema.col_names)
            return_type = output_schema
            output_schema = None
        elif isinstance(
            output_schema, Iterable
        ):  # with column names instead of StructType. Read type hints to infer column types.
            output_schema = tuple(output_schema)
            _validate_output_schema_names(output_schema)
            return_type = None
        else:
            raise ValueError(
                f"'output_schema' must be a list of column names or StructType or PandasDataFrameType instance to create a UDTF. Got {type(output_schema)}."
            )

        # Retrieve the UDTF name, input types.
        (
            object_name,
            is_pandas_udf,
            is_dataframe_input,
            output_schema,
            input_types,
            opt_arg_defaults,
        ) = process_registration_inputs(
            self._session,
            TempObjectType.TABLE_FUNCTION,
            handler,
            return_type,
            input_types,
            name,
            output_schema=output_schema,
        )

        # Capture original parameters.
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.udtf, stmt)
            ast_id = stmt.uid

            build_udtf(
                ast,
                handler,
                output_schema=output_schema,
                input_types=input_types,
                name=name,
                stage_location=stage_location,
                imports=imports,
                packages=packages,
                replace=replace,
                if_not_exists=if_not_exists,
                parallel=parallel,
                max_batch_size=max_batch_size,
                strict=strict,
                secure=secure,
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

        udtf = MockUserDefinedTableFunction(
            handler,
            output_schema,
            input_types,
            object_name,
            packages=packages,
            _ast=ast,
            _ast_id=ast_id,
        )

        # Add to registry to MockPlan can execute.
        self._registry[object_name] = udtf

        return udtf
