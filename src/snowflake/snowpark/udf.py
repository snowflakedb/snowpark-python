#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import io
import os
import pickle
from typing import Callable, List, NamedTuple, Optional, Tuple, Union, get_type_hints

import cloudpickle

from snowflake.snowpark.column import Column
from snowflake.snowpark.internal.sp_expressions import (
    Expression as SPExpression,
    SnowflakeUDF,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.types.sf_types import DataType, StringType
from snowflake.snowpark.types.types_package import (
    _python_type_to_snow_type,
    convert_to_sf_type,
    snow_type_to_sp_type,
)


class UserDefinedFunction:
    def __init__(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        name: str,
        is_return_nullable: bool = False,
    ):
        self.func = func
        self.return_type = return_type
        self.input_types = input_types
        self.name = name
        self.is_return_nullable = is_return_nullable

    def __call__(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
    ) -> Column:
        exprs = Utils.parse_positional_args_to_list(*cols)
        if not all(type(e) in [Column, str] for e in exprs):
            raise TypeError(
                "UDF {} input must be Column, str, or list".format(self.name)
            )

        return Column(
            self.__create_udf_expression(
                [
                    e.expression if type(e) == Column else Column(e).expression
                    for e in exprs
                ]
            )
        )

    def __create_udf_expression(self, exprs: List[SPExpression]) -> SnowflakeUDF:
        if len(exprs) != len(self.input_types):
            raise ValueError(
                "Incorrect number of arguments passed to the UDF:"
                " Expected: {}, Found: {}".format(len(exprs), len(self.input_types))
            )
        return SnowflakeUDF(
            self.name,
            exprs,
            snow_type_to_sp_type(self.return_type),
            nullable=self.is_return_nullable,
        )


class _UDFColumn(NamedTuple):
    datatype: DataType
    name: str


class UDFRegistration:

    _handler_name = "compute"

    def __init__(self, session):
        self.session = session

    def register(
        self,
        func: Callable,
        return_type: Optional[DataType] = None,
        input_types: Optional[List[DataType]] = None,
        name: Optional[str] = None,
    ) -> UserDefinedFunction:
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable "
                f"(__call__ is not defined): {type(func)}"
            )

        # get the udf name
        udf_name = (
            name
            or f"{self.session.getFullyQualifiedCurrentSchema()}.tempUDF_{Utils.random_number()}"
        )
        Utils.validate_object_name(udf_name)

        # get return and input types
        if return_type or input_types:
            new_return_type = return_type if return_type else StringType()
            is_return_nullable = False
            new_input_types = input_types if input_types else []
        else:
            (
                new_return_type,
                is_return_nullable,
                new_input_types,
            ) = self.__get_types_from_type_hints(func)

        # register udf
        self.__do_register_udf(func, new_return_type, new_input_types, udf_name)
        return UserDefinedFunction(
            func, return_type, new_input_types, udf_name, is_return_nullable
        )

    def __get_types_from_type_hints(
        self, func: Callable
    ) -> Tuple[DataType, bool, List[DataType]]:
        # For Python 3.10+, the result values of get_type_hints()
        # will become strings, which we have to change the implementation
        # here at that time. https://www.python.org/dev/peps/pep-0563/
        num_args = func.__code__.co_argcount
        python_types_dict = get_type_hints(func)
        assert "return" in python_types_dict, f"The return type must be specified"
        assert len(python_types_dict) - 1 == num_args, (
            f"The number of arguments ({num_args}) is different from "
            f"the number of argument type hints ({len(python_types_dict) - 1})"
        )

        return_type, is_nullable = _python_type_to_snow_type(
            python_types_dict["return"]
        )
        input_types = []
        # types are in order
        for key, python_type in python_types_dict.items():
            if key != "return":
                input_types.append(_python_type_to_snow_type(python_type)[0])

        return return_type, is_nullable, input_types

    def __do_register_udf(
        self,
        func: Callable,
        return_type: DataType,
        input_types: List[DataType],
        udf_name: str,
        stage_location: Optional[str] = None,
    ):
        arg_names = [f"arg{i+1}" for i in range(len(input_types))]
        input_args = [
            _UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]
        code = self.__generate_python_code(func, arg_names)

        upload_stage = (
            Utils.normalize_stage_location(stage_location)
            if stage_location
            else self.session.getSessionStage()
        )
        dest_prefix = Utils.get_udf_upload_prefix(udf_name)
        dest_file_name = f"udf_py_{Utils.random_number()}.py"
        upload_file_stage_location = f"{upload_stage}/{dest_prefix}/{dest_file_name}"
        with io.BytesIO(bytes(code, "utf8")) as input_stream:
            self.session.conn.upload_stream(
                input_stream=input_stream,
                stage_location=upload_stage,
                dest_filename=dest_file_name,
                dest_prefix=dest_prefix,
                compress_data=False,
                overwrite=True,
            )

        # build imports string
        all_urls = [
            *self.session._resolve_imports(upload_stage),
            upload_file_stage_location,
        ]
        all_imports = ",".join([f"'{url}'" for url in all_urls])
        self.__create_python_udf(
            return_type=return_type,
            input_args=input_args,
            handler=f"{os.path.splitext(dest_file_name)[0]}.{self._handler_name}",
            udf_name=udf_name,
            all_imports=all_imports,
            is_temporary=stage_location is None,
        )

    def __generate_python_code(self, func: Callable, arg_names: List[str]) -> str:
        pickled_func = cloudpickle.dumps(func, protocol=pickle.HIGHEST_PROTOCOL)
        args = ",".join(arg_names)
        code = f"""
import pickle

func = pickle.loads(bytes.fromhex('{pickled_func.hex()}'))

def {self._handler_name}({args}):
    return func({args})
"""
        return code

    def __create_python_udf(
        self,
        return_type: DataType,
        input_args: List[_UDFColumn],
        handler: str,
        udf_name: str,
        all_imports: str,
        is_temporary: bool,
    ):
        return_sql_type = convert_to_sf_type(return_type)
        input_sql_types = [convert_to_sf_type(arg.datatype) for arg in input_args]
        sql_func_args = ",".join(
            [f"{a.name} {t}" for a, t in zip(input_args, input_sql_types)]
        )
        create_udf_query = f"""
CREATE {"TEMPORARY" if is_temporary else ""} FUNCTION {udf_name}({sql_func_args})
RETURNS {return_sql_type}
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
IMPORTS=({all_imports})
HANDLER='{handler}'
"""
        self.session._run_query(create_udf_query)
