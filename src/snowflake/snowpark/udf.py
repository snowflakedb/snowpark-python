#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import io
import pickle
import zipfile
from typing import Callable, List, NamedTuple, Optional, Tuple, Union

import cloudpickle

from snowflake.snowpark.column import Column
from snowflake.snowpark.internal.sp_expressions import (
    Expression as SPExpression,
    SnowflakeUDF,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.types.sf_types import DataType, StringType
from snowflake.snowpark.types.types_package import (
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
    ):
        if not callable(func):
            raise TypeError(
                "Invalid function: not a function or callable (__call__ is not defined): {}".format(
                    type(func)
                )
            )

        self.func = func
        self.return_type = return_type
        self.input_types = input_types
        self.name = name

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
            nullable=(self.return_type is None),
        )


class UDFColumn(NamedTuple):
    datatype: DataType
    name: str


class UDFRegistration:
    def __init__(self, session):
        self.session = session

    def register(
        self,
        func: Callable,
        return_type: DataType = StringType(),
        input_types: Optional[List[DataType]] = None,
        name: Optional[str] = None,
        stage_location: Optional[str] = None,
    ) -> UserDefinedFunction:
        udf_name = (
            name
            or f"{self.session.getFullyQualifiedCurrentSchema()}.tempUDF_{Utils.random_number()}"
        )
        Utils.validate_object_name(udf_name)
        input_types_list = input_types if input_types else []
        self.__do_register_udf(
            func, return_type, input_types_list, udf_name, stage_location
        )
        return UserDefinedFunction(func, return_type, input_types_list, udf_name)

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
            UDFColumn(dt, arg_name) for dt, arg_name in zip(input_types, arg_names)
        ]
        code = self.__generate_python_code(func, arg_names)

        upload_stage = (
            Utils.normalize_stage_location(stage_location)
            if stage_location
            else self.session.getSessionStage()
        )
        dest_prefix = Utils.get_udf_upload_prefix(udf_name)
        # TODO: SNOW-406036 don't zip .py file
        dest_filename = f"udf_py_{Utils.random_number()}.zip"
        upload_file_stage_location = f"{upload_stage}/{dest_prefix}/{dest_filename}"
        # TODO: SNOW-406036 upload python file instead of zip containing udf
        #  after the server side issue is fixed
        input_stream = io.BytesIO()
        with zipfile.ZipFile(
            input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            zf.writestr(f"{dest_filename.split('.')[0]}.py", code)
        self.session.conn.upload_stream(
            input_stream=input_stream,
            stage_location=upload_stage,
            dest_filename=dest_filename,
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
            py_file_name=dest_filename,
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

def compute({args}):
    return func({args})
"""
        return code

    def __create_python_udf(
        self,
        return_type: DataType,
        input_args: List[UDFColumn],
        py_file_name: str,
        udf_name: str,
        all_imports: str,
        is_temporary: bool,
    ):
        return_sql_type = convert_to_sf_type(return_type)
        input_sql_types = [convert_to_sf_type(arg.datatype) for arg in input_args]
        sql_func_args = ",".join(
            [f"{a.name} {t}" for a, t in zip(input_args, input_sql_types)]
        )
        # TODO: always have `RUNTIME_VERSION=3.8` fields when prod has this commit
        #  https://github.com/snowflakedb/snowflake/commit/22fe9c4caa46ef9d47a58591d3a74cfde9c571dc
        current_sf_version = float(
            self.session._run_query("select current_version()")[0][0][:4]
        )
        create_udf_query = f"""
CREATE {"TEMPORARY" if is_temporary else ""} FUNCTION {udf_name}({sql_func_args})
RETURNS {return_sql_type}
LANGUAGE PYTHON
{"RUNTIME_VERSION=3.8" if current_sf_version >= 5.32 else ""}
IMPORTS=({all_imports})
HANDLER='{py_file_name.split(".")[0]}.compute'
"""
        self.session._run_query(create_udf_query)
