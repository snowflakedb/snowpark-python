#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
from functools import cached_property
from typing import Optional, Union, List, Callable
import logging
import pytz
from dateutil import parser

from snowflake.snowpark._internal.data_source.dbms_dialects import BaseDialect
from snowflake.snowpark._internal.data_source.drivers import BaseDriver
from snowflake.snowpark._internal.data_source.utils import (
    detect_dbms,
    DBMS_MAP,
    DRIVER_MAP,
)

from snowflake.snowpark._internal.data_source.datasource_reader import DataSourceReader
from snowflake.snowpark._internal.type_utils import type_string_to_type_object
from snowflake.snowpark._internal.data_source.datasource_typing import Connection
from snowflake.snowpark.types import (
    StructType,
    _NumericType,
    DateType,
    DataType,
)

logger = logging.getLogger(__name__)


class DataSourcePartitioner:
    def __init__(
        self,
        create_connection: Callable[[], "Connection"],
        table_or_query: str,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
        query_timeout: Optional[int] = 0,
        fetch_size: Optional[int] = 0,
        custom_schema: Optional[Union[str, StructType]] = None,
        predicates: Optional[List[str]] = None,
        session_init_statement: Optional[List[str]] = None,
    ) -> None:
        self.create_connection = create_connection
        self.table_or_query = table_or_query
        self.column = column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions
        self.query_timeout = query_timeout
        self.fetch_size = fetch_size
        self.custom_schema = custom_schema
        self.predicates = predicates
        self.session_init_statement = session_init_statement
        conn = create_connection()
        dbms_type, driver = detect_dbms(conn)
        self.dialect_class = DBMS_MAP.get(dbms_type, BaseDialect)
        self.driver_class = DRIVER_MAP.get(driver, BaseDriver)
        self.dialect = self.dialect_class()
        self.driver = self.driver_class(create_connection)

    def reader(self) -> DataSourceReader:
        return DataSourceReader(
            self.driver_class,
            self.create_connection,
            self.schema,
            self.fetch_size,
            self.query_timeout,
            self.session_init_statement,
        )

    @cached_property
    def schema(self) -> StructType:
        if self.custom_schema is None:
            return self.driver.infer_schema_from_description(self.table_or_query)
        else:
            if isinstance(self.custom_schema, str):
                schema = type_string_to_type_object(self.custom_schema)
                if not isinstance(schema, StructType):
                    raise ValueError(
                        f"Invalid schema string: {self.custom_schema}. "
                        f"You should provide a valid schema string representing a struct type."
                        'For example: "id INTEGER, int_col INTEGER, text_col STRING".'
                    )
                return schema
            elif isinstance(self.custom_schema, StructType):
                return self.custom_schema
            else:
                raise ValueError(
                    f"Invalid schema type: {type(self.custom_schema)}."
                    'The schema should be either a valid schema string, for example: "id INTEGER, int_col INTEGER, text_col STRING".'
                    'or a valid StructType, for example: StructType([StructField("ID", IntegerType(), False)])'
                )

    @cached_property
    def partitions(self) -> List[str]:
        select_query = self.dialect.generate_select_query(
            self.table_or_query, self.schema
        )
        logger.debug(f"Generated select query: {select_query}")
        if self.column is None:
            if (
                self.lower_bound is not None
                or self.upper_bound is not None
                or self.num_partitions is not None
            ):
                raise ValueError(
                    "when column is not specified, lower_bound, upper_bound, num_partitions are expected to be None"
                )
            if self.predicates is None:
                partitioned_queries = [select_query]
            else:
                partitioned_queries = self.generate_partition_with_predicates(
                    select_query, self.predicates
                )
        else:
            if (
                self.lower_bound is None
                or self.upper_bound is None
                or self.num_partitions is None
            ):
                raise ValueError(
                    "when column is specified, lower_bound, upper_bound, num_partitions must be specified"
                )

            column_type = None
            for field in self.schema.fields:
                col = (
                    self.column
                    if self.column[0] == '"' and self.column[-1] == '"'
                    else self.column.upper()
                )
                if field.name == col:
                    column_type = field.datatype
                    break
            if column_type is None:
                raise ValueError(f"Specified column {self.column} does not exist")

            if not isinstance(column_type, (_NumericType, DateType)):
                raise ValueError(
                    f"unsupported type {column_type}, column must be a numeric type like int and float, or date type"
                )
            partitioned_queries = self.generate_partition(
                select_query,
                column_type,
                self.column,
                self.lower_bound,
                self.upper_bound,
                self.num_partitions,
            )
        return partitioned_queries

    @staticmethod
    def generate_partition(
        select_query: str,
        column_type: DataType,
        column: Optional[str] = None,
        lower_bound: Optional[Union[str, int]] = None,
        upper_bound: Optional[Union[str, int]] = None,
        num_partitions: Optional[int] = None,
    ) -> List[str]:
        processed_lower_bound = to_internal_value(lower_bound, column_type)
        processed_upper_bound = to_internal_value(upper_bound, column_type)
        if processed_lower_bound > processed_upper_bound:
            raise ValueError("lower_bound cannot be greater than upper_bound")

        if processed_lower_bound == processed_upper_bound or num_partitions <= 1:
            return [select_query]

        if (processed_upper_bound - processed_lower_bound) >= num_partitions or (
            processed_upper_bound - processed_lower_bound
        ) < 0:
            actual_num_partitions = num_partitions
        else:
            actual_num_partitions = processed_upper_bound - processed_lower_bound
            logger.warning(
                "The number of partitions is reduced because the specified number of partitions is less than the difference between upper bound and lower bound."
            )

        # decide stride length
        upper_stride = (
            processed_upper_bound / decimal.Decimal(actual_num_partitions)
        ).quantize(decimal.Decimal("1e-18"), rounding=decimal.ROUND_HALF_EVEN)
        lower_stride = (
            processed_lower_bound / decimal.Decimal(actual_num_partitions)
        ).quantize(decimal.Decimal("1e-18"), rounding=decimal.ROUND_HALF_EVEN)
        precise_stride = upper_stride - lower_stride
        stride = int(precise_stride)

        lost_num_of_strides = (
            (precise_stride - decimal.Decimal(stride))
            * decimal.Decimal(actual_num_partitions)
            / decimal.Decimal(stride)
        )
        lower_bound_with_stride_alignment = processed_lower_bound + int(
            (lost_num_of_strides / 2 * decimal.Decimal(stride)).quantize(
                decimal.Decimal("1"), rounding=decimal.ROUND_HALF_UP
            )
        )

        current_value = lower_bound_with_stride_alignment

        partition_queries = []
        for i in range(actual_num_partitions):
            l_bound = (
                f"{column} >= '{to_external_value(current_value, column_type)}'"
                if i != 0
                else ""
            )
            current_value += stride
            u_bound = (
                f"{column} < '{to_external_value(current_value, column_type)}'"
                if i != actual_num_partitions - 1
                else ""
            )

            if u_bound == "":
                where_clause = l_bound
            elif l_bound == "":
                where_clause = f"{u_bound} OR {column} is null"
            else:
                where_clause = f"{l_bound} AND {u_bound}"

            partition_queries.append(select_query + f" WHERE {where_clause}")

        return partition_queries

    @staticmethod
    def generate_partition_with_predicates(
        select_query: str, predicates: List[str]
    ) -> List[str]:
        return [select_query + f" WHERE {predicate}" for predicate in predicates]


def to_internal_value(value: Union[int, str, float], column_type: DataType):
    if isinstance(column_type, _NumericType):
        return int(value)
    else:
        # TODO: SNOW-1909315: support timezone
        dt = parser.parse(value)
        return int(dt.replace(tzinfo=pytz.UTC).timestamp())


def to_external_value(value: Union[int, str, float], column_type: DataType):
    if isinstance(column_type, _NumericType):
        return value
    else:
        # TODO: SNOW-1909315: support timezone
        return datetime.datetime.fromtimestamp(value, tz=pytz.UTC)
