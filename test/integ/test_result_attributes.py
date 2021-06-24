#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import uuid
from typing import List

from src.snowflake.snowpark.internal.analyzer.sf_attribute import Attribute

# TODO fix 'src.' in imports
from src.snowflake.snowpark.session import Session
from src.snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    GeographyType,
    LongType,
    MapType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
)

from ..utils import Utils as utils


def get_table_attributes(session: "Session", name: str) -> List["Attribute"]:
    return session.get_result_attributes("select * from {name}".format(name=name))


def get_attributes_with_types(
    session: "Session", name: str, types: List[str]
) -> List["Attribute"]:
    attributes = []
    try:
        schema = ",".join(["col_{} {}".format(idx, tp) for idx, tp in enumerate(types)])
        utils.create_table(session, name, schema)
        attributes = get_table_attributes(session, name)
    finally:
        utils.drop_table(session, name)
    return attributes


def test_integer_type(session_cnx, db_parameters):
    integers = ["number", "decimal", "numeric", "bigint", "int", "integer", "smallint"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, integers)
    for attribute in attributes:
        assert type(attribute.datatype) == LongType


def test_float_type(session_cnx, db_parameters):
    floats = ["float", "float4", "double", "real"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, floats)
    for attribute in attributes:
        assert type(attribute.datatype) == DoubleType


def test_string_type(session_cnx, db_parameters):
    strings = ["varchar", "char", "character", "string", "text"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, strings)
    for attribute in attributes:
        assert type(attribute.datatype) == StringType


def test_binary_type(session_cnx, db_parameters):
    binarys = ["binary", "varbinary"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, binarys)
    for attribute in attributes:
        assert type(attribute.datatype) == BinaryType


def test_logical_type(session_cnx, db_parameters):
    logicals = ["boolean"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, logicals)
    for attribute in attributes:
        assert type(attribute.datatype) == BooleanType


def test_date_and_time_type(session_cnx, db_parameters):
    dates = {
        "date": DateType,
        "datetime": TimestampType,
        "time": TimeType,
        "timestamp": TimestampType,
        "timestamp_ltz": TimestampType,
        "timestamp_ntz": TimestampType,
        "timestamp_tz": TimestampType,
    }
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, list(dates.keys()))
    for attribute, expected_type in zip(attributes, dates.values()):
        assert type(attribute.datatype) == expected_type


def test_semi_structured_type(session_cnx, db_parameters):
    semi_structures = ["variant", "object"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, semi_structures)
    assert type(attributes[0].datatype) == VariantType
    assert type(attributes[1].datatype) == MapType


def test_array_type(session_cnx, db_parameters):
    semi_structures = ["array"]
    table_name = utils.random_name()
    with session_cnx(db_parameters) as session:
        attributes = get_attributes_with_types(session, table_name, semi_structures)
    assert type(attributes[0].datatype) == ArrayType


def test_describe_schema_matches_execute_schema_for_show_queries(
    session_cnx, db_parameters
):
    objs = [
        "tables",
        "transactions",
        "locks",
        "schemas",
        "objects",
        "views",
        "columns",
        "sequences",
        "stages",
        "pipes",
        "streams",
        "tasks",
        "procedures",
        "parameters",
        "functions",
        "shares",
        "roles",
        "grants",
        "warehouses",
        "databases",
        "variables",
        "regions",
        "integrations",
    ]
    with session_cnx(db_parameters) as session:
        for obj in objs:
            query = "show {}".format(obj)
            # describe query
            show_query_schema_describe = session.get_result_attributes(query)
            assert len(show_query_schema_describe) > 0
            # execute query
            session._run_query(query)
            show_query_schema_execute = session.conn._cursor.description
            assert len(show_query_schema_execute) > 0
            assert [attribute.name for attribute in show_query_schema_describe] == [
                '"' + column[0] + '"' for column in show_query_schema_execute
            ]
