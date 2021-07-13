#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import datetime
import time
from array import array
from decimal import Decimal
from json.decoder import JSONDecodeError

import pytest

from snowflake.snowpark.types.sf_types import Variant


def test_variant_basic_data_types():
    assert Variant(0).as_int() == 0
    assert Variant(0.0).as_float() == 0
    assert Variant(0).as_string() == "0"
    assert Variant(1.1).as_float() == 1.1
    assert Variant(1.1).as_int() == 1
    assert Variant(1.1).as_string() == "1.1"
    assert Variant(1).as_float() == 1.0
    assert Variant(1234).as_int() == 1234
    assert Variant(1234).as_float() == 1234.0
    assert Variant(Decimal(1.23)).as_decimal() == Decimal(1.23)
    assert Variant(Decimal(1.23)).as_float() == 1.23
    assert Variant(Decimal(0)).as_float() == 0
    assert Variant(True).as_bool()
    assert Variant(False).as_int() == 0
    assert Variant(False).as_string() == "False"
    assert Variant("test").as_string() == "test"
    assert Variant("test").as_json_string() == "test"
    assert Variant("1").as_int() == 1
    assert Variant("1.1").as_float() == 1.1
    assert Variant("").as_string() == ""
    assert Variant(bytearray([1, 2, 3])).as_bytes() == bytearray([1, 2, 3])
    assert Variant(bytearray([1, 2, 3])).as_bytearray() == bytearray([1, 2, 3])
    assert Variant(bytearray()).as_bytes() == bytearray()
    t = datetime.datetime.strptime("2021-02-03T04:05:06.000007", "%Y-%m-%dT%H:%M:%S.%f")
    assert Variant(t, datetime_format="%Y-%m-%dT%H:%M:%S.%f").as_datetime() == t
    assert (
        Variant(t, datetime_format="%Y-%m-%dT%H:%M:%S.%f").as_string()
        == "2021-02-03T04:05:06.000007"
    )
    assert Variant(t.date(), datetime_format="%Y-%m-%d").as_date() == t.date()
    assert Variant(t.time(), datetime_format="%H:%M:%S.%f").as_time() == t.time()
    timestamp = time.time()
    assert Variant(timestamp).as_date() == datetime.date.fromtimestamp(timestamp)
    assert Variant(timestamp).as_datetime() == datetime.datetime.fromtimestamp(
        timestamp
    )
    assert Variant(0).as_datetime() == datetime.datetime.fromtimestamp(0)
    assert Variant(None).as_string() == "None"


def test_variant_semi_structured_data_types():
    l = Variant([True, 1]).as_list()
    assert l[0].as_bool()
    assert l[1].as_int() == 1
    d = Variant({"a": 1, "b": 2}).as_dict()
    assert d["a"].as_int() == 1
    assert d["b"].as_int() == 2
    assert Variant(array("b", [1, 2, 3])).as_bytearray() == bytearray([1, 2, 3])
    assert Variant([1, 2, 3]).as_bytearray() == bytearray([1, 2, 3])

    assert Variant([1, 2, 3]).as_json_string() == "[1, 2, 3]"
    assert Variant(["1", "2", "3"]).as_json_string() == "['1', '2', '3']"
    assert Variant({"a": 1, "b": 2}).as_json_string() == "{'a': 1, 'b': 2}"
    assert Variant({"a": "1", "b": "2"}).as_json_string() == "{'a': '1', 'b': '2'}"

    v = Variant("[1, 2, 3]", is_json_string=True)
    assert v.as_list() == [Variant(1), Variant(2), Variant(3)]
    v = Variant('{"a": [1, 2], "b": "c"}', is_json_string=True)
    assert v.as_dict() == {"a": Variant([1, 2]), "b": Variant("c")}

    assert Variant([None]).as_list()[0].as_string() == "None"


def test_variant_equal():
    assert Variant("123") == Variant("123")
    assert Variant("123") != Variant(123)
    assert Variant("'") != Variant('"')


def test_variant_negative():
    with pytest.raises(ValueError) as ex_info:
        Variant(1).as_bool()
    assert "Conversion from Variant of Number to Boolean is not supported." in str(
        ex_info
    )
    with pytest.raises(ValueError) as ex_info:
        Variant(1).as_bytes()
    assert "Conversion from Variant of Number to Binary is not supported." in str(
        ex_info
    )
    with pytest.raises(ValueError) as ex_info:
        Variant("1.1").as_int()
    assert "invalid literal for int() with base 10" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        Variant([1.1, 2, 3]).as_bytes()
    assert "'float' object cannot be interpreted as an integer" in str(ex_info)
    with pytest.raises(TypeError) as ex_info:
        Variant(array("f", [1, 2, 3])).as_bytearray()
    assert "'float' object cannot be interpreted as an integer" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        Variant(time.time()).as_time()
    assert "Conversion from Variant of Number to Time is not supported." in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        Variant(time.time() * 1000).as_datetime()
    assert "out of range" in str(ex_info)
    t = datetime.datetime.strptime("2021-02-03T04:05:06.000007", "%Y-%m-%dT%H:%M:%S.%f")
    with pytest.raises(AssertionError) as ex_info:
        Variant(t)
    assert "The format of datetime should be specified" in str(ex_info)
    with pytest.raises(ValueError) as ex_info:
        Variant(t, datetime_format="%Y-%m-%dT%H:%M:%S.%f").as_float()
    assert "Conversion from Variant of Timestamp to Number is not supported." in str(
        ex_info
    )
    with pytest.raises(ValueError) as ex_info:
        Variant(t, datetime_format="%Y-%m-%dT%H:%M:%S.%f").as_date()
    assert "Conversion from Variant of Timestamp to Date is not supported." in str(
        ex_info
    )
    with pytest.raises(TypeError) as ex_info:
        Variant(1, is_json_string=True)
    assert "The input must be a string if `is_json_string` is set to True" in str(
        ex_info
    )
    with pytest.raises(JSONDecodeError):
        Variant("test", is_json_string=True).as_string()
    with pytest.raises(JSONDecodeError):
        Variant('{"a: [1, 2], "b": "c"}', is_json_string=True)
    with pytest.raises(AttributeError) as ex_info:
        Variant('{"a": [1, 2], "b": "c"}', is_json_string=False).as_dict()
    assert "object has no attribute 'items'" in str(ex_info)
