#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime

# import pandas as pd
import json
import re
import sys
import warnings

import modin.pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io.json as pio
import pytest
from _plotly_utils.optional_imports import get_module
from pytz import timezone

import snowflake.snowpark.modin.plugin

# from snowflake.snowpark.session import Session; session = Session.builder.create()


orjson = get_module("orjson")

eastern = timezone("US/Eastern")


# Testing helper
def build_json_opts(pretty=False):
    opts = {"sort_keys": True}
    if pretty:
        opts["indent"] = 2
    else:
        opts["separators"] = (",", ":")
    return opts


def to_json_test(value, pretty=False):
    return json.dumps(value, **build_json_opts(pretty=pretty))


def isoformat_test(dt_value):
    if isinstance(dt_value, np.datetime64):
        return str(dt_value)
    elif isinstance(dt_value, datetime.datetime):
        return dt_value.isoformat()
    else:
        raise ValueError(f"Unsupported date type: {type(dt_value)}")


def build_test_dict(value):
    return dict(a=value, b=[3, value], c=dict(Z=value))


def build_test_dict_string(value_string, pretty=False):
    if pretty:
        non_pretty_str = build_test_dict_string(value_string, pretty=False)
        return to_json_test(json.loads(non_pretty_str), pretty=True)
    else:
        value_string = str(value_string).replace(" ", "")
        return """{"a":%s,"b":[3,%s],"c":{"Z":%s}}""" % tuple([value_string] * 3)


def check_roundtrip(value, engine, pretty):
    encoded = pio.to_json_plotly(value, engine=engine, pretty=pretty)
    decoded = pio.from_json_plotly(encoded, engine=engine)
    reencoded = pio.to_json_plotly(decoded, engine=engine, pretty=pretty)
    assert encoded == reencoded

    # Check from_plotly_json with bytes on Python 3
    if sys.version_info.major == 3:
        encoded_bytes = encoded.encode("utf8")
        decoded_from_bytes = pio.from_json_plotly(encoded_bytes, engine=engine)
        assert decoded == decoded_from_bytes


# Fixtures
if orjson is not None:
    engines = ["json", "orjson", "auto"]
else:
    engines = ["json", "auto"]


@pytest.fixture(scope="module", params=engines)
def engine(request):
    return request.param


@pytest.fixture(scope="module", params=[False])
def pretty(request):
    return request.param


@pytest.fixture(scope="module", params=["float64", "int32", "uint32"])
def graph_object(request):
    return request.param


@pytest.fixture(scope="module", params=["float64", "int32", "uint32"])
def numeric_numpy_array(request):
    dtype = request.param
    return np.linspace(-5, 5, 4, dtype=dtype)


@pytest.fixture(scope="module")
def object_numpy_array(request):
    return np.array(["a", 1, [2, 3]], dtype="object")


@pytest.fixture(scope="module")
def numpy_unicode_array(request):
    return np.array(["A", "BB", "CCC"], dtype="U")


@pytest.fixture(
    scope="module",
    params=[
        datetime.datetime(2003, 7, 12, 8, 34, 22),
        datetime.datetime.now(),
        np.datetime64(datetime.datetime.utcnow()),
        pd.Timestamp(datetime.datetime.now()),
        eastern.localize(datetime.datetime(2003, 7, 12, 8, 34, 22)),
        eastern.localize(datetime.datetime.now()),
        pd.Timestamp(datetime.datetime.now(), tzinfo=eastern),
    ],
)
def datetime_value(request):
    return request.param


@pytest.fixture(
    params=[
        list,  # plain list of datetime values
        lambda a: pd.DatetimeIndex(a),  # Pandas DatetimeIndex
        lambda a: pd.Series(pd.DatetimeIndex(a)),  # Pandas Datetime Series
        lambda a: pd.DatetimeIndex(a).values,  # Numpy datetime64 array
        lambda a: np.array(a, dtype="object"),  # Numpy object array of datetime
    ]
)
def datetime_array(request, datetime_value):
    return request.param([datetime_value] * 3)


@pytest.mark.skip
# Encoding tests
def test_graph_object_input(engine, pretty):
    scatter = go.Scatter(x=[1, 2, 3], y=np.array([4, 5, 6]))
    result = pio.to_json_plotly(scatter, engine=engine)
    expected = """{"x":[1,2,3],"y":[4,5,6],"type":"scatter"}"""
    assert result == expected
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_numeric_numpy_encoding(numeric_numpy_array, engine, pretty):
    value = build_test_dict(numeric_numpy_array)
    result = pio.to_json_plotly(value, engine=engine, pretty=pretty)

    array_str = to_json_test(numeric_numpy_array.tolist())
    expected = build_test_dict_string(array_str, pretty=pretty)
    assert result == expected
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_numpy_unicode_encoding(numpy_unicode_array, engine, pretty):
    value = build_test_dict(numpy_unicode_array)
    result = pio.to_json_plotly(value, engine=engine, pretty=pretty)

    array_str = to_json_test(numpy_unicode_array.tolist())
    expected = build_test_dict_string(array_str)
    assert result == expected
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_object_numpy_encoding(object_numpy_array, engine, pretty):
    value = build_test_dict(object_numpy_array)
    result = pio.to_json_plotly(value, engine=engine, pretty=pretty)

    array_str = to_json_test(object_numpy_array.tolist())
    expected = build_test_dict_string(array_str)
    assert result == expected
    check_roundtrip(result, engine=engine, pretty=pretty)


def test_datetime(datetime_value, engine, pretty):
    value = build_test_dict(datetime_value)
    result = pio.to_json_plotly(value, engine=engine, pretty=pretty)
    expected = build_test_dict_string(f'"{isoformat_test(datetime_value)}"')
    assert result == expected
    check_roundtrip(result, engine=engine, pretty=pretty)


def test_datetime_arrays(datetime_array, engine, pretty):
    value = build_test_dict(datetime_array)
    result = pio.to_json_plotly(value, engine=engine)

    def to_str(v):
        try:
            v = v.isoformat(sep="T")
        except (TypeError, AttributeError):
            pass

        return str(v)

    if isinstance(datetime_array, list):
        dt_values = [to_str(d) for d in datetime_array]
    elif isinstance(datetime_array, pd.Series):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            # Series.dt.to_pydatetime will return Index[object]
            # https://github.com/pandas-dev/pandas/pull/52459
            dt_values = [
                to_str(d) for d in np.array(datetime_array.dt.to_pydatetime()).tolist()
            ]
    elif isinstance(datetime_array, pd.DatetimeIndex):
        dt_values = [to_str(d) for d in datetime_array.to_pydatetime().tolist()]
    else:  # numpy datetime64 array
        dt_values = [to_str(d) for d in datetime_array]

    array_str = to_json_test(dt_values)
    expected = build_test_dict_string(array_str)
    if orjson:
        # orjson always serializes datetime64 to ns, but json will return either
        # full seconds or microseconds, if the rest is zeros.
        # we don't care about any trailing zeros
        trailing_zeros = re.compile(r'[.]?0+"')
        result = trailing_zeros.sub('"', result)
        expected = trailing_zeros.sub('"', expected)

    assert result == expected
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_object_array(engine, pretty):
    fig = px.scatter(px.data.tips(), x="total_bill", y="tip", custom_data=["sex"])
    result = fig.to_plotly_json()
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_nonstring_key(engine, pretty):
    value = build_test_dict({0: 1})
    result = pio.to_json_plotly(value, engine=engine)
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_mixed_string_nonstring_key(engine, pretty):
    value = build_test_dict({0: 1, "a": 2})
    result = pio.to_json_plotly(value, engine=engine)
    check_roundtrip(result, engine=engine, pretty=pretty)


@pytest.mark.skip
def test_sanitize_json(engine):
    layout = {"title": {"text": "</script>\u2028\u2029"}}
    fig = go.Figure(layout=layout)
    fig_json = pio.to_json_plotly(fig, engine=engine)
    layout_2 = json.loads(fig_json)["layout"]
    del layout_2["template"]

    assert layout == layout_2

    replacements = {
        "<": "\\u003c",
        ">": "\\u003e",
        "/": "\\u002f",
        "\u2028": "\\u2028",
        "\u2029": "\\u2029",
    }

    for bad, good in replacements.items():
        assert bad not in fig_json
        assert good in fig_json
