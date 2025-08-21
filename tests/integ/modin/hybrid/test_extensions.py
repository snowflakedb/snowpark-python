#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import pandas as native_pd
import pytest
from modin.config import context as modin_config_context
from numpy.testing import assert_array_equal
from pytest import param

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.fixture(
    params=[
        pytest.param(
            lambda obj, *args, **kwargs: obj.to_iceberg(*args, **kwargs),
            id="method",
        ),
        pytest.param(pd.to_iceberg, id="function"),
    ]
)
def to_iceberg(request):
    return request.param


@pytest.fixture(params=["Ray", "Pandas", "Snowflake"])
def global_backend(request):
    with modin_config_context(Backend=request.param):
        yield request.param


@sql_count_checker(query_count=0)
@pytest.mark.parametrize("object_backend", ["Ray", "Pandas"])
@pytest.mark.parametrize(
    "to_pandas",
    [
        pytest.param(lambda obj: obj.to_pandas(), id="method"),
        pytest.param(pd.to_pandas, id="function"),
    ],
)
def test_to_pandas(object_backend, to_pandas, global_backend):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_backend(object_backend)
    assert to_pandas(df).equals(df._to_pandas())
    assert to_pandas(df["a"]).equals(df["a"]._to_pandas())


@pytest.mark.parametrize(
    "object_backend",
    [param("Ray", marks=pytest.mark.skip(reason="SNOW-2276090")), "Pandas"],
)
@pytest.mark.parametrize(
    "to_snowpark",
    [
        param(
            lambda object, *args, **kwargs: object.to_snowpark(*args, **kwargs),
            id="method",
        ),
        param(
            lambda object, *args, **kwargs: pd.to_snowpark(object, *args, **kwargs),
            id="function",
        ),
    ],
)
@sql_count_checker(query_count=4)
def test_to_snowpark(object_backend, to_snowpark, global_backend):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_backend(object_backend)
    assert (
        to_snowpark(df)
        .to_pandas()  # query #1
        .equals(df.move_to("snowflake").to_snowpark().to_pandas())  # query #2
    )
    assert df.get_backend() == object_backend

    column = df["a"]
    assert column.get_backend() == object_backend
    assert (
        to_snowpark(column)
        .to_pandas()  # query #3
        .equals(column.move_to("snowflake").to_snowpark().to_pandas())  # query #4
    )
    assert column.get_backend() == object_backend


@pytest.mark.parametrize(
    "object_backend",
    [param("Ray", marks=pytest.mark.skip(reason="SNOW-2276090")), "Pandas"],
)
@pytest.mark.parametrize(
    "to_snowflake",
    [
        param(
            lambda object, *args, **kwargs: object.to_snowflake(*args, **kwargs),
            id="method",
        ),
        param(
            lambda object, *args, **kwargs: pd.to_snowflake(object, *args, **kwargs),
            id="function",
        ),
    ],
)
@sql_count_checker(query_count=6)
def test_to_snowflake(object_backend, to_snowflake, test_table_name, global_backend):
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).set_backend(object_backend)
    to_snowflake(df, test_table_name, if_exists="replace", index=False)  # query #1
    assert (
        pd.read_snowflake(test_table_name)  # query #2
        .to_pandas()  # query #3
        # to_snowflake() and back round trip changes dtypes.
        .astype(df.to_pandas().dtypes)  # no queries (data is native)
        .equals(df.to_pandas())
    )

    column = df["a"]
    assert column.get_backend() == object_backend
    to_snowflake(column, test_table_name, if_exists="replace", index=False)  # query #4
    assert (
        pd.read_snowflake(test_table_name)  # query #5
        .to_pandas()  # query #6
        # # to_snowflake() and back round trip changes dtypes.
        .astype(column.to_pandas().dtype)  # no queries (data is native)
        .equals(column.to_pandas().to_frame())
    )


@pytest.fixture(params=["Pandas", "Ray"])
def backend_for_read_snowflake(request):
    with modin_config_context(Backend=request.param):
        yield request.param


def test_read_snowflake_on_different_backend(
    backend_for_read_snowflake, test_table_name
):
    native_df = native_pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    snowpark_df = pd.session.create_dataframe(native_df)
    snowpark_df.write.save_as_table(test_table_name, table_type="temp")

    with SqlCounter(query_count=2):
        result_df = pd.read_snowflake(test_table_name)

    assert result_df.get_backend() == backend_for_read_snowflake
    assert result_df.to_pandas().astype(native_df.dtypes).equals(native_df)


@sql_count_checker(query_count=4)
@pytest.mark.parametrize(
    "object_backend",
    [param("Ray", marks=pytest.mark.skip(reason="SNOW-2276090")), "Pandas"],
)
def test_dataframe_to_iceberg(
    object_backend,
    session,
    to_iceberg,
    test_table_name,
    global_backend,
):
    native_dataframe = native_pd.DataFrame([1])
    modin_dataframe = pd.DataFrame(native_dataframe).set_backend(object_backend)

    session.sql(
        "CREATE EXTERNAL VOLUME if not exists python_connector_iceberg_exvol"
    ).collect()
    iceberg_config = {
        "external_volume": "python_connector_iceberg_exvol",
        "catalog": "SNOWFLAKE",
        "base_location": "python_connector_merge_gate",
        "storage_serialization_policy": "OPTIMIZED",
    }
    to_iceberg(
        modin_dataframe,
        table_name=test_table_name,
        mode="overwrite",
        iceberg_config=iceberg_config,
        index=False,
    )
    snow_dataframe = pd.read_snowflake(test_table_name)
    assert_array_equal(
        # Snowpark handles index objects differently from native pandas, so just check values
        snow_dataframe.values,
        native_dataframe.values,
    )


@sql_count_checker(query_count=4)
@pytest.mark.parametrize(
    "object_backend",
    [param("Ray", marks=pytest.mark.skip(reason="SNOW-2276090")), "Pandas"],
)
def test_series_to_iceberg(
    object_backend,
    session,
    to_iceberg,
    test_table_name,
    global_backend,
):
    native_series = native_pd.Series([1, 2, 3], name="x")
    modin_series = pd.Series(native_series).set_backend(object_backend)

    session.sql(
        "CREATE EXTERNAL VOLUME if not exists python_connector_iceberg_exvol"
    ).collect()
    iceberg_config = {
        "external_volume": "python_connector_iceberg_exvol",
        "catalog": "SNOWFLAKE",
        "base_location": "python_connector_merge_gate",
        "storage_serialization_policy": "OPTIMIZED",
    }
    to_iceberg(
        modin_series,
        table_name=test_table_name,
        mode="overwrite",
        iceberg_config=iceberg_config,
        index=False,
    )
    snow_series = pd.read_snowflake(test_table_name).iloc[:, 0]
    assert_array_equal(
        # Snowpark handles index objects differently from native pandas, so just check values
        snow_series.values,
        native_series.values,
    )


@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "function,method_name",
    (
        param(
            lambda obj: obj.create_or_replace_view(name="test_view"),
            "create_or_replace_view",
            id="create_or_replace_view_method",
        ),
        param(
            lambda obj: obj.create_or_replace_dynamic_table(
                name="test_dynamic_table",
            ),
            "create_or_replace_dynamic_table",
            id="create_or_replace_dynamic_table_method",
        ),
        param(
            lambda obj: obj.to_view(name="test_view"),
            "to_view",
            id="to_view_method",
        ),
        param(
            lambda obj: pd.to_view(obj, name="test_view"),
            "to_view",
            id="to_view_function",
        ),
        param(
            lambda obj: obj.to_dynamic_table(
                name="test_dynamic_table",
                warehouse="test_warehouse",
                lag="test_lag",
            ),
            "to_dynamic_table",
            id="to_dynamic_table_method",
        ),
        param(
            lambda obj: pd.to_dynamic_table(
                obj,
                name="test_dynamic_table",
                warehouse="test_warehouse",
                lag="test_lag",
            ),
            "to_dynamic_table",
            id="to_dynamic_table_function",
        ),
        param(
            lambda obj: obj.cache_result(),
            "cache_result",
            id="cache_result_method",
        ),
    ),
)
@pytest.mark.parametrize("modin_class", [pd.DataFrame, pd.Series])
@pytest.mark.parametrize("object_backend", ["Ray", "Pandas"])
def test_unimplemented_extensions(
    function, method_name, modin_class, global_backend, object_backend
):
    object = modin_class([1]).set_backend(object_backend)
    with pytest.raises(
        NotImplementedError,
        match=re.escape(
            f"Modin supports the method {modin_class.__name__}.{method_name} on the Snowflake backend, but not on the backend {object_backend}."
        ),
    ):
        function(object)
