#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import numpy as np
import pandas as native_pd
import pytest

from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import TypeMapper
from snowflake.snowpark.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    TimestampType,
    VariantType,
)


class DummyUserDefinedDtype(native_pd.api.extensions.ExtensionDtype):
    """A dummy user-defined dtype for testing."""

    @property
    def type(self):
        return type(self)

    @property
    def name(self) -> str:
        return "dummy"

    @classmethod
    def construct_from_string(cls, string):
        if string == cls().name:
            return cls()
        raise TypeError(f"Cannot construct a '{cls.__name__}' from '{string}'")


@pytest.mark.parametrize(
    "pandas_type, snow_type",
    [
        (np.dtype("int64"), LongType),
        (np.dtype("float64"), DoubleType),
        (np.dtype("bool"), BooleanType),
        (np.dtype("O"), VariantType),
        (native_pd.StringDtype(), StringType),
        (native_pd.BooleanDtype(), BooleanType),
        (native_pd.Int64Dtype(), LongType),
        (native_pd.Float64Dtype(), DoubleType),
        (native_pd.DatetimeTZDtype(tz="UTC"), TimestampType),
    ],
)
def test_typemapper_to_snowflake_supported(pandas_type, snow_type):
    """Tests that TypeMapper.to_snowflake correctly maps supported pandas dtypes."""
    assert isinstance(TypeMapper.to_snowflake(pandas_type), snow_type)


@pytest.mark.parametrize(
    "unsupported_type",
    [
        native_pd.CategoricalDtype(),
        native_pd.PeriodDtype("D"),
        native_pd.IntervalDtype(),
        DummyUserDefinedDtype(),
    ],
)
def test_typemapper_to_snowflake_unsupported(unsupported_type):
    """Tests that TypeMapper.to_snowflake raises NotImplementedError for unsupported dtypes."""
    with pytest.raises(NotImplementedError):
        TypeMapper.to_snowflake(unsupported_type)


@pytest.mark.parametrize(
    "pandas_type", [native_pd.Timedelta, np.dtype("timedelta64[ns]")]
)
def test_type_mapper_to_timedelta_SNOW_1635405(pandas_type):
    assert isinstance(TypeMapper.to_snowflake(pandas_type), TimedeltaType)
