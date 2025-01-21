#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Hashable
from typing import Literal, NamedTuple, Optional, Union

import numpy as np

# Snowpark pandas API always treats the pandas label as a tuple(LabelComponent), when the length of tuple is > 1,
# it represents multi-index, otherwise it is single level.
LabelComponent = Hashable
LabelTuple = tuple[LabelComponent, ...]
# can be removed once move to pandas 2.0
DropKeep = Literal["first", "last", False]

# pandas defines list-like as objects that are considered list-like are for example Python lists, tuples, sets, NumPy arrays,
# and pandas Series according to https://pandas.pydata.org/docs/reference/api/pandas.api.types.is_list_like.html. Define
# them for Snowpark pandas here. Note that we exclude Snowpark pandas Series here explicitly.
ListLike = Union[set, list, tuple, np.ndarray]

ListLikeOfFloats = Union[set[float], list[float], tuple[float]]


class LabelIdentifierPair(NamedTuple):
    """
    pair between pandas label and the corresponding snowflake quoted identifier.
    """

    # Internal representation for pandas label used to access pandas dataframe
    label: LabelTuple
    # Used to access the snowpark dataframe with data in snowflake
    snowflake_quoted_identifier: str


JoinTypeLit = Literal["left", "right", "inner", "outer", "cross", "asof"]
AlignTypeLit = Literal[
    # If align column values matches exactly, merge frames line by line (this is
    # equivalent to joining on row position) otherwise perform LEFT OUTER JOIN on
    # align columns.
    "left",
    # If align column values matches exactly, merge frames line by line (this is
    # equivalent to joining on row position) otherwise perform FULL OUTER JOIN on
    # align columns.
    "outer",
    # If align column values matches exactly, merge frames line by line (this is
    # equivalent to joining on row position) otherwise perform INNER JOIN on
    # align columns
    "inner",
    # If align column values matches exactly, merge frames line by line (this is
    # equivalent to joining on row position) otherwise
    # - perform LEFT OUTER JOIN if left frame is non-empty
    # - perform RIGHT OUTER JOIN if left frame is empty
    "coalesce",
]  # right and inner can also be supported if needed

AlignSortLit = [
    # Align operator provides a default sorting capability, which sort the
    # align key lexicographically when the align type is outer, and the original
    # dataframe is not aligned. No sort will happen for other align types.
    "default_sort",
    # Always sort the align key lexicographically regardless of align type.
    "sort",
    # Do not sort the align key regardless of the align type.
    "no_sort",
]

SnowflakeSupportedFileTypeLit = Union[
    Literal["csv"], Literal["json"], Literal["parquet"]
]


class PandasLabelToSnowflakeIdentifierPair(NamedTuple):
    """
    Pair between pandas label and the corresponding snowflake quoted identifier.
    """

    # pandas label
    pandas_label: Optional[Hashable]
    # Snowflake quoted identifier
    snowflake_quoted_identifier: str


# once updated to pandas 2.0, remove this, because this can be directly imported from pandas._typing
InterpolateOptions = Literal[
    "linear",
    "time",
    "index",
    "values",
    "nearest",
    "zero",
    "slinear",
    "quadratic",
    "cubic",
    "barycentric",
    "polynomial",
    "krogh",
    "piecewise_polynomial",
    "spline",
    "pchip",
    "akima",
    "cubicspline",
    "from_derivatives",
]
