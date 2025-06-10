#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Do not look up certain attributes in columns or index, as they're used for some
# special purposes, like serving remote context
# TODO: SNOW-1643986 examine whether to update upstream modin to follow this
_ATTRS_NO_LOOKUP = {
    "____id_pack__",
    "__name__",
    "_cache",
    "_ipython_canary_method_should_not_exist_",
    "_ipython_display_",
    "_repr_html_",
    "_repr_javascript_",
    "_repr_jpeg_",
    "_repr_json_",
    "_repr_latex_",
    "_repr_markdown_",
    "_repr_mimebundle_",
    "_repr_pdf_",
    "_repr_png_",
    "_repr_svg_",
    "__array_struct__",
    "__array_interface__",
    "_typ",
}


SERIES_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE_ERROR_MESSAGE = (
    "Currently do not support Series or list-like keys with range-like values"
)

SERIES_SETITEM_SLICE_AS_SCALAR_VALUE_ERROR_MESSAGE = (
    "Currently do not support assigning a slice value as if it's a scalar value"
)

SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SERIES_ERROR_MESSAGE = (
    "Snowpark pandas DataFrame cannot be used as an indexer with Series"
)

SERIES_SETITEM_INCOMPATIBLE_INDEXER_WITH_SCALAR_ERROR_MESSAGE = (
    "Scalar key incompatible with {} value"
)

SERIES_ITEMS_WARNING_MESSAGE = (
    "Series.items may result in executing one more query to fetch each row of this series. For better "
    "performance, consider instead using a method like Series.apply that Snowpark pandas "
    "can execute lazily and without fetching data from Snowflake."
)

DF_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE = (
    "Currently do not support Series or list-like keys with range-like values"
)

DF_SETITEM_SLICE_AS_SCALAR_VALUE = (
    "Currently do not support assigning a slice value as if it's a scalar value"
)

DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE = (
    "{} will result eager evaluation and potential data pulling, which is inefficient. For efficient Snowpark "
    "pandas usage, consider rewriting the code with an operator (such as DataFrame.apply or DataFrame.applymap) which "
    "can work on the entire DataFrame in one shot."
)
