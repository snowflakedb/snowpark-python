#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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
