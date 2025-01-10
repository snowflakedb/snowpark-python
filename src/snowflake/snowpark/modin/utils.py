#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""Collection of general utility functions, mostly for internal use."""


import functools
import importlib
import inspect
import os
import re
import types
from collections.abc import Hashable, Sequence
from pathlib import Path
from textwrap import dedent, indent
from typing import (
    Any,
    Callable,
    Literal,
    NoReturn,
    Optional,
    Protocol,
    TypeVar,
    Union,
    runtime_checkable,
)

import numpy as np
import pandas
from pandas._libs.lib import NoDefault, no_default
from pandas.core.dtypes.inference import is_float, is_integer
from pandas.util._decorators import Appender

from snowflake.snowpark.modin.config import DocModule
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage

T = TypeVar("T")
"""Generic type parameter"""

Fn = TypeVar("Fn", bound=Callable)
"""Function type parameter (used in decorators that don't change a function's signature)"""


@runtime_checkable
class SupportsPrivateToPandas(Protocol):  # noqa: PR01
    """Structural type for objects with a ``_to_pandas`` method (note the leading underscore)."""

    def _to_pandas(self) -> Any:  # noqa: GL08
        # TODO add proper return type
        pass


@runtime_checkable
class SupportsPublicToPandas(Protocol):  # noqa: PR01
    """Structural type for objects with a ``to_pandas`` method (without a leading underscore)."""

    def to_pandas(self) -> Any:  # noqa: GL08
        pass


@runtime_checkable
class SupportsPublicToNumPy(Protocol):  # noqa: PR01
    """Structural type for objects with a ``to_numpy`` method (without a leading underscore)."""

    def to_numpy(self) -> Any:  # pragma: no cover  # noqa: GL08
        pass


@runtime_checkable
class SupportsPrivateToNumPy(Protocol):  # noqa: PR01
    """Structural type for objects with a ``_to_numpy`` method (note the leading underscore)."""

    def _to_numpy(self) -> Any:  # pragma: no cover  # noqa: GL08
        pass


PANDAS_API_URL_TEMPLATE = f"https://pandas.pydata.org/pandas-docs/version/{pandas.__version__}/reference/api/{{}}.html"

MODIN_UNNAMED_SERIES_LABEL = "__reduced__"
"""
The '__reduced__' name is used internally by the query compiler as a column name to
represent pandas Series objects that are not explicitly assigned a name, so as to
distinguish between an N-element series and 1xN dataframe.
"""


def _make_api_url(token: str) -> str:
    """
    Generate the link to pandas documentation.

    Parameters
    ----------
    token : str
        Part of URL to use for generation.

    Returns
    -------
    str
        URL to pandas doc.

    Notes
    -----
    This function is extracted for better testability.
    """
    return PANDAS_API_URL_TEMPLATE.format(token)


def _get_indent(doc: str) -> int:
    """
    Compute indentation in docstring.

    Parameters
    ----------
    doc : str
        The docstring to compute indentation for.

    Returns
    -------
    int
        Minimal indent (excluding empty lines).
    """
    indents = _get_indents(doc)
    return min(indents) if indents else 0


def _get_indents(source: Union[list, str]) -> list:
    """
    Compute indentation for each line of the source string.

    Parameters
    ----------
    source : str or list of str
        String to compute indents for. Passed list considered
        as a list of lines of the source string.

    Returns
    -------
    list of ints
        List containing computed indents for each line.
    """
    indents = []

    if not isinstance(source, list):
        source = source.splitlines()

    for line in source:
        if not line.strip():
            continue
        for pos, ch in enumerate(line):  # noqa: B007
            if ch != " ":
                break
        indents.append(pos)
    return indents


def format_string(template: str, **kwargs: str) -> str:
    """
    Insert passed values at the corresponding placeholders of the specified template.

    In contrast with the regular ``str.format()`` this function computes proper
    indents for the placeholder values.

    Parameters
    ----------
    template : str
        Template to substitute values in.
    **kwargs : dict
        Dictionary that maps placeholder names with values.

    Returns
    -------
    str
        Formated string.
    """
    # We want to change indentation only for those values which placeholders are located
    # at the start of the line, in that case the placeholder sets an indentation
    # that the filling value has to obey.
    # RegExp determining placeholders located at the beginning of the line.
    regex = r"^( *)\{(\w+)\}"
    for line in template.splitlines():
        if line.strip() == "":
            continue
        match = re.search(regex, line)
        if match is None:
            continue
        nspaces = len(match.group(1))
        key = match.group(2)

        value = kwargs.get(key)
        if not value:
            continue
        value = dedent(value)

        # Since placeholder is located at the beginning of a new line,
        # it already has '\n' before it, so to avoid double new lines
        # we want to discard the first leading '\n' at the value line,
        # the others leading '\n' are considered as being put on purpose
        if value[0] == "\n":
            value = value[1:]
        # `.splitlines()` doesn't preserve last empty line,
        # so we have to restore it further
        value_lines = value.splitlines()
        # We're not indenting the first line of the value, since it's already indented
        # properly because of the placeholder indentation.
        indented_lines = [
            indent(line, " " * nspaces) if line != "\n" else line
            for line in value_lines[1:]
        ]
        # If necessary, restoring the last line dropped by `.splitlines()`
        if value[-1] == "\n":
            indented_lines += [" " * nspaces]

        indented_value = "\n".join([value_lines[0], *indented_lines])
        kwargs[key] = indented_value

    return template.format(**kwargs)


def align_indents(source: str, target: str) -> str:
    """
    Align indents of two strings.

    Parameters
    ----------
    source : str
        Source string to align indents with.
    target : str
        Target string to align indents.

    Returns
    -------
    str
        Target string with indents aligned with the source.
    """
    source_indent = _get_indent(source)
    target = dedent(target)
    return indent(target, " " * source_indent)


def append_to_docstring(message: str) -> Callable[[Fn], Fn]:
    """
    Create a decorator which appends passed message to the function's docstring.

    Parameters
    ----------
    message : str
        Message to append.

    Returns
    -------
    callable
    """

    def decorator(func: Fn) -> Fn:
        to_append = align_indents(func.__doc__ or "", message)
        return Appender(to_append)(func)

    return decorator


def _replace_doc(
    source_obj: object,
    target_obj: object,
    overwrite: bool,
    apilink: Optional[Union[str, list[str]]],
    parent_cls: Optional[Fn] = None,
    attr_name: Optional[str] = None,
    modify_doc: Optional[Callable[[object, str], str]] = None,
) -> None:
    """
    Replace docstring in `target_obj`, possibly taking from `source_obj` and augmenting.

    Can append the link to pandas API online documentation.

    Parameters
    ----------
    source_obj : object
        Any object from which to take docstring from.
    target_obj : object
        The object which docstring to replace.
    overwrite : bool
        Forces replacing the docstring with the one from `source_obj` even
        if `target_obj` has its own non-empty docstring.
    apilink : str | List[str], optional
        If non-empty, insert the link(s) to pandas API documentation.
        Should be the prefix part in the URL template, e.g. "pandas.DataFrame".
    parent_cls : class, optional
        If `target_obj` is an attribute of a class, `parent_cls` should be that class.
        This is used for generating the API URL as well as for handling special cases
        like `target_obj` being a property.
    attr_name : str, optional
        Gives the name to `target_obj` if it's an attribute of `parent_cls`.
        Needed to handle some special cases and in most cases could be determined automatically.
    modify_doc: Optional[Callable[[object, str],str]], default: None
        If not None, then call this function to retrieve a new docstring to use.
    """
    if isinstance(target_obj, (staticmethod, classmethod)):
        # we cannot replace docs on decorated objects, we must replace them
        # on original functions instead
        target_obj = target_obj.__func__

    source_doc = source_obj.__doc__ or ""
    target_doc = target_obj.__doc__ or ""
    overwrite = overwrite or not target_doc
    doc = source_doc if overwrite else target_doc

    if parent_cls and not attr_name:
        if isinstance(target_obj, property):
            attr_name = target_obj.fget.__name__  # type: ignore[union-attr]
        elif isinstance(target_obj, (staticmethod, classmethod)):
            attr_name = target_obj.__func__.__name__
        else:
            attr_name = target_obj.__name__  # type: ignore[attr-defined]

    if (
        source_doc.strip()
        and apilink
        and "pandas API documentation for " not in target_doc
        and (not (attr_name or "").startswith("_"))
    ):
        apilink_l = [apilink] if not isinstance(apilink, list) and apilink else apilink
        links = []
        for link in apilink_l:
            if attr_name:
                token = f"{link}.{attr_name}"
            else:
                token = link
            url = _make_api_url(token)
            links.append(f"`{token} <{url}>`_")

        indent_line = " " * _get_indent(doc)
        notes_section = f"\n{indent_line}Note\n{indent_line}-----\n"

        url_line = f"{indent_line}See pandas API documentation for {', '.join(links).strip()} for more.\n"
        notes_section_with_url = notes_section + url_line

        if notes_section in doc:
            doc = doc.replace(notes_section, notes_section_with_url)
        else:
            doc += notes_section_with_url

    # apply custom modify doc
    if modify_doc is not None:
        doc = modify_doc(target_obj, doc)

    if parent_cls and isinstance(target_obj, property):
        if overwrite:
            target_obj.fget.__doc_inherited__ = True  # type: ignore[union-attr]
        assert attr_name is not None
        setattr(
            parent_cls,
            attr_name,
            property(target_obj.fget, target_obj.fset, target_obj.fdel, doc),
        )
    else:
        if overwrite:
            target_obj.__doc_inherited__ = True  # type: ignore[attr-defined]
        target_obj.__doc__ = doc


# This is a map from objects whose docstrings we are overriding to functions that
# take a DocModule string and override the docstring according to the
# DocModule. When we update DocModule, we can use this map to update all
# inherited docstrings.
_docstring_inheritance_calls: list[Callable[[str], None]] = []

# This is a set of (class, attribute_name) pairs whose docstrings we have
# already replaced since we last updated DocModule. Note that we don't store
# the attributes themselves since we replace property attributes instead of
# modifying them in place:
# https://github.com/modin-project/modin/blob/e9dbcc127913db77473a83936e8b6bb94ef84f0d/modin/utils.py#L353
_attributes_with_docstrings_replaced: set[tuple[type, str]] = set()


def _documentable_obj(obj: object) -> bool:
    """Check if `obj` docstring could be patched."""
    return bool(
        callable(obj)
        or (isinstance(obj, property) and obj.fget)
        or (isinstance(obj, (staticmethod, classmethod)) and obj.__func__)
    )


def _update_inherited_docstrings(doc_module: DocModule) -> None:
    """
    Update all inherited docstrings.

    Parameters
    ----------
    doc_module: DocModule
        The current DocModule
    """
    _attributes_with_docstrings_replaced.clear()
    for doc_inheritance_call in _docstring_inheritance_calls:
        doc_inheritance_call(doc_module=doc_module.get())


def _inherit_docstrings_in_place(
    cls_or_func,
    doc_module,
    parent,
    excluded,
    overwrite_existing,
    apilink,
    modify_doc,
) -> None:
    # Import the docs module and get the class (e.g. `DataFrame`).
    imported_doc_module = importlib.import_module(doc_module)
    # Set the default parent so we can use it in case some docs are missing from
    # parent module.
    default_parent = parent
    # Try to get the parent object from the doc module, and if it isn't there,
    # get it from parent instead. We only do this if we are overriding pandas
    # documentation. We don't touch other docs.
    if doc_module != DocModule.default and "pandas" in str(
        getattr(parent, "__module__", "")
    ):
        parent_name = (
            # DocModule should use the class BasePandasDataset to override the
            # docstrings of BasePandasDataset, even if BasePandasDataset
            # normally inherits docstrings from a different `parent`.
            # TODO(https://github.com/modin-project/modin/issues/7134): upstream
            # this fix to Modin.
            "BasePandasDataset"
            if getattr(cls_or_func, "__name__", "") == "BasePandasDataset"
            # For other classes, override docstrings with the class that has the
            # same name as the `parent` class, e.g. DataFrame inherits
            # docstrings from doc_module.DataFrame.
            else getattr(parent, "__name__", "")
        )
        parent = getattr(imported_doc_module, parent_name, parent)
    if parent != default_parent:
        # Reset API link in case the docs are overridden.
        apilink = None
        overwrite_existing = True

    if parent not in excluded:
        _replace_doc(
            parent, cls_or_func, overwrite_existing, apilink, modify_doc=modify_doc
        )

    if not isinstance(cls_or_func, types.FunctionType):
        seen = set()
        for base in cls_or_func.__mro__:  # type: ignore[attr-defined]
            if base is object:
                continue
            for attr, obj in base.__dict__.items():
                if attr in seen or (base, attr) in _attributes_with_docstrings_replaced:
                    continue
                seen.add(attr)
                # Try to get the attribute from the docs class first, then
                # from the default parent (pandas), and if it's not in either,
                # set `parent_obj` to `None`.
                parent_obj = getattr(parent, attr, getattr(default_parent, attr, None))
                if (
                    parent_obj in excluded
                    or not _documentable_obj(parent_obj)
                    or not _documentable_obj(obj)
                ):
                    continue

                _replace_doc(
                    parent_obj,
                    obj,
                    overwrite_existing,
                    apilink,
                    parent_cls=cls_or_func,
                    attr_name=attr,
                    modify_doc=modify_doc,
                )

                _attributes_with_docstrings_replaced.add((base, attr))


def _inherit_docstrings(
    parent: object,
    excluded: list[object] = [],  # noqa: B006
    overwrite_existing: bool = False,
    apilink: Optional[Union[str, list[str]]] = None,
    modify_doc: Optional[Callable[[object, str], str]] = None,
) -> Callable[[Fn], Fn]:
    """
    Create a decorator which overwrites decorated object docstring(s).

    It takes `parent` __doc__ attribute. Also overwrites __doc__ of
    methods and properties defined in the target or its ancestors if it's a class
    with the __doc__ of matching methods and properties from the `parent`.

    Parameters
    ----------
    parent : object
        Parent object from which the decorated object inherits __doc__.
    excluded : list, default: []
        List of parent objects from which the class does not
        inherit docstrings.
    overwrite_existing : bool, default: False
        Allow overwriting docstrings that already exist in
        the decorated class.
    apilink : str | List[str], optional
        If non-empty, insert the link(s) to pandas API documentation.
        Should be the prefix part in the URL template, e.g. "pandas.DataFrame".
    modify_doc: Optional[Callable[[str],str]], default: None
        If not None, then call this function to retrieve a new docstring to use.

    Returns
    -------
    callable
        Decorator which replaces the decorated object's documentation with `parent` documentation.

    Notes
    -----
    Keep in mind that the function will override docstrings even for attributes which
    are not defined in target class (but are defined in the ancestor class),
    which means that ancestor class attribute docstrings could also change.
    """
    # NOTE this implementation fixes https://github.com/modin-project/modin/issues/7138,
    # which we need to fix upstream.
    def decorator(cls_or_func: Fn) -> Fn:
        inherit_docstring_in_place = functools.partial(
            _inherit_docstrings_in_place,
            cls_or_func=cls_or_func,
            parent=parent,
            excluded=excluded,
            overwrite_existing=overwrite_existing,
            apilink=apilink,
            modify_doc=modify_doc,
        )
        inherit_docstring_in_place(doc_module=DocModule.get())
        _docstring_inheritance_calls.append(inherit_docstring_in_place)
        return cls_or_func

    return decorator


DocModule.subscribe(_update_inherited_docstrings)


def expanduser_path_arg(argname: str) -> Callable[[Fn], Fn]:
    """
    Decorate a function replacing its path argument with "user-expanded" value.

    Parameters
    ----------
    argname : str
        Name of the argument which is containing a path to be expanded.

    Returns
    -------
    callable
        Decorator which performs the replacement.
    """

    def decorator(func: Fn) -> Fn:
        signature = inspect.signature(func)
        assert (
            getattr(signature.parameters.get(argname), "name", None) == argname
        ), f"Function {func} does not take '{argname}' as argument"

        @functools.wraps(func)
        def wrapped(*args: tuple, **kw: dict) -> Any:
            params = signature.bind(*args, **kw)
            if patharg := params.arguments.get(argname, None):
                if isinstance(patharg, str) and patharg.startswith(
                    "~"
                ):  # pragma: no cover
                    params.arguments[argname] = os.path.expanduser(patharg)
                elif isinstance(patharg, Path):  # pragma: no cover
                    params.arguments[argname] = patharg.expanduser()
                return func(*params.args, **params.kwargs)
            return func(*args, **kw)

        return wrapped  # type: ignore[return-value]

    return decorator


def extract_sections(text: str) -> list[dict[str, str]]:
    """
    split docstring into sections.
    Args:
        text: input text

    Returns:
        list of dictionaries where each dictionary contains a key heading, and a key content.
    """
    lines = text.split("\n")
    # produces list of lines that contain only - chars
    # this means that the line before is a heading for a start of a section (if not whitespace)
    heading_linenos = list(
        map(
            lambda t: t[0],
            filter(
                lambda t: len(t[1]) > 0 and t[1].count("-") == len(t[1]),
                zip(range(len(lines)), lines),
            ),
        )
    )

    last_lineno = 0
    last_heading = ""
    sections = []
    for lineno in heading_linenos:
        # is line before heading
        heading = ""
        if lineno - 1 >= 0:
            heading = lines[lineno - 1]

            # if only whitespace, it's a section starting with ---- (line sep)
            if len(heading.strip()) == 0:
                heading = ""
        # fetch section as string
        section_lines = lines[last_lineno : lineno - 1]

        # remove ----- if section has one
        if section_lines[0].startswith("-"):
            section_lines = section_lines[1:]

        sections.append({"heading": last_heading, "content": "\n".join(section_lines)})
        last_lineno = lineno
        last_heading = heading

    # fetch section as string
    section_lines = lines[last_lineno:]

    # remove ----- if section has one
    if section_lines[0].startswith("-"):
        section_lines = section_lines[1:]

    sections.append({"heading": last_heading, "content": "\n".join(section_lines)})

    return sections


def assemble_sections_to_string(sections: list[dict[str, str]]) -> str:
    """
    create docstring from individual sections given as dictionaries (see extract sections)
    Args:
        sections:
            list of dictionaries where each dictionary contains a key heading, and a key content.
    Returns:
        combined string
    """

    return "\n".join(
        map(
            lambda section: section["heading"]
            + "\n"
            + "-" * len(section["heading"])
            + "\n"
            + section["content"],
            sections,
        )
    )


def find_index(seq: Sequence[Any], predicate: Callable) -> Optional[int]:
    """
    find index of element that satisfies first condition from left to right
    Args:
        seq: iterable to check with predicate
        predicate: predicate function to determine whether a match occurred or not

    Returns:
        index of first matching element, if no match None
    """

    indices = list(
        map(
            lambda t: t[0],
            filter(lambda t: predicate(t[1]), zip(range(len(seq)), seq)),
        )
    )
    if not indices:
        return None
    return indices[0]


# operators are similar and use the same example, generate docstring depending on current API support
def _create_operator_docstring(
    parent: object,
    excluded: list[object] = [],  # noqa: B006
    overwrite_existing: bool = False,
    apilink: Optional[Union[str, list[str]]] = None,
) -> Callable[[Fn], Fn]:
    def modify_doc(parent: object, doc: str) -> str:

        sections = extract_sections(doc)

        # add a Caution section to warn that fill_value / level are not supported yet.
        # before examples
        index = find_index(sections, lambda s: s["heading"] == "Examples")
        assert index is not None

        sections.insert(
            index,
            {
                "heading": "Caution",
                "content": "Snowpark pandas API does not support `fill_value` and `level` except for default values.",
            },
        )

        # When invalid numeric values are present, Snowpark denotes them with `None` while native pandas uses `NaN`.
        sections.insert(
            index,
            {
                "heading": "Caution",
                "content": "Snowpark pandas API denotes invalid numeric results with `None` while pandas uses `NaN`.",
            },
        )

        op_name = parent.__name__  # type: ignore[attr-defined]
        sections.insert(
            index,
            {
                "heading": "Caution",
                "content": "In Snowpark pandas API, whenever a binary operation involves a NULL value, "
                f"the result will preserve NULL values, e.g. NULL.{op_name}(<other>) will yield NULL.",
            },
        )

        # for div operators, add another caution section pointing out the different div-by-zero behavior
        if "floordiv" in op_name or "truediv" in op_name:
            sections.insert(
                index,
                {
                    "heading": "Caution",
                    "content": "Snowpark pandas API will always produce a division by zero error if the right hand side contains one or more zeroes. This is different from pandas which only produces a ZeroDivisionError exception when ``dtype='object'``.",
                },
            )

        if "pow" in op_name:
            # Zero raised to negative powers is supported in Snowpark and results in inf values. None ** 0 = 1 and 1 ** None = 1
            sections.insert(
                index,
                {
                    "heading": "Caution",
                    "content": "In Snowpark pandas API, zero raised to negative powers produces `inf` value as the result while pandas raises a ValueError. In Snowpark pandas API, `None` raised to `0` and `1` raised to `None` produce `1.0` as the result. pandas produces `np.nan` for both cases.",
                },
            )

        # Currently unimplemented, behavior unknown. Raises NotImplementedError.
        # TODO: SNOW-916731 bug with %d and 0.
        # TODO: SNOW-916733 support string and non-numeric data as operands.
        if "mod" in op_name or "rmod" in op_name:
            sections.insert(
                index,
                {
                    "heading": "Caution",
                    "content": """
                    Snowpark pandas API computes `mod` differently from pandas and Python. Snowpark pandas API only supports numeric data with the `mod` operator. Below is a table highlighting the differences in modulo computation for Python, pandas, and Snowpark pandas.
                    +---------------------------+---------------------------+---------------------------+
                    | Python                    | pandas                    | Snowpark pandas           |
                    +---------------------------+---------------------------+---------------------------+
                    |  7 %  5 =  2              |  7 %  5 =  2              |  7 %  5 =  2              |
                    +---------------------------+---------------------------+---------------------------+
                    |  7 % -5 = -3              |  7 % -5 = -3              |  7 % -5 =  2              |
                    +---------------------------+---------------------------+---------------------------+
                    | -7 %  5 =  3              | -7 %  5 =  3              | -7 %  5 = -2              |
                    +---------------------------+---------------------------+---------------------------+
                    | -7 % -5 = -2              | -7 % -5 = -2              | -7 % -5 = -2              |
                    +---------------------------+---------------------------+---------------------------+
                    """,
                },
            )

        # in original pandas, e.g. for https://pandas.pydata.org/pandas-docs/version/1.5.3/reference/api/pandas.Series.sub.html
        # and all the other operators a standard example is used with the following data and always fill_value=0
        # a = pd.Series([1, 1, 1, np.nan], index=['a', 'b', 'c', 'd'])
        # b = pd.Series([1, np.nan, 1, np.nan], index=['a', 'b', 'd', 'e'])
        # a <op> b
        # we explicitly re-generate it here to get the correct behavior without fill_value.
        # there are also obvious doc gaps in pandas, so the best would be to generate the examples properly using Snowpark pandas API here.
        # because Snowpark pandas API requires a connection, for now use pandas.
        import pandas as pd

        # we differ here with the data from pandas docs, because pandas has different div by zero behavior
        # this here is the original pandas example data:
        # a_data = {'data':[1, 1, 1, np.nan], 'index':["a", "b", "c", "d"]}
        # b_data = {'data': [0, 1, 2, np.nan, 1], 'index' : ["a", "b", "c", "d", "f"]}
        a_data = {"data": [1, -2, 0, np.nan], "index": ["a", "b", "c", "d"]}
        b_data = {"data": [-2, 1, 3, np.nan, 1], "index": ["a", "b", "c", "d", "f"]}
        a_doc = "pd.Series([1, -2, 0, np.nan], index=['a', 'b', 'c', 'd'])"
        b_doc = "pd.Series([-2, 1, 3, np.nan, 1], index=['a', 'b', 'c', 'd', 'f'])"

        # to avoid div-by-zero, swap for rtruediv and rfloordiv a,b
        if op_name in ["rtruediv", "rfloordiv"]:
            a_data, b_data = b_data, a_data
            a_doc, b_doc = b_doc, a_doc

        # Snowpark and native pandas can have different behavior for negative mod operations.
        if op_name in ["mod", "rmod"]:
            a_data = {"data": [1, 2, 0, np.nan], "index": ["a", "b", "c", "d"]}
            b_data = {"data": [2, 1, 3, np.nan, 1], "index": ["a", "b", "c", "d", "f"]}
            a_doc = "pd.Series([1, 2, 0, np.nan], index=['a', 'b', 'c', 'd'])"
            b_doc = "pd.Series([2, 1, 3, np.nan, 1], index=['a', 'b', 'c', 'd', 'f'])"

        a = pd.Series(**a_data)
        b = pd.Series(**b_data)

        assert hasattr(a, op_name)
        ans = getattr(a, op_name)(b)

        # in Snowflake, comparing with NULL returns NULL
        if op_name in ["eq", "ne", "ge", "le", "gt", "lt"]:
            ans = ans.astype(object)
            ans[["d", "f"]] = None

        content = ">>> a = " + a_doc + "\n"
        content += ">>> a\n"
        content += str(a) + "\n"
        content += ">>> b = " + b_doc + "\n"
        content += ">>> b\n"
        content += str(b) + "\n"
        content += f">>> a.{op_name}(b)\n"
        content += str(ans) + "\n"

        # replace examples section
        index = find_index(sections, lambda s: s["heading"] == "Examples")
        assert index is not None
        sections[index] = {"heading": "Examples", "content": content}

        return assemble_sections_to_string(sections)

    return _inherit_docstrings(
        parent,
        excluded,
        overwrite_existing=overwrite_existing,
        apilink=apilink,
        modify_doc=modify_doc,
    )


# TODO add proper type annotation
def to_pandas(modin_obj: SupportsPrivateToPandas) -> Any:
    """
    Convert a Modin DataFrame/Series to a pandas DataFrame/Series.

    Parameters
    ----------
    modin_obj : modin.DataFrame, modin.Series
        The Modin DataFrame/Series to convert.

    Returns
    -------
    pandas.DataFrame or pandas.Series
        Converted object with type depending on input.
    """
    return modin_obj._to_pandas()


def hashable(obj: bool) -> bool:
    """
    Return whether the `obj` is hashable.

    Parameters
    ----------
    obj : object
        The object to check.

    Returns
    -------
    bool
    """
    # Happy path: if there's no __hash__ method, the object definitely isn't hashable
    if not hasattr(obj, "__hash__"):
        return False
    # Otherwise, we may still need to check for type errors, as in the case of `hash(([],))`.
    # (e.g. an unhashable object inside a tuple)
    try:
        hash(obj)
    except TypeError:
        return False
    return True


def try_cast_to_pandas(obj: Any, squeeze: bool = False) -> Any:
    """
    Convert `obj` and all nested objects from Modin to pandas if it is possible.

    If no convertion possible return `obj`.

    Parameters
    ----------
    obj : object
        Object to convert from Modin to pandas.
    squeeze : bool, default: False
        Squeeze the converted object(s) before returning them.

    Returns
    -------
    object
        Converted object.
    """
    if isinstance(obj, SupportsPrivateToPandas):
        result = obj._to_pandas()
        if squeeze:
            result = result.squeeze(axis=1)
        return result
    if isinstance(obj, SupportsPublicToPandas):
        result = obj.to_pandas()
        if squeeze:
            result = result.squeeze(axis=1)
        # Query compiler case, it doesn't have logic about convertion to Series
        if (
            isinstance(getattr(result, "name", None), str)
            and result.name == MODIN_UNNAMED_SERIES_LABEL
        ):
            result.name = None
        return result
    if isinstance(obj, (list, tuple)):
        return type(obj)([try_cast_to_pandas(o, squeeze=squeeze) for o in obj])
    if isinstance(obj, dict):
        return {k: try_cast_to_pandas(v, squeeze=squeeze) for k, v in obj.items()}
    if callable(obj):
        module_hierarchy = getattr(obj, "__module__", None)
        module_hierarchy = (
            [""] if module_hierarchy is None else module_hierarchy.split(".")
        )
        fn_name = getattr(obj, "__name__", None)
        if fn_name and module_hierarchy[0] == "modin":
            return (
                getattr(pandas.DataFrame, fn_name, obj)
                if module_hierarchy[-1] == "dataframe"
                else getattr(pandas.Series, fn_name, obj)
            )
    return obj


def wrap_into_list(*args: Any, skipna: bool = True) -> list[Any]:
    """
    Wrap a sequence of passed values in a flattened list.

    If some value is a list by itself the function appends its values
    to the result one by one instead inserting the whole list object.

    Parameters
    ----------
    *args : tuple
        Objects to wrap into a list.
    skipna : bool, default: True
        Whether or not to skip nan or None values.

    Returns
    -------
    list
        Passed values wrapped in a list.
    """

    def isnan(o: Any) -> bool:
        return o is None or (isinstance(o, float) and np.isnan(o))

    res = []
    for o in args:
        if skipna and isnan(o):
            continue
        if isinstance(o, list):
            res.extend(o)
        else:
            res.append(o)
    return res


def wrap_udf_function(func: Callable) -> Callable:
    """
    Create a decorator that makes `func` return pandas objects instead of Modin.

    Parameters
    ----------
    func : callable
        Function to wrap.

    Returns
    -------
    callable
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = func(*args, **kwargs)
        # if user accidently returns modin DataFrame or Series
        # casting it back to pandas to properly process
        return try_cast_to_pandas(result)

    wrapper.__name__ = func.__name__
    return wrapper


def instancer(_class: Callable[[], T]) -> T:
    """
    Create a dummy instance each time this is imported.

    This serves the purpose of allowing us to use all of pandas plotting methods
    without aliasing and writing each of them ourselves.

    Parameters
    ----------
    _class : object

    Returns
    -------
    object
        Instance of `_class`.
    """
    return _class()


def import_optional_dependency(name: str, message: str) -> types.ModuleType:
    """
    Import an optional dependecy.

    Parameters
    ----------
    name : str
        The module name.
    message : str
        Additional text to include in the ImportError message.

    Returns
    -------
    module : ModuleType
        The imported module.
    """
    try:
        return importlib.import_module(name)
    except ImportError:
        raise ImportError(
            f"Missing optional dependency '{name}'. {message} "
            + f"Use pip or conda to install {name}."
        ) from None


def is_property(key: Any) -> bool:
    """Check whether `key` is a property."""
    return type(key) == property


def error_not_implemented_parameter(param: str, condition: bool) -> Optional[NoReturn]:
    """
    Raises a ``NotImplementedError`` for a parameter ``param``
    when its argument ``arg`` is not None.

    Parameters
    ----------
    param : str
    Name of the parameter that is not implemented.

    arg : Any
    Value of the parameter that is not implemented.

    Raises
    ------
    NotImplementedError if ``arg`` is None
    """

    if condition:
        ErrorMessage.not_implemented(f"{param} is not implemented.")

    return None


def warn_not_supported_parameter(param: str, condition: bool, fn: str) -> None:
    """
    Invokes ``WarningMessage.ignored_argument`` for parameter ``param``
    of function ``fn`` if its argument ``arg`` is not None.

    Parameters
    ----------
    param : str
    Name of the parameter that has been ignored.

    arg : Any
    Value of the parameter that has been ignored.

    fn : str
    Name of the function with parameter ``param``
    """

    if condition:
        WarningMessage.ignored_argument(fn, param, "")


def should_parse_header(
    header: Optional[Union[int, Sequence[int], Literal["infer"]]],
    names: Optional[Union[Sequence[Hashable], NoDefault]],
) -> bool:
    """
    Returns whether the first row of a CSV file is used as
    the column names for a DataFrame.

    Parameters
    ----------
    header : int, list of int, None
    Row number(s) to use as the column names, and the start of the
    data.

    names : array-like
    List of column names to use.

    Returns
    -------
    Whether the first row of the CSV file will be used as column names.
    """

    should_parse_first_row = header in ["infer", 0] and (names is None or header == 0)

    return should_parse_first_row


def translate_pandas_default(arg: Any) -> Any:
    """
    Returns None if `arg` is no_default, otherwise `arg`.

    Parameters
    ----------
    arg : Any
    Argument `arg` to be translated.

    Returns
    -------
    None if `arg` is no_default, otherwise the original
    value of `arg`.
    """

    return None if arg is no_default else arg


def validate_int_kwarg(value: int, arg_name: str, float_allowed: bool = False) -> int:
    """
    check whether the argument passed is integer type

    Parameters
    ----------
    value : int
        The integer value to be validated
    arg_name: str
        Name of the argument. To be reflected in the error message.
    float_allowed: bool
        Whether allow integer value to be represented in float, for example, 1.0.
        However, values like 2.3 is not valid.

    Returns
    -------
    Integer representation of the value.

    ValueError is raised if the value is not a valid integer
    """
    if is_integer(value) or (float_allowed and is_float(value) and value.is_integer()):  # type: ignore[attr-defined]
        return int(value)

    raise ValueError(f"{arg_name} must be an integer")


def doc_replace_dataframe_with_link(_obj: Any, doc: str) -> str:
    """
    Helper function to be passed as the `modify_doc` parameter to `_inherit_docstrings`. This replaces
    all unqualified instances of "DataFrame" with ":class:`~modin.pandas.DataFrame`" to
    prevent it from linking automatically to snowflake.snowpark.DataFrame: see SNOW-1233342.

    To prevent it from overzealously replacing examples in doctests or already-qualified paths, it
    replaces matches of DataFrame that are preceded by whitespace and followed by whitespace or a comma
    (to cover cases like "param: Series or DataFrame, optional").
    """
    return re.sub(
        r"(?<=\s)DataFrame(?=[\s,])",
        ":class:`~modin.pandas.DataFrame`",
        doc,
    )


class classproperty:
    """
    Decorator that allows creating read-only class properties.

    Parameters
    ----------
    func : method

    Examples
    --------
    >>> class A:
    ...     field = 10
    ...     @classproperty
    ...     def field_x2(cls):
    ...             return cls.field * 2
    ...
    >>> print(A.field_x2)
    20
    """

    def __init__(self, func: Any) -> None:
        self.fget = func

    def __get__(self, instance: Any, owner: Any) -> Any:  # noqa: GL08
        return self.fget(owner)
