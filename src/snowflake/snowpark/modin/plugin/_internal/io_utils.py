#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import glob
import os
from collections.abc import Hashable
from typing import Any, Callable, Union

import numpy as np
import pandas as native_pd

import snowflake.snowpark.modin.pandas as pd
from snowflake.snowpark.session import Session

PANDAS_KWARGS = {"names", "index_col", "usecols", "dtype"}


def upload_local_path_to_snowflake_stage(
    session: Session, path: str, sf_stage: str
) -> None:
    """
    Uploads the contents of a local filepath (file or folder) ``path``
    to a staged location ``sf_stage`` in Snowflake.

    Parameters
    ----------
    session : Session
    Session object in Snowpark.

    path : str
    File path to local file or folder.

    sf_stage : str
    Name of Snowflake stage to upload files to.
    """

    # local file that begins with '@' (represents SF stage)
    if path.startswith(r"\@"):
        path = path[1:]

    # Snowflake uses glob patterns by default,
    # so escape any file path for special symbols like *, . in glob patterns
    if os.path.isdir(path):
        files = os.listdir(path)
        files_to_upload = [
            glob.escape(os.path.join(path, file))
            for file in files
            if os.path.isfile(os.path.join(path, file))
        ]
    elif os.path.isfile(path):
        files_to_upload = [glob.escape(path)]
    else:
        raise ValueError("path must be a folder or a file.")

    for file in files_to_upload:
        session.file.put(file, sf_stage, overwrite=True)


def is_local_filepath(filepath: str) -> bool:
    """
    Returns whether a filepath is local.

    Parameters
    ----------
    filepath : str
    File path to file or folder

    Returns
    -------
    bool
    Whether a filepath is local.
    """

    return not filepath.startswith("@") or filepath.startswith(r"\@")


def get_non_pandas_kwargs(kwargs: Any) -> Any:
    """
    Returns a new dict without pandas keyword
    arguments.

    Args:
        kwargs : Dict of keyword arguments to filter.

    Returns:
        dict without pandas kwargs.
    """

    snowpark_reader_kwargs = {
        kwarg_name: kwarg_value
        for kwarg_name, kwarg_value in kwargs.items()
        if kwarg_name not in PANDAS_KWARGS
    }

    return snowpark_reader_kwargs


def get_columns_to_keep_for_usecols(
    usecols: Union[Callable, list[str], list[int]],
    columns: "pd.Index",
    maintain_usecols_order: bool = False,
) -> list[Hashable]:
    """
    Returns a subset of `df_columns` to keep, based on `usecols`.

    Parameters
    ----------
    usecols : Callable, list of str, list of int.
        If `usecols` is a Callable, the callable function will be evaluated against the column names,
        returning names where the callable function evaluates to True. If `usecols` is a list, all elements must either
        be positional (i.e. integer indices into the document columns) or strings
        that correspond to column names provided either by the user in `names` or
        inferred from the document header row(s).

    columns : `pd.Index`.
        An index containing all DataFrame column labels

    maintain_usecols_order : bool, default False
        If True, the result's order is based on usecols. Otherwise, the order is based on columns.

    Returns
    -------
    List.
        Subset of columns to keep.

    Raises
    ------
    ValueError
        If column(s) expected in `usecols` are not found in frame's columns `columns`.
    """
    _usecols = usecols
    if callable(_usecols):
        keep = [column for column in columns if _usecols(column)]
    elif len(_usecols) == 0:
        keep = []
    else:
        if isinstance(_usecols, native_pd.core.series.Series):
            _usecols = _usecols.values

        if isinstance(_usecols[0], str):  # type: ignore
            invalid_columns = [column for column in _usecols if column not in columns]  # type: ignore
            if invalid_columns:
                raise ValueError(
                    f"'usecols' do not match columns, columns expected but not found: {invalid_columns}"
                )
        else:
            if not all(isinstance(c, int) or isinstance(c, np.int64) for c in _usecols):  # type: ignore
                raise ValueError(
                    "'usecols' must either be list-like of all strings, all unicode, all integers or a callable."
                )
            invalid_columns = [
                column for column in _usecols if column < 0 or column >= len(columns)  # type: ignore
            ]
            if invalid_columns:
                raise ValueError(
                    f"'usecols' do not match columns, columns expected but not found: {invalid_columns}"
                )

            # Turn index references to pandas labels.
            _usecols = [columns[column] for column in _usecols]  # type: ignore

        l1, l2 = (_usecols, columns) if maintain_usecols_order else (columns, _usecols)
        keep = [column for column in l1 if column in l2]
    return keep
