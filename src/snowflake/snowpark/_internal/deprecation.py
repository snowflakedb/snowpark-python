#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import functools
import warnings

from snowflake.snowpark._internal import utils


def deprecate(
    *, deprecate_version, remove_version, extra_warning_text="", extra_doc_string=""
):
    def deprecate_wrapper(func):
        deprecate_text = (
            f"Deprecated from version {deprecate_version}. "
            f"Will be removed from version {remove_version}. "
        )
        warning_text = (
            f"{func.__name__} is deprecated. "
            f"{deprecate_text}"
            f"The current API version is {utils.snowpark_version}. "
            f"{extra_warning_text} \n\n"
        )
        doc_string = f"{deprecate_text} {extra_doc_string} \n\n"
        func.__doc__ = f"{(func.__doc__ or '')}\n\n{' '*8}{doc_string}\n"

        @functools.wraps(func)
        def func_call_wrapper(*args, **kwargs):
            warnings.warn(warning_text, category=DeprecationWarning)
            return func(*args, **kwargs)

        return func_call_wrapper

    return deprecate_wrapper
