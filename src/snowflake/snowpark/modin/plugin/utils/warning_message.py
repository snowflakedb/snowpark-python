#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import functools
from logging import getLogger
from typing import Any, Callable

logger = getLogger(__name__)

SET_DATAFRAME_ATTRIBUTE_WARNING = (
    "Snowpark pandas API doesn't allow columns to be created via a new attribute name - see "
    + "https://pandas.pydata.org/pandas-docs/stable/indexing.html#attribute-access"
)


TUPLES_STORED_AS_ARRAY_DEFAULT_MESSAGE = (
    "Snowflake backend doesn't support tuples datatype. Tuple row labels are stored as"
    " ARRAY"
)

ORDER_BY_IN_SQL_QUERY_NOT_GUARANTEED_WARNING = (
    "The SQL query passed in to this invocation of `pd.read_snowflake` contains an ORDER BY "
    "clause. Currently, Snowpark pandas does not guarantee order is preserved when an ORDER BY is "
    "used with `pd.read_snowflake`. To ensure ordering, please use `pd.read_snowflake(...).sort_values(...)`."
)


def materialization_warning(func: Callable) -> Any:
    """The decorator to issue warning messages for operations lead to materialization with inadvertent slowness."""

    @functools.wraps(func)
    def wrap(*args, **kwargs):  # type: ignore
        WarningMessage.single_warning(
            "The current operation leads to materialization and can be slow if the data is large!"
        )
        return func(*args, **kwargs)

    return wrap


# TODO SNOW-828589: throw default to pandas warning here
class WarningMessage:
    printed_warnings: set[int] = set()  # Set of hashes of printed warnings

    @classmethod
    def single_warning(cls, message: str) -> None:
        """Warning will only be printed out at the first time."""
        message_hash = hash(message)
        if message_hash in cls.printed_warnings:
            logger.debug(f"Single Warning: {message} was raised and suppressed.")
            return

        logger.debug(f"Single Warning: {message} was raised.")
        logger.warning(message, stacklevel=2)
        cls.printed_warnings.add(message_hash)

    @classmethod
    def ignored_argument(cls, operation: str, argument: str, message: str) -> None:
        cls.single_warning(
            f"The argument `{argument}` of `{operation}` has been ignored by Snowpark pandas API:\n{message}."
        )

    # TODO SNOW-859965: Clean up ErrorMessage.mismatch_with_pandas in groupby.py
    @classmethod
    def mismatch_with_pandas(cls, operation: str, message: str) -> None:
        cls.single_warning(
            f"`{operation}` implementation may have mismatches with pandas:\n{message}."
        )

    @classmethod
    def tuples_stored_as_array(
        cls, message: str = TUPLES_STORED_AS_ARRAY_DEFAULT_MESSAGE
    ) -> None:
        cls.single_warning(message)

    @classmethod
    def index_to_pandas_warning(cls, func_name: str) -> None:
        # TODO: SNOW-1359041 re-enable it once lazy index representation is ready
        # cls.single_warning(
        #     f"The index method {func_name} currently calls to_pandas() and materializes data. In future updates, this method will be lazily evaluated"
        # )
        pass

    @classmethod
    def warning_if_engine_args_is_set(
        cls,
        operation: str,
        args: Any,
        kwargs: Any,
    ) -> None:  # pragma: no cover
        """
        Invokes ``ignored_argument`` for operation
        ``operation`` if arguments ``args`` or ``kwargs`` is set.

        Commonly used when ``engine`` or ``engine_kwargs`` are set for the given operation
        which Snowflake ignores as the ``engine`` is always SQL.

        Parameters
        ----------
        operation : str
            Name of operation.

        args : Any
            Arguments passed into operation ``operation``.

        kwargs : Any
            Keyword arguments passed into operation ``operation``.
        """
        engine_parameter_ignored_message = (
            "Snowpark pandas API executes on Snowflake. "
            "Ignoring engine related arguments to select a different execution engine."
        )

        if args:
            WarningMessage.ignored_argument(
                operation,
                "engine",
                engine_parameter_ignored_message,
            )

        if kwargs:
            WarningMessage.ignored_argument(
                operation,
                "engine_kwargs",
                engine_parameter_ignored_message,
            )

    @classmethod
    def lost_type_warning(cls, operation: str, type: str) -> None:
        cls.single_warning(
            f"`{type}` may be lost in `{operation}`'s result, please use `astype` to convert the result type back."
        )
