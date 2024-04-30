#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import logging
import sys
import uuid
from collections.abc import Hashable
from dataclasses import dataclass
from typing import Any, Optional, Union

import pandas

from snowflake.snowpark._internal.type_utils import (
    ColumnOrName,
    ColumnOrSqlExpr,
    LiteralType,
)
from snowflake.snowpark._internal.utils import parse_positional_args_to_list
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.functions import (
    coalesce,
    count,
    iff,
    lit,
    max as max_,
    not_,
    row_number,
    sum as sum_,
)
from snowflake.snowpark.modin.plugin._typing import AlignTypeLit, JoinTypeLit
from snowflake.snowpark.row import Row
from snowflake.snowpark.session import Session
from snowflake.snowpark.table_function import TableFunctionCall
from snowflake.snowpark.types import StructType
from snowflake.snowpark.window import Window

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from collections.abc import Iterable
else:
    from collections.abc import Iterable


@dataclass(frozen=True)
class OrderingColumn:
    """
    Representation of an ordering column for the dataframe. The ordering column is
    used for recovering the data order for the dataframe.
    """

    # The snowflake quoted identifier for the column that is used for ordering.
    snowflake_quoted_identifier: str
    # Whether sort the column in ascending or descending order, by default it is sort in ascending order.
    ascending: bool = True
    # Whether the null value comes before or after the none null values during sort, by default,
    # null values comes after non-null values.
    na_last: bool = True

    @property
    def snowpark_column(self) -> Column:
        """
        The corresponding SnowparkColumn that can be used for snowpark
        dataframe for sorting.
        """
        col = Column(self.snowflake_quoted_identifier)

        if self.ascending and self.na_last:
            return col.asc_nulls_last()
        elif self.ascending and not self.na_last:
            return col.asc_nulls_first()
        elif not self.ascending and self.na_last:
            return col.desc_nulls_last()
        else:  # i.e., not self.ascending and not self.na_last:
            return col.desc_nulls_first()

    def reverse(self) -> "OrderingColumn":
        """
        Generate a reversed OrderingColumn. To reverse the order of an `OrderingColumn`, we need to reverse both
        `ascending` and `na_last`. For example, an `OrderingColumn`( "column1" with ascending=True and na_last=False):

            SELECT column1
            FROM VALUES (1), (null), (2), (null), (3)
            ORDER BY column1
            ASC NULLS FIRST;

        The results will be (null), (null), 1, 2, 3.

        To reverse the order, the new `OrderingColumn` will be "column1" with ascending=False and na_last=True:

            SELECT column1
            FROM VALUES (1), (null), (2), (null), (3)
            ORDER BY column1
            DESC NULLS LAST;

        The results become 3, 2, 1, (null), (null).

        Returns:
            The reversed column.
        """
        return OrderingColumn(
            snowflake_quoted_identifier=self.snowflake_quoted_identifier,
            ascending=not self.ascending,
            na_last=not self.na_last,
        )


class DataFrameReference:
    """
    A class for referencing a SnowparkDataFrame object and providing access to its schema,
    which is designed to enable the mutability of Snowpark DataFrame and sharing across different OrderedDataFrame.
    """

    # Snowflake quoted identifiers of ALL Snowpark dataframe columns that is cached in the class.
    # This must be in the same length and order as the Snowpark dataframe.
    # Note that this field is Optional, when it is None, it doesn't mean the Snowpark dataframe
    # doesn't have any column, it only means the quoted identifiers of the Snowpark dataframe isn't
    # cached by the class.
    cached_snowflake_quoted_identifiers_tuple: Optional[tuple[str, ...]]

    def __init__(
        self,
        snowpark_dataframe: SnowparkDataFrame,
        snowflake_quoted_identifiers: Optional[list[str]] = None,
    ) -> None:
        """
        Constructor for  DataFrameReference.

        Args:
            snowpark_dataframe: SnowparkDataFrame. The Snowark dataframe that it refers to
            snowflake_quoted_identifiers: the snowflake quoted identifiers for the Snowpark dataframe that it
                refers to. Must include identifiers for all columns, not a subset.
        """
        self.snowpark_dataframe = snowpark_dataframe
        self._id = uuid.uuid4()  # for debug purpose only
        if snowflake_quoted_identifiers is not None:
            self.cached_snowflake_quoted_identifiers_tuple = tuple(
                snowflake_quoted_identifiers
            )
        else:
            self.cached_snowflake_quoted_identifiers_tuple = None

    @property
    def schema(self) -> StructType:
        """
        Get the schema of the referenced SnowparkDataFrame.

        Returns:
            StructType: The schema of the referenced SnowparkDataFrame.
        """
        return self.snowpark_dataframe.schema

    @property
    def snowflake_quoted_identifiers(self) -> list[str]:
        """
        The snowflake quoted identifiers for all snowpark dataframe columns.
        """
        if self.cached_snowflake_quoted_identifiers_tuple is None:
            # if there is no cached snowflake quoted identifiers, reach to the
            # Snowpark schema to fetch the identifiers. Note this will trigger one query
            # describe call to server.
            self.cached_snowflake_quoted_identifiers_tuple = tuple(
                f.column_identifier.quoted_name for f in self.schema.fields
            )

        return list(self.cached_snowflake_quoted_identifiers_tuple)


def _raise_if_identifier_not_exists(
    identifiers: list[str], existing_identifiers: list[str], category: str = ""
) -> None:
    """Checks whether all elements in `identifiers` existing in `existing_identifiers`"""
    existing_identifiers_set = set(existing_identifiers)
    for identifier in identifiers:
        if not isinstance(identifier, str):
            raise AssertionError(
                f"Only column identifier with string type is allowed for {category}"
            )
        if identifier not in existing_identifiers_set:
            raise AssertionError(
                f"{category} {identifier} not found in {existing_identifiers}"
            )


class OrderedDataFrame:
    """
    A mutable class representing an ordered DataFrame with projection columns and ordering columns.
    It allows you to specify a DataFrameReference to reference the source Snowpark DataFrame,
    select projected columns, define ordering columns and the row position identifier. We only
    provide a minimal set of dataframe operation methods in this class, which are enough to implement
    pandas dataframe operations.

    Note:
        - This class maintains project columns and ordering columns, where some ordering columns may
          not be in project columns.
        - All identifiers used here are Snowflake quoted identifiers.
        - Currently, only select and ensure_row_position_column operation will not update
          the DataFrameReference inside the class.
        - "Order" or "Ordering" is defined as the order of the source Snowpark DataFrame, which may
          or may not exist. If not exist, we assign a default order that all projected columns will
          be used as ordering columns
        - Ordering semantics,
            - `select`, `filter`, `limit` and `dropna` don't change ordering columns.
            - `sort` will update ordering columns by the specified columns.
            - `group_by` will use the grouped columns as ordering columns.
            - `rename` will keep the original ordering columns with updated names.
            - `join` with `right` join method preserves the right frame order, followed by self/left
               frame order. For all other join methods, the result preserves the self/left frame order
               first, followed by the right frame order.
            - `align` sorts lexicographically on the align keys for `outer` method. For `left`, it
               preserves the left order first, and then followed by right order, same as join.
            - `union_all`, `pivot`, `unpivot` and `agg` uses the default order.
    """

    # A DataFrameReference object referencing the source Snowpark DataFrame.
    _dataframe_ref: DataFrameReference
    # The snowflake quoted identifiers for projected columns, which is represented as tuple of
    # string to ensure immutability. Note that the projected columns is a subset of snowflake quoted
    # identifiers of the underlying Snowpark DataFrame in _dataframe_ref.
    _projected_column_snowflake_quoted_identifiers_tuple: tuple[str, ...]
    # a tuple of OrderingColumn objects that defines the order of the current OrderedDataFrame
    # It must be part of projected columns
    _ordering_columns_tuple: tuple[OrderingColumn, ...]
    # row position snowflake quoted identifier
    row_position_snowflake_quoted_identifier: Optional[str]
    # row count snowflake quoted identifier
    row_count_snowflake_quoted_identifier: Optional[str]

    def __init__(
        self,
        dataframe_ref: DataFrameReference,
        *,
        projected_column_snowflake_quoted_identifiers: Optional[list[str]] = None,
        ordering_columns: Optional[list[OrderingColumn]] = None,
        row_position_snowflake_quoted_identifier: Optional[str] = None,
        row_count_snowflake_quoted_identifier: Optional[str] = None,
    ) -> None:
        self._dataframe_ref = dataframe_ref

        all_existing_snowflake_quoted_identifiers = (
            self._dataframe_ref.snowflake_quoted_identifiers
        )

        # If projected_columns is not specified, it will be all columns in the source Snowpark DataFrame
        if projected_column_snowflake_quoted_identifiers:
            _raise_if_identifier_not_exists(
                projected_column_snowflake_quoted_identifiers,
                all_existing_snowflake_quoted_identifiers,
                "projected column",
            )
            self._projected_column_snowflake_quoted_identifiers_tuple = tuple(
                projected_column_snowflake_quoted_identifiers
            )
        else:
            self._projected_column_snowflake_quoted_identifiers_tuple = tuple(
                all_existing_snowflake_quoted_identifiers
            )
        # If ordering_columns is not specified, all projected columns will be used as
        # ordering columns by default.
        # Note that an ordering column may not be in projected columns,
        # and only need to be in the source Snowpark dataframe
        if ordering_columns is not None:
            _raise_if_identifier_not_exists(
                [column.snowflake_quoted_identifier for column in ordering_columns],
                all_existing_snowflake_quoted_identifiers,
                "ordering column",
            )
            self._ordering_columns_tuple = tuple(ordering_columns)
        else:
            self._ordering_columns_tuple = tuple(
                OrderingColumn(column)
                for column in self._projected_column_snowflake_quoted_identifiers_tuple
            )
        assert self.ordering_columns, "ordering_columns cannot be empty"
        if row_position_snowflake_quoted_identifier:
            _raise_if_identifier_not_exists(
                [row_position_snowflake_quoted_identifier],
                all_existing_snowflake_quoted_identifiers,
                "row position column",
            )
        if row_count_snowflake_quoted_identifier:
            _raise_if_identifier_not_exists(
                [row_count_snowflake_quoted_identifier],
                all_existing_snowflake_quoted_identifiers,
                "row count column",
            )
        self.row_position_snowflake_quoted_identifier = (
            row_position_snowflake_quoted_identifier
        )
        self.row_count_snowflake_quoted_identifier = (
            row_count_snowflake_quoted_identifier
        )

    @property
    def ordering_columns(self) -> list[OrderingColumn]:
        return list(self._ordering_columns_tuple)

    def _ordering_snowpark_columns(self) -> list[Column]:
        """
        Returns a list of SnowparkColumns that can be applied to the snowpark
        dataframe to derive the ordered result.

        Returns:
            List of SnowparkColumn
        """
        return [col.snowpark_column for col in self.ordering_columns]

    @property
    def ordering_column_snowflake_quoted_identifiers(self) -> list[str]:
        """
        Get all snowflake quoted identifiers for the ordering columns.

        Return:
            List of snowflake quoted identifier for the ordering columns
        """
        return [col.snowflake_quoted_identifier for col in self.ordering_columns]

    def _row_position_snowpark_column(self) -> Column:
        """
        Returns a row position Snowpark column for the dataframe.
        If row position column already exist in the current dataframe, it will be directly returned.
        Otherwise, the row position column will be generated based on the ordering columns.

        Return:
            SnowparkColumn to get the row position column.
        """

        if self.row_position_snowflake_quoted_identifier is not None:
            return Column(self.row_position_snowflake_quoted_identifier)

        return row_number().over(Window.order_by(self._ordering_snowpark_columns())) - 1

    @property
    def projected_column_snowflake_quoted_identifiers(self) -> list[str]:
        """
        Returns:
            List of snowflake quoted identifiers for the projected columns of the current ordered dataframe.
        """
        return list(self._projected_column_snowflake_quoted_identifiers_tuple)

    def ensure_row_position_column(self) -> "OrderedDataFrame":
        """
        Returns an OrderedDataFrame with a row position column, and the row position column is guaranteed to
        be part of the projected column.
        If self.row_position_snowflake_quoted_identifier is not None, there is already a row position column,
        no new row position column will be generated.
        Note that the returned OrderedDataframe retains the original ordering columns.
        """
        if self.row_position_snowflake_quoted_identifier is not None:
            # if the row position column is not part of the projected columns, do a select to make sure
            # the row position column becomes part of the projected columns.
            if (
                self.row_position_snowflake_quoted_identifier
                not in self.projected_column_snowflake_quoted_identifiers
            ):
                return self.select(
                    self.projected_column_snowflake_quoted_identifiers
                    + [self.row_position_snowflake_quoted_identifier]
                )
            else:
                return self

        from snowflake.snowpark.modin.plugin._internal.utils import (
            ROW_POSITION_COLUMN_LABEL,
        )

        row_position_snowflake_quoted_identifier = (
            self.generate_snowflake_quoted_identifiers(
                pandas_labels=[ROW_POSITION_COLUMN_LABEL],
                wrap_double_underscore=True,
            )[0]
        )
        ordered_dataframe = self.select(
            *self.projected_column_snowflake_quoted_identifiers,
            self._row_position_snowpark_column().as_(
                row_position_snowflake_quoted_identifier
            ),
        )
        # inplace update so dataframe_ref can be shared. Note that we keep
        # the original ordering columns.
        ordered_dataframe.row_position_snowflake_quoted_identifier = (
            row_position_snowflake_quoted_identifier
        )
        return ordered_dataframe

    def ensure_row_count_column(self) -> "OrderedDataFrame":
        """
        Returns an OrderedDataFrame with a row count column, and the row count column is guaranteed to
        be part of the projected column.
        If self.row_count_snowflake_quoted_identifier is not None, there is already a row count column,
        no new row count column will be generated.
        Note that the returned OrderedDataframe retains the original ordering columns.
        """
        if self.row_count_snowflake_quoted_identifier is not None:
            # if the row count column is not part of the projected columns, do a select to make sure
            # the row count column becomes part of the projected columns.
            if (
                self.row_count_snowflake_quoted_identifier
                not in self.projected_column_snowflake_quoted_identifiers
            ):
                return self.select(
                    self.projected_column_snowflake_quoted_identifiers
                    + [self.row_count_snowflake_quoted_identifier]
                )
            else:
                return self

        from snowflake.snowpark.modin.plugin._internal.utils import (
            ROW_COUNT_COLUMN_LABEL,
        )

        row_count_snowflake_quoted_identifier = (
            self.generate_snowflake_quoted_identifiers(
                pandas_labels=[ROW_COUNT_COLUMN_LABEL],
                wrap_double_underscore=True,
            )[0]
        )
        ordered_dataframe = self.select(
            *self.projected_column_snowflake_quoted_identifiers,
            count("*").over().as_(row_count_snowflake_quoted_identifier),
        )
        # inplace update so dataframe_ref can be shared. Note that we keep
        # the original ordering columns.
        ordered_dataframe.row_count_snowflake_quoted_identifier = (
            row_count_snowflake_quoted_identifier
        )
        return ordered_dataframe

    def generate_snowflake_quoted_identifiers(
        self,
        *,
        pandas_labels: list[Hashable],
        excluded: Optional[list[str]] = None,
        wrap_double_underscore: Optional[bool] = False,
    ) -> list[str]:
        """
        See detailed docstring of generate_snowflake_quoted_identifiers_helper in
        snowflake/snowpark/modin/plugin/_internal/utils.
        The only difference between this method and generate_snowflake_quoted_identifiers_helper is
        that all snowflake quoted identifier of the underlying snowpark dataframe in the dataframe_ref
        will be added to `excluded`.
        """
        from snowflake.snowpark.modin.plugin._internal.utils import (
            generate_snowflake_quoted_identifiers_helper,
        )

        if not excluded:
            excluded = []
        existing_identifiers = self._dataframe_ref.snowflake_quoted_identifiers
        return generate_snowflake_quoted_identifiers_helper(
            pandas_labels=pandas_labels,
            excluded=excluded + existing_identifiers,
            wrap_double_underscore=wrap_double_underscore,
        )

    @property
    def schema(self) -> StructType:
        """Get the schema of OrderedDataFrame. It only includes the schema of projected columns."""
        quoted_identifier_to_field_mapping = {
            field.column_identifier.quoted_name: field
            for field in self._dataframe_ref.schema.fields
        }
        return StructType(
            [
                quoted_identifier_to_field_mapping[identifier]
                for identifier in self.projected_column_snowflake_quoted_identifiers
                if identifier in quoted_identifier_to_field_mapping
            ]
        )

    @property
    def queries(self) -> dict[str, list[str]]:
        """Get underlying SQL queries of an OrderedDataFrame."""
        return self._dataframe_ref.snowpark_dataframe.queries

    @property
    def session(self) -> Session:
        """Returns a Snowpark session object associated with this ordered dataframe."""
        return self._dataframe_ref.snowpark_dataframe.session

    def _get_active_column_snowflake_quoted_identifiers(
        self,
        include_ordering_columns: bool = True,
        include_row_position_column: bool = True,
        include_row_count_column: bool = True,
    ) -> list[str]:
        """
        Get the snowflake quoted identifiers for columns that are active for the ordering dataframe.
        The columns that are active for an ordering dataframe includes the projected columns, ordering columns
        and row position column if exists, ordering columns and row position column can be excluded based on
        configuration.

        Args:
            include_ordering_columns: whether include the snowflake quoted identifiers for the ordering columns in the result
            include_row_position_column: whether include the snowflake quoted identifier for the row position column in the result
            include_row_count_column: whether or not to include the snowflake quoted identifier for the row count column in the result

        Returns:
            List of the snowflake quoted identifiers for the in use columns.

        """

        column_quoted_identifiers = self.projected_column_snowflake_quoted_identifiers
        if include_ordering_columns:
            extra_ordering_column_quoted_identifiers = [
                quoted_identifier
                for quoted_identifier in self.ordering_column_snowflake_quoted_identifiers
                if quoted_identifier not in column_quoted_identifiers
            ]
            column_quoted_identifiers.extend(extra_ordering_column_quoted_identifiers)
        if (
            include_row_position_column
            and self.row_position_snowflake_quoted_identifier is not None
        ):
            if (
                self.row_position_snowflake_quoted_identifier
                not in column_quoted_identifiers
            ):
                column_quoted_identifiers.append(
                    self.row_position_snowflake_quoted_identifier
                )
        if (
            include_row_count_column
            and self.row_count_snowflake_quoted_identifier is not None
        ):
            if (
                self.row_count_snowflake_quoted_identifier
                not in column_quoted_identifiers
            ):
                column_quoted_identifiers.append(
                    self.row_count_snowflake_quoted_identifier
                )

        return column_quoted_identifiers

    def _extract_quoted_identifiers_from_column_or_name(
        self, col: ColumnOrName
    ) -> list[str]:
        """
        Extract the snowflake quoted identifiers out of a Column or column name with following rule:
        1) when it is a Snowpark column, only column with alias name can be handled, the alias name is
            extracted as the quoted identifier.
        2) when it is a str
            a) if it is a start (*), then all projected columns snowflake quoted identifiers are added
            b) otherwise, it is treated as a name of existing column, and only active columns of the current
                ordered dataframe can be used.
        e) AssertionError is raised for all cases can not be handled.
        """
        from snowflake.snowpark.modin.plugin._internal.utils import (
            is_valid_snowflake_quoted_identifier,
        )

        if isinstance(col, Column):
            if col._expression.pretty_name == "ALIAS":
                column_name = col._named().name
                assert is_valid_snowflake_quoted_identifier(
                    column_name
                ), f"Invalid snowflake quoted identifier {column_name} used"
                return [column_name]
            else:
                raise AssertionError(
                    f"Column {col} is invalid, only column with alias name is allowed!"
                )
        elif isinstance(col, str):
            if col == "*":
                # star adds all projected columns to the result
                return self.projected_column_snowflake_quoted_identifiers
            else:
                active_columns = self._get_active_column_snowflake_quoted_identifiers()
                if col in active_columns:
                    return [col]
                else:
                    raise AssertionError(
                        f"Column {col} is not in active columns {active_columns}"
                    )
        else:
            raise AssertionError(
                f"Can not extract name for {col}, only Column with alias name or str can be handled!"
            )

    def select(
        self,
        *cols: Union[
            Union[ColumnOrName, TableFunctionCall],
            Iterable[Union[ColumnOrName, TableFunctionCall]],
        ],
    ) -> "OrderedDataFrame":
        """
        Returns a DataFrame by selecting the specified columns.
        Any existing columns that are not in `cols` will be still retained in the
        source dataframe reference/snowpark DataFrame (unless `cols` contains TableFunctionCall object),
        which will be shared across multiple ordered DataFrames.
        Note that ordering columns will not be changed.

        See detailed docstring in Snowpark DataFrame's select. Compared with Snowpark DataFrame's select,
        this select has the following restrictions:
        1. To select an existing column, must use column name instead of column expression. For example:
            select(col("a")) is not allowed, but select("a") is allowed. Note: only existing active columns
            can be selected, which includes projected columns, ordering columns, row position column and
            row_count column.
        2. if you want to select a Column object, it must have an alias.
        3. You can't select an aggregated column anymore (e.g., `max("a").as_("a")`).
           To select an aggregated column, use `agg()`.
        """
        exprs = parse_positional_args_to_list(*cols)
        assert exprs, "The input of select() cannot be empty"

        # a list of new Column objects to be selected for new OrderedDataFrame
        new_snowpark_column_objects: list[Column] = []
        # a list of snowflake quoted identifiers as projected columns for new OrderedDataFrame
        new_projected_columns: list[str] = []
        for e in exprs:
            if isinstance(e, TableFunctionCall):
                # we couldn't handle TableFunctionCall, so just use the original select
                snowpark_dataframe = self._dataframe_ref.snowpark_dataframe.select(
                    *cols
                )
                return OrderedDataFrame(DataFrameReference(snowpark_dataframe))
            elif isinstance(e, (Column, str)):
                column_names = self._extract_quoted_identifiers_from_column_or_name(e)
                new_projected_columns.extend(column_names)
                if isinstance(e, Column):
                    new_snowpark_column_objects.append(e)
            else:
                raise AssertionError(
                    "Only columns with alias name, column names and table functions are accepted"
                )

        dataframe_ref = self._dataframe_ref
        if len(new_snowpark_column_objects) > 0:
            existing_columns = self._dataframe_ref.snowflake_quoted_identifiers
            # this is an inplace update of the snowpark dataframe so it can be shared
            dataframe_ref.snowpark_dataframe = dataframe_ref.snowpark_dataframe.select(
                existing_columns + new_snowpark_column_objects
            )
            # update the managed quoted identifiers for the dataframe reference to the new set
            # of identifiers.
            new_column_identifiers = existing_columns + [
                quoted_identifier
                for quoted_identifier in new_projected_columns
                if quoted_identifier not in existing_columns
            ]
            dataframe_ref.cached_snowflake_quoted_identifiers_tuple = tuple(
                new_column_identifiers
            )
            logging.debug(
                f"The Snowpark DataFrame in DataFrameReference with id={dataframe_ref._id} is updated"
            )

        return OrderedDataFrame(
            dataframe_ref,
            projected_column_snowflake_quoted_identifiers=new_projected_columns,
            # keep the original ordering columns and row position column
            ordering_columns=self.ordering_columns,
            row_position_snowflake_quoted_identifier=self.row_position_snowflake_quoted_identifier,
            row_count_snowflake_quoted_identifier=self.row_count_snowflake_quoted_identifier,
        )

    def dropna(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Iterable[str]]] = None,
    ) -> "OrderedDataFrame":
        """
        Returns a new DataFrame that excludes all rows containing fewer than
        a specified number of non-null and non-NaN values in the specified
        columns. Note that ordering columns will not be changed.

        See detailed docstring in Snowpark DataFrame's dropna.
        """

        projected_dataframe_ref = self._to_projected_snowpark_dataframe_reference(
            include_ordering_columns=True
        )
        snowpark_dataframe = projected_dataframe_ref.snowpark_dataframe.dropna(
            how=how, thresh=thresh, subset=subset
        )
        # dropna doesn't change the column quoted identifiers
        result_column_quoted_identifiers = (
            projected_dataframe_ref.snowflake_quoted_identifiers
        )
        return OrderedDataFrame(
            DataFrameReference(snowpark_dataframe, result_column_quoted_identifiers),
            projected_column_snowflake_quoted_identifiers=result_column_quoted_identifiers,
            ordering_columns=self.ordering_columns,
        )

    def union_all(self, other: "OrderedDataFrame") -> "OrderedDataFrame":
        """
        Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), including any duplicate rows. Both input
        DataFrames must contain the same number of columns.
        Note that the ordering columns will become all columns in the DataFrame.
        TODO SNOW-966319: set ordering columns for union_all

        See detailed docstring in Snowpark DataFrame's union_all.
        """
        self_snowpark_dataframe_ref = self._to_projected_snowpark_dataframe_reference()
        other_snowpark_dataframe_ref = (
            other._to_projected_snowpark_dataframe_reference()
        )
        # union all result uses the snowflake quoted identifiers of self snowpark dataframe
        result_column_quoted_identifiers = (
            self_snowpark_dataframe_ref.snowflake_quoted_identifiers
        )
        snowpark_dataframe = self_snowpark_dataframe_ref.snowpark_dataframe.union_all(
            other_snowpark_dataframe_ref.snowpark_dataframe
        )
        return OrderedDataFrame(
            DataFrameReference(snowpark_dataframe, result_column_quoted_identifiers),
            projected_column_snowflake_quoted_identifiers=result_column_quoted_identifiers,
        )

    def _extract_aggregation_result_column_quoted_identifiers(
        self,
        *agg_exprs: ColumnOrName,
    ) -> list[str]:
        """
        Extract the quoted identifiers for the aggregation columns.
        """
        exprs = parse_positional_args_to_list(*agg_exprs)
        # extract the aggregation function name
        result_column_quoted_identifiers: list[str] = []
        for e in exprs:
            if isinstance(e, (Column, str)):
                column_names = self._extract_quoted_identifiers_from_column_or_name(e)
                result_column_quoted_identifiers.extend(column_names)
            else:
                raise AssertionError(
                    "Only column name or column expression with alias name can be used as aggregation expression"
                )

        return result_column_quoted_identifiers

    def group_by(
        self,
        cols: Iterable[str],
        *agg_exprs: ColumnOrName,
    ) -> "OrderedDataFrame":
        """
        Groups rows by the columns specified by expressions (similar to GROUP BY in
        SQL), which are followed by aggregations.
        Note that the ordering columns will become `cols`.

        Args:
            cols: A list of Snowflake quoted identifiers for group by. We don't allow Column objects
                here, which is different from Snowpark DataFrame's group_by
            agg_exprs: aggregation expressions

        See detailed docstring in Snowpark DataFrame's group_by.
        """
        # the result columns for groupby aggregate are the groupby columns + the aggregation columns
        result_column_quoted_identifiers: list[str] = [
            identifier for identifier in cols
        ]  # add the groupby columns
        # add the aggregation columns
        result_column_quoted_identifiers += (
            self._extract_aggregation_result_column_quoted_identifiers(*agg_exprs)
        )

        return OrderedDataFrame(
            DataFrameReference(
                self._dataframe_ref.snowpark_dataframe.group_by(cols).agg(*agg_exprs),
                snowflake_quoted_identifiers=result_column_quoted_identifiers,
            ),
            projected_column_snowflake_quoted_identifiers=result_column_quoted_identifiers,
            ordering_columns=[OrderingColumn(identifier) for identifier in cols],
        )

    def sort(
        self,
        *cols: Union[OrderingColumn, Iterable[OrderingColumn]],
    ) -> "OrderedDataFrame":
        """
        Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).
        Note that this is not a real "sort", but just set the ordering columns.
        """
        # parse cols to get the new Ordering columns
        ordering_columns = parse_positional_args_to_list(*cols)

        # check all ordering_columns are in the current active columns
        _raise_if_identifier_not_exists(
            [column.snowflake_quoted_identifier for column in ordering_columns],
            self._get_active_column_snowflake_quoted_identifiers(),
            "ordering column",
        )

        # check if the column is the same as the ordering column now
        if ordering_columns == self.ordering_columns:
            return self

        return OrderedDataFrame(
            self._to_projected_snowpark_dataframe_reference(
                include_row_count_column=True
            ),
            projected_column_snowflake_quoted_identifiers=self.projected_column_snowflake_quoted_identifiers,
            ordering_columns=ordering_columns,
            # should reset the row position column since ordering is updated
            row_position_snowflake_quoted_identifier=None,
            # No need to reset row count, since sorting should not add/drop rows.
            row_count_snowflake_quoted_identifier=self.row_count_snowflake_quoted_identifier,
        )

    def pivot(
        self,
        pivot_col: ColumnOrName,
        values: Optional[Union[Iterable[LiteralType]]] = None,
        default_on_null: Optional[LiteralType] = None,
        *agg_exprs: Union[Column, tuple[ColumnOrName, str], dict[str, str]],
    ) -> "OrderedDataFrame":
        """
        Rotates this DataFrame by turning the unique values from one column in the input
        expression into multiple columns and aggregating results (followed by `agg_exprs`)
        where required on any remaining column values.
        Note that the ordering columns will become all columns in the DataFrame.

        See detailed docstring in Snowpark DataFrame's pivot.
        """
        snowpark_dataframe = self.to_projected_snowpark_dataframe()
        return OrderedDataFrame(
            # the pivot result columns for dynamic pivot are data dependent, a schema call is required
            # to know all the quoted identifiers for the pivot result.
            DataFrameReference(
                snowpark_dataframe.pivot(
                    pivot_col=pivot_col,
                    values=values,
                    default_on_null=default_on_null,
                ).agg(*agg_exprs)
            )
        )

    def unpivot(
        self,
        value_column: str,
        name_column: str,
        column_list: list[str],
        col_mapper: Optional[dict[str, str]] = None,
    ) -> "OrderedDataFrame":
        """
        Rotates a table by transforming columns into rows.
        UNPIVOT is a relational operator that accepts two columns (from a table or subquery),
        along with a list of columns, and generates a row for each column specified in the list.
        Note that the ordering columns will become all columns in the DataFrame.

        Prior to the unpivot the dataframe reference will be updated to avoid ambigious column
        issues. An optional map, column_list_name_map can be used to map temporary column names
        back to the expected column names used for the variable column in unpivot.

        Args:
            value_column: name of the value column, typically "values"
            name_column: name of the variable column, containing the column names from the unpivot
            column_list: list of columns to unpivot
            col_mapper: a mapping between the current column_list names and the desired
               column names which would be used in the new name_column

        """
        # check columns in column_list are in projected columns
        _raise_if_identifier_not_exists(
            [quoted_identifier for quoted_identifier in column_list],
            self.projected_column_snowflake_quoted_identifiers,
            "unpivot column list",
        )
        # Apply a select to project only the desired columns, and return a non-sharable
        # dataframe reference because unpivot is destructive to the types of alignment/join
        # optimizations intended.
        projected_dataframe_ref = self._to_projected_snowpark_dataframe_reference(
            col_mapper=col_mapper
        )
        unpivot_column_list = column_list
        # Remap the columns from the projection back to the desired names. This is
        # particularly important if the projection has applied transformations to the
        # columns to normalize the data.
        if col_mapper is not None:
            unpivot_column_list = []
            for col in column_list:
                if col in col_mapper:
                    unpivot_column_list.append(col_mapper[col])
                else:
                    unpivot_column_list.append(col)

        # all columns other than the unpivot columns are retained in the result
        result_column_quoted_identifiers = [
            quoted_identifier
            for quoted_identifier in projected_dataframe_ref.snowflake_quoted_identifiers
            if quoted_identifier not in unpivot_column_list
        ]
        # add the name column and value colum to the result
        result_column_quoted_identifiers += [name_column, value_column]
        return OrderedDataFrame(
            DataFrameReference(
                projected_dataframe_ref.snowpark_dataframe.unpivot(
                    value_column=value_column,
                    name_column=name_column,
                    column_list=unpivot_column_list,
                ),
                snowflake_quoted_identifiers=result_column_quoted_identifiers,
            ),
            projected_column_snowflake_quoted_identifiers=result_column_quoted_identifiers,
        )

    def agg(
        self,
        *exprs: ColumnOrName,
    ) -> "OrderedDataFrame":
        """
        Aggregates the data in the DataFrame. Use this method if you don't need to
        group the data. Note that the ordering columns will become all columns in the DataFrame.

        See detailed docstring in Snowpark DataFrame's agg.
        """
        snowpark_dataframe = self._dataframe_ref.snowpark_dataframe.agg(*exprs)
        result_column_quoted_identifiers = (
            self._extract_aggregation_result_column_quoted_identifiers(*exprs)
        )
        return OrderedDataFrame(
            DataFrameReference(snowpark_dataframe, result_column_quoted_identifiers),
            projected_column_snowflake_quoted_identifiers=result_column_quoted_identifiers,
        )

    def _deduplicate_active_column_snowflake_quoted_identifiers(
        self,
        against: "OrderedDataFrame",
        include_ordering_columns: bool = True,
        include_row_position_column: bool = True,
        include_row_count_column: bool = True,
    ) -> "OrderedDataFrame":
        """
        Deduplicate all active column snowflake quoted identifiers of the current OrderedDataFrame against the
        given OrderedDataFrame. The active columns involve projected columns, ordering columns and row position column.
        For example:
            current dataframe has projected columns = ['"A"', '"B"', '"C"'], ordering columns = ['"D"', '"A"'], and
                row position column = ['"row_pos"']
            and given ordered dataframe to de-duplicate against has
                projected columns = ['"A"', '"B"', '"E"'], ordering columns = ['"A"', '"E"'], and
                row position column = ['"row_pos"']
            After deduplication, the result ordered dataframe will have
                projected_columns = ['"A_<suffix>"', '"B_<suffix>"', '"C"'], ordering columns = ['"D"', '"A_<suffix>"'],
                and row position column = ['"row_pos_<suffix>"']
            Where column '"A"', '"B"' and '"row_pos"' is deduplicated since they conflict with the columns in self.

        Args:
            against: The OrderedDataFrame to deduplicate
            include_ordering_columns: Whether to include the ordering columns during deduplication.
            include_row_position_column: Whether to include the row position column during deduplication.
            include_row_count_column: Whether to include the row count column during deduplication.

        Returns:
            An OrderedDataFrame with ordering and projected columns properly deduplicated
        """
        from snowflake.snowpark.modin.plugin._internal.utils import (
            unquote_name_if_quoted,
        )

        quoted_identifiers_to_deduplicate = (
            self._get_active_column_snowflake_quoted_identifiers(
                include_ordering_columns=include_ordering_columns,
                include_row_position_column=include_row_position_column,
                include_row_count_column=include_row_count_column,
            )
        )
        quoted_identifiers_to_deduplicate_against = (
            against._get_active_column_snowflake_quoted_identifiers(
                include_ordering_columns=include_ordering_columns,
                include_row_position_column=include_row_position_column,
                include_row_count_column=include_row_count_column,
            )
        )
        original_projected_columns_quoted_identifiers = (
            self.projected_column_snowflake_quoted_identifiers
        )

        deduplicated_quoted_identifiers: list[str] = []
        deduplicated_columns: list[Column] = []
        for quoted_identifier in quoted_identifiers_to_deduplicate:
            # if it conflicts with the left identifier, alias the column to a new name
            if quoted_identifier in quoted_identifiers_to_deduplicate_against:
                unquoted_name = unquote_name_if_quoted(quoted_identifier)
                deduplicated_quoted_identifier = (
                    self.generate_snowflake_quoted_identifiers(
                        pandas_labels=[unquoted_name],
                        excluded=quoted_identifiers_to_deduplicate_against
                        + deduplicated_quoted_identifiers,
                    )[0]
                )
                deduplicated_column = Column(quoted_identifier).as_(
                    deduplicated_quoted_identifier
                )
            else:
                # Note here we use the column name directly, because column expression with no alias name is not
                # allowed for the select method in ordered dataframe, and column name is used to retain
                # original columns.
                deduplicated_column = quoted_identifier
                deduplicated_quoted_identifier = quoted_identifier
            deduplicated_columns.append(deduplicated_column)
            deduplicated_quoted_identifiers.append(deduplicated_quoted_identifier)

        column_identifiers_rename_map = dict(
            zip(quoted_identifiers_to_deduplicate, deduplicated_quoted_identifiers)
        )

        deduplicated_ordered_frame = self.select(deduplicated_columns)

        new_projected_columns_quoted_identifiers = [
            column_identifiers_rename_map[quoted_identifier]
            for quoted_identifier in original_projected_columns_quoted_identifiers
        ]
        # get the ordering columns for the right frame after rename
        new_ordering_columns = [
            OrderingColumn(
                column_identifiers_rename_map.get(
                    order_col.snowflake_quoted_identifier,
                    order_col.snowflake_quoted_identifier,
                ),
                order_col.ascending,
                order_col.na_last,
            )
            for order_col in self.ordering_columns
        ]

        new_row_position_snowflake_quoted_identifier = (
            column_identifiers_rename_map.get(
                self.row_position_snowflake_quoted_identifier,
                self.row_position_snowflake_quoted_identifier,
            )
            if self.row_position_snowflake_quoted_identifier
            else None
        )

        new_row_count_snowflake_quoted_identifier = (
            column_identifiers_rename_map.get(
                self.row_count_snowflake_quoted_identifier,
                self.row_count_snowflake_quoted_identifier,
            )
            if self.row_count_snowflake_quoted_identifier
            else None
        )

        return OrderedDataFrame(
            dataframe_ref=deduplicated_ordered_frame._dataframe_ref,
            projected_column_snowflake_quoted_identifiers=new_projected_columns_quoted_identifiers,
            ordering_columns=new_ordering_columns,
            row_position_snowflake_quoted_identifier=new_row_position_snowflake_quoted_identifier,
            row_count_snowflake_quoted_identifier=new_row_count_snowflake_quoted_identifier,
        )

    def join(
        self,
        right: "OrderedDataFrame",
        left_on_cols: Optional[list[str]] = None,
        right_on_cols: Optional[list[str]] = None,
        how: JoinTypeLit = "inner",
    ) -> "OrderedDataFrame":
        """
        Performs equi join of the specified type (``how``) with the current
        DataFrame and another DataFrame (``right``) on a list of columns from left
        and right OrderedDataFrame(``left_on`` and ``right_on``). Proper de-conflicting
        happens on the right OrderedDataFrame columns to make sure there is no conflicting
        column names between self and right.

        ** NOTE that EQUAL_NULL is used as equality comparison instead of regular EQUAL comparison
        operator(==), since this is use case in Snowpark pandas **

        Args:
            right: The other OrderedDataFrame to join.
            left_on_cols: A list of column names from self OrderedDataFrame to be used for the join.
            right_on_cols: A list of column names from right OrderedDataFrame to be used for the join.
            how: We support the following join types:
                - Inner join: "inner" (the default value)
                - Left outer join: "left"
                - Right outer join: "right"
                - Full outer join: "outer"
                - Cross join: "cross"

            ** NOTE:
                1) the length of left_on_cols and right_on_cols are required to be the same. If no left_on_cols
                   and right_on_columns is provided, the join is performed with no join on expression, should be only
                   used by cross join.
                2) This interface is not the same as the interface provided by Snowpark dataframe, which allow arbitrary
                   on expression. We restrict the support to only equvi join in ordered dataframe is because eqvi join
                   is more efficient and which is the only required usage for now. Consider to support general join on
                   expression in the future if needed.
                3) when the join is a self-join on the row position columns, the join is skipped and
                   we just select new de-duplicated columns from the right dataframe. The ordering columns
                   and position columns of left dataframe are used for the result ordered dataframe.

        Return:
            An OrderedDataFrame representation of the join result with the following property
                1) Projected columns in the order of left projected columns + right projected columns
                2) For right outer join, the ordering columns preserves the right order, followed by left order.
                    For other join methods, the ordering columns preserves the left order, followed by right order.

        """
        left_on_cols = left_on_cols or []
        right_on_cols = right_on_cols or []
        assert len(left_on_cols) == len(
            right_on_cols
        ), "left_on_cols and right_on_cols must be of same length"
        _raise_if_identifier_not_exists(
            left_on_cols,
            self.projected_column_snowflake_quoted_identifiers,
            "join left_on_cols",
        )

        _raise_if_identifier_not_exists(
            right_on_cols,
            right.projected_column_snowflake_quoted_identifiers,
            "join right_on_cols",
        )

        is_join_needed = True
        # join is not needed for `left`, `right`, `inner` and `outer` join for self join
        # on row position column since row position column is a unique column.
        if how in [
            "left",
            "right",
            "inner",
            "outer",
        ] and self._is_self_join_on_row_position_column(
            left_on_cols, right, right_on_cols
        ):
            is_join_needed = False

        original_right_quoted_identifiers = (
            right.projected_column_snowflake_quoted_identifiers
        )
        # De-duplicate the column identifiers of right against self (left), so that
        # Snowpark doesn't perform any de-duplication on the result dataframe during join.
        right = right._deduplicate_active_column_snowflake_quoted_identifiers(
            self,
            include_row_position_column=False,
            include_row_count_column=False,
        )
        new_right_quoted_identifiers = (
            right.projected_column_snowflake_quoted_identifiers
        )

        # the new projected columns are set to self._projected_columns + right._projected_column after de-conflict
        projected_column_snowflake_quoted_identifiers = (
            self.projected_column_snowflake_quoted_identifiers
            + right.projected_column_snowflake_quoted_identifiers
        )

        if not is_join_needed:
            # if no join needed, we simply return the deduplicated right frame with the projected columns
            # set to the left.projected_column_snowflake_quoted_identifiers and the deduplicated the right
            # projected_column_snowflake_quoted_identifiers.
            return OrderedDataFrame(
                right._dataframe_ref,
                projected_column_snowflake_quoted_identifiers=projected_column_snowflake_quoted_identifiers,
                ordering_columns=self.ordering_columns,
                row_position_snowflake_quoted_identifier=self.row_position_snowflake_quoted_identifier,
                row_count_snowflake_quoted_identifier=self.row_count_snowflake_quoted_identifier,
            )

        # reproject the snowpark dataframe with only necessary columns
        left_snowpark_dataframe_ref = self._to_projected_snowpark_dataframe_reference(
            include_ordering_columns=True
        )
        right_snowpark_dataframe_ref = right._to_projected_snowpark_dataframe_reference(
            include_ordering_columns=True
        )

        right_identifiers_rename_map = dict(
            zip(original_right_quoted_identifiers, new_right_quoted_identifiers)
        )
        # get the new mapped right on identifier
        right_on_cols = [right_identifiers_rename_map[key] for key in right_on_cols]

        # Generate sql ON clause 'EQUAL_NULL(col1, col2) and EQUAL_NULL(col3, col4) ...'
        on = None
        for left_col, right_col in zip(left_on_cols, right_on_cols):
            eq = Column(left_col).equal_null(Column(right_col))
            on = eq if on is None else on & eq

        # If we are doing a cross join, `on` cannot be specified.
        if how != "cross":
            snowpark_dataframe = left_snowpark_dataframe_ref.snowpark_dataframe.join(
                right_snowpark_dataframe_ref.snowpark_dataframe, on, how
            )
        else:
            snowpark_dataframe = left_snowpark_dataframe_ref.snowpark_dataframe.join(
                right_snowpark_dataframe_ref.snowpark_dataframe, how=how
            )

        # for right join, we preserve the right order first, then left order.
        # for all join type, left order is preserved first, then right order.
        if how == "right":
            ordering_columns = right.ordering_columns + self.ordering_columns
        else:
            ordering_columns = self.ordering_columns + right.ordering_columns

        return OrderedDataFrame(
            DataFrameReference(
                snowpark_dataframe,
                # the result of join retains column quoted identifier of both left + right
                snowflake_quoted_identifiers=left_snowpark_dataframe_ref.snowflake_quoted_identifiers
                + right_snowpark_dataframe_ref.snowflake_quoted_identifiers,
            ),
            projected_column_snowflake_quoted_identifiers=projected_column_snowflake_quoted_identifiers,
            ordering_columns=ordering_columns,
        )

    def _has_same_base_ordered_dataframe(self, other: "OrderedDataFrame") -> bool:
        """
        Check if self has the same base ordered dataframe as `other`.

        Two OrderedDataFrame have the same base ordered dataframe if and only if they use the same dataframe reference
        and have the same ordering columns.
        """
        return (
            self._dataframe_ref == other._dataframe_ref
            and self.ordering_columns == other.ordering_columns
        )

    def _is_self_align_on_same_cols(
        self,
        left_on_cols: list[str],
        right: "OrderedDataFrame",
        right_on_cols: list[str],
    ) -> bool:
        """
        Check if the self (left) OrderedDataFrame and right OrderedDataFrame have the same base ordered dataframe,
        and align on the same columns.

        Args:
            left_on_cols: the align on columns for self (left).
            right: the right OrderedDataFrame for align.
            right_on_cols: the align on columns for right.

        Returns:
            True if self (left) and right shares the same Snowpark dataframe, and aligns on the same columns.
        """
        if self._has_same_base_ordered_dataframe(right):
            right_cols_to_verify = right_on_cols
            if (
                self.row_position_snowflake_quoted_identifier
                and right.row_position_snowflake_quoted_identifier
                and right.row_position_snowflake_quoted_identifier in right_on_cols
            ):
                # When the self (left) and the right has the same base ordered dataframe, the row position
                # column of left and right can be treated as the same column even if they have different
                # identifiers, because they are basically duplication of the row position column. For example:
                # with ordered dataframe df1 = df['"A"', '"B"'] and ordered dataframe df2 = df['"B"', '"C"'],
                # those two ordered dataframe are directly derived from the same ordered dataframe, and therefor
                # has the same base ordered dataframe. Then we generate a row position column for df1 and df2
                # separately and align on the row position columns of df1 and df2, the row position column of df1
                # and df2 should have the same value since df1 and df2 has the same based ordered dataframe (which
                # uses the same dataframe reference and ordering).
                # Replaces the right.row_position_snowflake_quoted_identifier in right_on_cols with
                # self.row_position_snowflake_quoted_identifier for later comparison.
                right_cols_to_verify = [
                    self.row_position_snowflake_quoted_identifier
                    if quoted_identifier
                    == right.row_position_snowflake_quoted_identifier
                    else quoted_identifier
                    for quoted_identifier in right_on_cols
                ]

            return left_on_cols == right_cols_to_verify
        return False

    def align(
        self,
        right: "OrderedDataFrame",
        left_on_cols: list[str],
        right_on_cols: list[str],
        how: AlignTypeLit = "outer",
    ) -> "OrderedDataFrame":
        """
        Performs align operation of the specified join type (``how``) with the current
        DataFrame and another DataFrame (``right``) on a list of columns from left
        and right OrderedDataFrame(``left_on`` and ``right_on``). Proper de-conflicting
        happens on the right OrderedDataFrame columns to make sure there is no conflicting
        column names between self and right.

        If the left_on_cols of left(self) exact matches with the right_on_cols of right,
        the align operation merges the frame row by row, which is the same effect as join
        on row position column. Otherwise, it exposes the same behavior as regular join with
        given method (`how`).

        Columns from two frames are considered `exact match` if both have exact same values in
        exact same order. For example, [1, 2, 1] and [1, 2, 1] is considered the same, but [1, 2, 1]
        and [1, 1, 2] are not considered as the same.

        For the final result order, it follows the following rule:
        1) If the left_on_cols of left(self) exact matches with the right_on_cols of right, the
            result ordered frame preserves the order from the original left frame. (Note, there
            is an optimization that when the alignment is a self align on the same columns, the
            ordering columns is set exactly the same as the left)
        2) Otherwise, the order depends on the methods:
            a) left: preserves the left order, followed by right order
            b) coalesce: same as left. preserves the left order, followed by right order
            c) outer: sort lexicographically on the union of left_on_cols and right_on_cols
        Note that the above ordering behavior matches the align ordering behavior of pandas also. This
        decision is made because the align operator is mainly driven by pandas usage at this moment,
        there is no requirement that the order for the operator in ordering dataframe must match pandas.

        The projected columns of aligned result is guaranteed to be in order of
        <self/left projected columns> + <right projected columns with de-duplicate>

        Note: A join is typically required for align operator, however, if the two ordered dataframe has the same
            base ordered dataframe and aligns on the same columns, no join needs to be performed.

        Args:
            right: right DataFrame.
            left_on_cols: A list of column names from self OrderedDataFrame to be used for align.
            right_on_cols: A list of column names from right OrderedDataFrame to be used for align.
            how: We support the following align/join types:
                - "outer": Full outer align (default value)
                - "left": Left outer align
                - "coalesce": If left frame is not empty perform left outer align
                  otherwise perform right outer align. When left frame is empty, the
                  left_on column is replaced with the right_on column in the result.
                  This method can only be used when left_on and right_on type are
                  compatible, otherwise an error will be thrown.
        Returns:
            Aligned OrderedDataFrame.
        """

        assert len(left_on_cols) == len(
            right_on_cols
        ), "left_on_cols and right_on_cols must be of same length"

        _raise_if_identifier_not_exists(
            left_on_cols,
            self.projected_column_snowflake_quoted_identifiers,
            "align left_on_cols",
        )

        _raise_if_identifier_not_exists(
            right_on_cols,
            right.projected_column_snowflake_quoted_identifiers,
            "align right_on_cols",
        )

        if self._is_self_align_on_same_cols(left_on_cols, right, right_on_cols):
            # when it is self align on the same columns, there is no need to perform join, we simply deduplicate
            # the right ordered dataframe to produce the necessary projected columns for the final result.
            aligned_ordered_frame = (
                right._deduplicate_active_column_snowflake_quoted_identifiers(
                    self,
                    include_ordering_columns=False,
                    include_row_position_column=False,
                    include_row_count_column=False,
                )
            )
            return OrderedDataFrame(
                dataframe_ref=aligned_ordered_frame._dataframe_ref,
                projected_column_snowflake_quoted_identifiers=self.projected_column_snowflake_quoted_identifiers
                + aligned_ordered_frame.projected_column_snowflake_quoted_identifiers,
                ordering_columns=self.ordering_columns,
                row_position_snowflake_quoted_identifier=self.row_position_snowflake_quoted_identifier,
                row_count_snowflake_quoted_identifier=self.row_count_snowflake_quoted_identifier,
            )

        from snowflake.snowpark.modin.plugin._internal.join_utils import (
            JoinOrAlignOrderedDataframeResultHelper,
        )
        from snowflake.snowpark.modin.plugin._internal.utils import (
            ORDERING_COLUMN_LABEL,
        )

        original_left_projected_column_quoted_identifiers = (
            self.projected_column_snowflake_quoted_identifiers
        )
        original_right_projected_column_quoted_identifiers = (
            right.projected_column_snowflake_quoted_identifiers
        )
        # generate row position column for self and right, which is needed for align on column equivalence check
        left = self.ensure_row_position_column()
        right = right.ensure_row_position_column()
        # perform outer join
        joined_ordered_frame = left.join(
            right,
            left_on_cols=left_on_cols,
            right_on_cols=right_on_cols,
            how="outer",
        )

        sort = False
        if how == "outer":
            sort = True
        result_helper = JoinOrAlignOrderedDataframeResultHelper(
            left,
            right,
            joined_ordered_frame,
            left_on_cols,
            right_on_cols,
            how=how,
            sort=sort,
        )
        # get the ordered dataframe with correct order based on sort
        joined_ordered_frame = result_helper.join_or_align_result
        # update left_on_cols and right_on_cols
        left_on_cols = result_helper.map_left_quoted_identifiers(left_on_cols)
        right_on_cols = result_helper.map_right_quoted_identifiers(right_on_cols)

        result_projected_column_snowflake_quoted_identifiers = (
            result_helper.map_left_quoted_identifiers(
                original_left_projected_column_quoted_identifiers
            )
            + result_helper.map_right_quoted_identifiers(
                original_right_projected_column_quoted_identifiers
            )
        )

        # we have called ensure_row_position_column for the left and right frame above to make sure a
        # row positions column is generated for the left and right frame. Therefore,
        # row_position_snowflake_quoted_identifier can not be None for the left and right frame.
        assert left.row_position_snowflake_quoted_identifier is not None
        assert right.row_position_snowflake_quoted_identifier is not None
        left_row_pos = Column(
            result_helper.map_left_quoted_identifiers(
                [left.row_position_snowflake_quoted_identifier]
            )[0]
        )
        right_row_pos = Column(
            result_helper.map_right_quoted_identifiers(
                [right.row_position_snowflake_quoted_identifier]
            )[0]
        )
        # We use over() expression over all the data in frame. This adds a new column
        # with count where all values are same.  This way we avoid triggering any eager
        # evaluation.
        # Coalesce with lit(0), otherwise row_cont will be NULL for empty frame.
        left_count = coalesce(max_(left_row_pos).over() + 1, lit(0))
        from snowflake.snowpark.modin.plugin._internal.utils import (
            ROW_COUNT_COLUMN_LABEL,
        )

        left_count_identifier = (
            joined_ordered_frame.generate_snowflake_quoted_identifiers(
                pandas_labels=[ROW_COUNT_COLUMN_LABEL + "_left"]
            )[0]
        )
        # Add count of left frame as column. This is used in filter condition for "left"
        # align
        left_count_column = Column(left_count_identifier)
        extra_columns_to_append = [left_count.as_(left_count_identifier)]
        # Coalesce with lit(0), otherwise row_cont will be NULL for empty frame.
        right_count = coalesce(max_(right_row_pos).over() + 1, lit(0))
        eq_row_pos_count = sum_(iff(left_row_pos == right_row_pos, 1, 0)).over()

        ordering_columns = joined_ordered_frame.ordering_columns
        # 'col_matching_expr' represents if left_on_cols is an exact match with right_on_cols.
        # Add this as new column to frame. Note that ALL values for this column are same.
        # It will be TRUE if left_on_cols matches with right_on_cols otherwise it will be FALSE.
        col_matching_identifier = (
            joined_ordered_frame.generate_snowflake_quoted_identifiers(
                pandas_labels=["col_matching"]
            )[0]
        )
        col_matching_expr = (
            (left_count == right_count) & (left_count == eq_row_pos_count)
        ).as_(col_matching_identifier)
        col_matching_column = Column(col_matching_identifier)
        extra_columns_to_append.append(col_matching_expr)

        # Define the final ordering column.
        # As we mentioned in docstring, when left_on_cols and right_on_cols matches, the left
        # and right frame is merged row by row with the original order, and the row order of
        # original frame is retained.
        # However, when left_on_cols and right_on_cols doesn't match, we need to sort lexicographically
        # on the join keys for `outer` align, and preserve left order followed by right order for `left` align.
        # This means the ordering column changes based on the result of column matching situation. Due
        # to lazy evaluation, we do not know the column matching situation util the query is evaluated.
        # In order to achieve this, we add a column 'ordering_col' which is set to left row position if
        # input frames have matching left_on_cols and right_on_cols, otherwise this will be set to constant
        # 1 (a dummy ordering column has no effect).
        # Note that this is only needed by `outer` methods because it needs to sort on join keys. For `left`,
        # preserve the left order followed by right order can give the correct order for both matching case
        # and non-matching case.
        if how == "outer":
            global_order_col_identifier = (
                joined_ordered_frame.generate_snowflake_quoted_identifiers(
                    pandas_labels=[ORDERING_COLUMN_LABEL],
                    excluded=[col_matching_identifier],
                )[0]
            )
            global_order_expr = iff(col_matching_column, left_row_pos, lit(1)).as_(
                global_order_col_identifier
            )
            extra_columns_to_append = extra_columns_to_append + [global_order_expr]
            ordering_columns = [
                OrderingColumn(global_order_col_identifier)
            ] + ordering_columns

        joined_ordered_frame = joined_ordered_frame.select(
            joined_ordered_frame.projected_column_snowflake_quoted_identifiers
            + extra_columns_to_append
        )

        filter_expression = not_(col_matching_column) | (left_row_pos == right_row_pos)
        # If left_on_cols matches with right_on_cols, include only the rows when left row
        # position is same as right row position.
        # If left_on_cols does not match right_on_cols, all values in 'col_matching' column will
        # be False, so we include all rows.
        # Example 1 (matching columns):
        # left:
        # E  row_position, A
        # 2  0             a
        # 1  1             b
        # 2  2             c
        #
        # right:
        # F  row_position, B
        # 2  0             d
        # 1  1             e
        # 2  2             f
        # left outer join right on column E and F
        # (Note some columns here are only for illustration, they are not actually added to
        # frame)
        # E   left_row_pos, right_row_pos, A, B, eq_row_pos, eq_row_pos_count, col_matching, filter,  order_col, left_row_count
        # 2   0             0              a  d  1           3                 True           True    0          3
        # 2   0             2              a  f  0           3                 True           False   0          3
        # 1   1             1              b  e  1           3                 True           True    1          3
        # 2   2             0              c  d  0           3                 True           False   2          3
        # 2   2             2              c  f  1           3                 True           True    2          3
        #
        # Example 2 (not matching columns)
        # left:
        # E  row_position, A
        # 2  0             a
        # 1  1             b
        # 2  2             c
        #
        # right:
        # F  row_position, B
        # 2  0             d
        # 3  1             e
        # 2  2             f
        # left outer join right on column E and F
        # (Note some columns here are only for illustration, they are not actually added to
        # frame)
        # E   left_row_pos, right_row_pos, A,   B,   eq_row_pos, eq_row_pos_count, col_matching, filter,  order_col, left_row_count
        # 2   0             0              a    d    1           2                 False         True     1          3
        # 2   0             2              a    f    0           2                 False         True     1          3
        # 1   1             None           b    None 0           2                 False         True     1          3
        # 2   2             0              c    d    0           2                 False         True     1          3
        # 2   2             2              c    f    1           2                 False         True     1          3
        # 3   None          1              None e    0           2                 False         True     1          3

        # Example 3: (left is empty)
        # left:
        # E  row_position, A
        #  << now rows >>
        #
        # right:
        # F  row_position, B
        # 2  0             d
        # 3  1             e
        # 4  2             f
        # left outer join right on column E and F
        # E    F  left_row_pos, right_row_pos, A,   B,   eq_row_pos, eq_row_pos_count, col_matching, filter,  order_col, left_row_count
        # NULL 2  NULL          0             NULL  d    False       0                 False         True     1          0
        # NULL 3  NULL          1             NULL  e    False       0                 False         True     1          0
        # NULL 4  NULL          2             NULL  f    False       0                 False         True     1          0

        if how == "coalesce":
            # For left align if left frame row count is 0 we convert this to right join
            # behavior by filtering out rows where right_row_pos is null. Otherwise,
            # we provide left join behavior by filtering out rows where left_row_pos is
            # null.
            filter_expression = filter_expression & iff(
                left_count_column == 0,
                right_row_pos.is_not_null(),  # right join
                left_row_pos.is_not_null(),  # left join
            )
            from snowflake.snowpark.modin.plugin._internal.utils import (
                unquote_name_if_quoted,
            )

            # We also update left_on_cols to right_on_cols if left frame row count is 0
            # Generate new identifiers for 'left_on_cols'. The new columns keep
            # the left_on value if left is not empty, otherwise keep values from
            # corresponding right_on column values.
            new_left_on_cols = (
                joined_ordered_frame.generate_snowflake_quoted_identifiers(
                    pandas_labels=[unquote_name_if_quoted(c) for c in left_on_cols]
                )
            )
            select_list = []
            for identifier in result_projected_column_snowflake_quoted_identifiers:
                if identifier in left_on_cols:
                    # Using the example above new left_on column be generated as
                    # iff(left_count == 0, F, E)
                    # When left_count is 0, F (from right frame) becomes new left_on
                    # otherwise E (from left frame) remains left_on column.
                    i = left_on_cols.index(identifier)
                    select_list.append(
                        iff(
                            left_count_column == 0,
                            Column(right_on_cols[i]),
                            Column(left_on_cols[i]),
                        ).as_(new_left_on_cols[i])
                    )
                else:
                    select_list.append(identifier)
        elif how == "left":
            filter_expression = filter_expression & left_row_pos.is_not_null()
            select_list = result_projected_column_snowflake_quoted_identifiers
        else:  # outer
            select_list = result_projected_column_snowflake_quoted_identifiers

        joined_ordered_frame = joined_ordered_frame.filter(filter_expression).sort(
            ordering_columns
        )

        # call select to make sure only the result_projected_column_snowflake_quoted_identifiers are projected
        # in the join result
        return joined_ordered_frame.select(select_list)

    def filter(self, expr: ColumnOrSqlExpr) -> "OrderedDataFrame":
        """
        Filters rows based on the specified conditional expression.
        Note that ordering columns will not be changed.

        See detailed docstring in Snowpark DataFrame's filter.
        """
        projected_dataframe_ref = self._to_projected_snowpark_dataframe_reference(
            include_ordering_columns=True
        )
        snowpark_dataframe = projected_dataframe_ref.snowpark_dataframe.filter(expr)
        return OrderedDataFrame(
            DataFrameReference(
                snowpark_dataframe,
                # same columns are retained after filtering
                snowflake_quoted_identifiers=projected_dataframe_ref.snowflake_quoted_identifiers,
            ),
            projected_column_snowflake_quoted_identifiers=projected_dataframe_ref.snowflake_quoted_identifiers,
            ordering_columns=self.ordering_columns,
        )

    def limit(self, n: int, offset: int = 0, sort: bool = True) -> "OrderedDataFrame":
        """
        Returns a new DataFrame that contains at most ``n`` rows from the current
        DataFrame, skipping ``offset`` rows from the beginning (similar to LIMIT and OFFSET in SQL).
        Note that ordering columns will not be changed. Once difference of this limit and
        Snowpark DataFrame's limit is that we will sort the DataFrame first and return ``n`` ordered
        records, instead of returning ``n`` arbitrary records if sort is True.

        See detailed docstring in Snowpark DataFrame's limit.
        """
        projected_dataframe_ref = self._to_projected_snowpark_dataframe_reference(
            include_ordering_columns=True, sort=sort
        )
        snowpark_dataframe = projected_dataframe_ref.snowpark_dataframe.limit(
            n=n, offset=offset
        )
        return OrderedDataFrame(
            DataFrameReference(
                snowpark_dataframe,
                # the same columns are retained for limit
                snowflake_quoted_identifiers=projected_dataframe_ref.snowflake_quoted_identifiers,
            ),
            projected_column_snowflake_quoted_identifiers=projected_dataframe_ref.snowflake_quoted_identifiers,
            ordering_columns=self.ordering_columns,
        )

    @property
    def write(self) -> DataFrameWriter:
        """
        Returns a new DataFrameWriter object that you can use to write the data in the DataFrame to
        a Snowflake database or a stage location.

        Note that this DataFrameWriter object contains the ordering columns and row position columns,
        typically used for caching intermediate result in Snowpark pandas.

        See detailed docstring in Snowpark DataFrame's write.
        """
        return self.to_projected_snowpark_dataframe(
            include_ordering_columns=True,
            include_row_position_column=True,
            include_row_count_column=False,
        ).write

    def __getitem__(self, item: str) -> Column:
        return self._dataframe_ref.snowpark_dataframe[item]

    def collect(
        self,
        *,
        statement_params: Optional[dict[str, str]] = None,
        block: bool = True,
        log_on_exception: bool = False,
        case_sensitive: bool = True,
    ) -> list[Row]:
        """
        Executes the query representing this DataFrame and returns the result as a
        list of Row objects.
        The result will only contain project columns and ordered by ordering columns.

        See detailed docstring in Snowpark DataFrame's collect.
        """
        snowpark_dataframe = self.to_projected_snowpark_dataframe(sort=True)
        from snowflake.snowpark.modin.plugin._internal.utils import (
            get_default_snowpark_pandas_statement_params,
        )

        if statement_params is None:
            statement_params = get_default_snowpark_pandas_statement_params()
        else:
            statement_params.update(get_default_snowpark_pandas_statement_params())
        return snowpark_dataframe.collect(
            statement_params=statement_params,
            block=block,
            log_on_exception=log_on_exception,
            case_sensitive=case_sensitive,
        )

    def to_pandas(
        self,
        *,
        statement_params: Optional[dict[str, str]] = None,
        block: bool = True,
        **kwargs: dict[str, Any],
    ) -> pandas.DataFrame:
        """
        Converts a DataFrame to a native pandas DataFrame.
        The result will only contain project columns and ordered by ordering columns.

        See detailed docstring in Snowpark DataFrame's to_pandas.
        """
        # Although the query will be SELECT ... FROM (... ORDER BY ...)
        # the final result will still be ordered because there is an implicit
        # guarantee on the server side
        snowpark_dataframe = self.to_projected_snowpark_dataframe(sort=True)
        from snowflake.snowpark.modin.plugin._internal.utils import (
            get_default_snowpark_pandas_statement_params,
        )

        if statement_params is None:
            statement_params = get_default_snowpark_pandas_statement_params()
        else:
            statement_params.update(get_default_snowpark_pandas_statement_params())
        return snowpark_dataframe.to_pandas(
            statement_params=statement_params, block=block, **kwargs
        )

    def _to_projected_snowpark_dataframe_reference(
        self,
        include_ordering_columns: bool = False,
        include_row_position_column: bool = False,
        include_row_count_column: bool = False,
        sort: bool = False,
        col_mapper: Optional[dict[str, str]] = None,
    ) -> DataFrameReference:
        """
        Returns a dataframe reference with the referred Snowpark dataframe that
            1) selects the projected columns and ordering columns if `include_ordering_columns` is True
            2) sorted with the ordering columns if `sort` is True

        Args:
            include_ordering_columns: bool. Whether to include ordering columns in the result Snowpark Dataframe if
                not in projected columns.
            include_row_position_column: bool. Whether to include row position column in the result Snowpark Dataframe
                if not in projected columns.
            include_row_count_column: bool. Whether to include row count column in the result Snowpark Dataframe
                if not in projected columns.
            sort: bool. Whether sort the result Snowpark Dataframe based on the ordering columns.
            col_mapper: Optional[Dict[str, str]]. A dictionary mapping from existing snowflake quoted identifiers
                to new snowflake quoted identifiers for the result Snowpark dataframe.

        Returns:
            A DataFrameReference with the Snowpark dataframe with the selected columns and quoted identifier cache
            set correctly.
        """
        snowpark_dataframe = self._dataframe_ref.snowpark_dataframe
        if sort:
            snowpark_dataframe = snowpark_dataframe.sort(
                self._ordering_snowpark_columns()
            )

        columns_quoted_identifiers = (
            self._get_active_column_snowflake_quoted_identifiers(
                include_ordering_columns=include_ordering_columns,
                include_row_position_column=include_row_position_column,
                include_row_count_column=include_row_count_column,
            )
        )
        if col_mapper is not None:
            columns = []
            result_columns_quoted_identifiers = []
            # perform rename to the columns if col_mapper is provided
            for quoted_identifier in columns_quoted_identifiers:
                # project the columnname to an updated alias
                columns.append(
                    Column(quoted_identifier).as_(
                        col_mapper.get(quoted_identifier, quoted_identifier)
                    )
                )
                result_columns_quoted_identifiers.append(
                    col_mapper.get(quoted_identifier, quoted_identifier)
                )
        else:
            # No column renaming needed
            columns = columns_quoted_identifiers
            result_columns_quoted_identifiers = columns_quoted_identifiers

        return DataFrameReference(
            snowpark_dataframe.select(columns),
            snowflake_quoted_identifiers=result_columns_quoted_identifiers,
        )

    def to_projected_snowpark_dataframe(
        self,
        include_ordering_columns: bool = False,
        include_row_position_column: bool = False,
        include_row_count_column: bool = False,
        sort: bool = False,
        col_mapper: Optional[dict[str, str]] = None,
    ) -> SnowparkDataFrame:
        """
        Returns the Snowpark dataframe with all projected columns and ordering columns if `include_ordering_columns`
        is True. The Snowpark dataframe is sorted with the ordering columns if `sort` is True.

        Args:
            include_ordering_columns: bool. Whether to include ordering columns in the result Snowpark Dataframe if
                not in projected columns.
            include_row_position_column: bool. Whether to include row position column in the result Snowpark Dataframe
                if not in projected columns.
            include_row_count_column: bool. Whether to include row count column in the result Snowpark Dataframe
                if not in projected columns.
            sort: bool. Whether sort the result Snowpark Dataframe based on the ordering columns.
            col_mapper: Optional[Dict[str, str]]. A dictionary mapping from existing snowflake quoted identifiers
                to new snowflake quoted identifiers for the result Snowpark dataframe.

        Returns:
            A SnowparkDataFrame with the selected columns renamed properly.
        """
        return self._to_projected_snowpark_dataframe_reference(
            include_ordering_columns,
            include_row_position_column,
            include_row_count_column,
            sort,
            col_mapper,
        ).snowpark_dataframe

    def _is_self_join_on_row_position_column(
        self,
        left_on_cols: list[str],
        right: "OrderedDataFrame",
        right_on_cols: list[str],
    ) -> bool:
        """
        Returns True if a join is a self-join on the row position columns of two ordered dataframe.
        Specifically,
            - two ordered dataframes are the same base ordered dataframe, i.e. same dataframe reference and ordering column
            - left_on_cols is the row position column of left (self) dataframe and right_on_cols is the row position column of right dataframe,
              which also means the row position columns of two dataframes are not None.
        """
        return (
            self._has_same_base_ordered_dataframe(right)
            and (
                self.row_position_snowflake_quoted_identifier is not None
                and left_on_cols == [self.row_position_snowflake_quoted_identifier]
            )
            and (
                right.row_position_snowflake_quoted_identifier is not None
                and right_on_cols == [right.row_position_snowflake_quoted_identifier]
            )
        )

    def sample(self, n: Optional[int], frac: Optional[float]) -> "OrderedDataFrame":
        """
        Sample rows on an OrderedDataFrame
        Args:
            n: Number of rows to return. Cannot be used with `frac`.
            frac: Fraction of rows to return. Cannot be used with `n`.

        Returns:
            OrderedDataFrame sample. Note that the result's ordering and projected columns are the same as the original
            dataframe.
        """
        projected_dataframe_ref = self._to_projected_snowpark_dataframe_reference(
            include_ordering_columns=True
        )

        snowpark_dataframe = projected_dataframe_ref.snowpark_dataframe.sample(
            n=n, frac=frac
        )
        from snowflake.snowpark.modin.plugin._internal.utils import cache_result

        # Note: the returned frame is a cached result to make the sampled result deterministic. If we don't cache it,
        # then for example:
        # df_s = df.sample(frac=0.5)
        # assert df_s.index == df_s.index may fail because both the LHS and RHS will call the sample method during
        # evaluation and the results won't be deterministic.
        return cache_result(
            OrderedDataFrame(
                DataFrameReference(
                    snowpark_dataframe,
                    # same columns are retained after sampling
                    snowflake_quoted_identifiers=projected_dataframe_ref.snowflake_quoted_identifiers,
                ),
                projected_column_snowflake_quoted_identifiers=self.projected_column_snowflake_quoted_identifiers,
                ordering_columns=self.ordering_columns,
            )
        )
