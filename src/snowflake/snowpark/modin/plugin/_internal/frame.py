#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import functools
from collections.abc import Hashable
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable, NamedTuple, Optional, Union

import pandas as native_pd
from pandas._typing import IndexLabel
from pandas.core.dtypes.common import is_object_dtype

from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import col, last_value
from snowflake.snowpark.modin import pandas as pd
from snowflake.snowpark.modin.plugin._internal.ordered_dataframe import (
    OrderedDataFrame,
    OrderingColumn,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import TypeMapper
from snowflake.snowpark.modin.plugin._internal.utils import (
    DEFAULT_DATA_COLUMN_LABEL,
    INDEX_LABEL,
    ROW_POSITION_COLUMN_LABEL,
    append_columns,
    assert_duplicate_free,
    cache_result,
    count_rows,
    extract_pandas_label_from_snowflake_quoted_identifier,
    fill_missing_levels_for_pandas_label,
    from_pandas_label,
    get_distinct_rows,
    is_valid_snowflake_quoted_identifier,
    snowpark_to_pandas_helper,
    to_pandas_label,
)
from snowflake.snowpark.modin.plugin._typing import (
    LabelIdentifierPair,
    LabelTuple,
    PandasLabelToSnowflakeIdentifierPair,
)
from snowflake.snowpark.modin.utils import MODIN_UNNAMED_SERIES_LABEL
from snowflake.snowpark.types import DataType
from snowflake.snowpark.window import Window

logger = getLogger(__name__)

LEFT_PREFIX = "left"
RIGHT_PREFIX = "right"


class UpdatedInternalFrameResult(NamedTuple):
    """Contains the updated internal frame and mapping from old ids to new ids."""

    frame: "InternalFrame"
    old_id_to_new_id_mappings: dict[str, str]


@dataclass(frozen=True)
class InternalFrame:
    """
    internal abstraction of storage format to hold all information necessary to represent
    a pandas.DataFrame within Snowflake
    """

    # OrderedDataFrame representation of the state of the data hold by this internal frame
    # Ordering columns and row position column are maintained by OrderedDataFrame
    ordered_dataframe: OrderedDataFrame
    # Map between label and snowflake quoted identifier.
    # This map is maintained as an ordered list, which must be in the order of
    # pandas index columns + pandas data columns.
    # For MultiIndex as df.columns, the pandas label will be a tuple for each column.
    # An example of MultiIndex as df.columns:
    # pd.MultiIndex.from_tuples([('baz', 'A'), ('baz', 'B'), ('zoo', 'A'), ('zoo', 'B')])
    # the pandas labels of data columns will be [('baz', 'A'), ('baz', 'B'), ('zoo', 'A'), ('zoo', 'B')]
    label_to_snowflake_quoted_identifier: tuple[LabelIdentifierPair, ...]
    # Number of index columns for the pandas dataframe, where the first num_index_columns elements
    # of pandas_label_to_snowflake_quoted_identifier is for the pandas index columns
    num_index_columns: int
    # Store pandas labels for columns' index name or multiindex names, e.g., the labels is used to generate
    # df.columns.names
    # The length of data_column_index_names equals to number of multiindex levels.
    # For a 3-level MultiIndex, the value can be like ['A', 'B', 'C']
    data_column_index_names: tuple[LabelTuple, ...]

    @classmethod
    def create(
        cls,
        *,
        ordered_dataframe: OrderedDataFrame,
        data_column_pandas_labels: list[Hashable],
        data_column_pandas_index_names: list[Hashable],
        data_column_snowflake_quoted_identifiers: list[str],
        index_column_pandas_labels: list[Hashable],
        index_column_snowflake_quoted_identifiers: list[str],
    ) -> "InternalFrame":
        """
        Args:
            ordered_dataframe: underlying ordered dataframe used
            data_column_pandas_labels: A list of pandas hashable labels for pandas data columns.
            data_column_pandas_index_names: A list of hashable labels for pandas column index names
            data_column_snowflake_quoted_identifiers: A list of snowflake quoted identifiers for pandas data columns,
                represented by str. These identifiers are used to refer columns in underlying snowpark dataframe to
                access data in snowflake.
            index_column_pandas_labels: A list of pandas index column labels.
            index_column_snowflake_quoted_identifiers: A list of snowflake quoted identifiers for pandas index columns.
        """

        assert len(data_column_snowflake_quoted_identifiers) == len(
            data_column_pandas_labels
        ), f"data column label identifier length mismatch, labels {data_column_pandas_labels}, identifiers {data_column_snowflake_quoted_identifiers}"
        assert len(index_column_snowflake_quoted_identifiers) == len(
            index_column_pandas_labels
        ), f"index column label identifier length mismatch, labels {index_column_pandas_labels}, identifiers {index_column_snowflake_quoted_identifiers}"

        # List of pandas_label_to_snowflake_quoted_identifier mapping for index columns
        index_columns_mapping: list[LabelIdentifierPair] = [
            LabelIdentifierPair(
                # index column labels is always flat with only one level
                from_pandas_label(pandas_label, num_levels=1),
                snowflake_quoted_identifier,
            )
            for pandas_label, snowflake_quoted_identifier in zip(
                index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers,
            )
        ]

        # List of pandas_label_to_snowflake_quoted_identifier mapping for data columns
        data_columns_mapping: list[LabelIdentifierPair] = [
            LabelIdentifierPair(
                from_pandas_label(
                    pandas_label,
                    num_levels=len(data_column_pandas_index_names),
                ),
                snowflake_quoted_identifier,
            )
            for pandas_label, snowflake_quoted_identifier in zip(
                data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers,
            )
        ]

        return cls(
            ordered_dataframe=ordered_dataframe,
            label_to_snowflake_quoted_identifier=tuple(
                index_columns_mapping + data_columns_mapping
            ),
            num_index_columns=len(index_column_snowflake_quoted_identifiers),
            data_column_index_names=tuple(
                # data_column_index_names is always flat with only one level
                from_pandas_label(name, num_levels=1)
                for name in data_column_pandas_index_names
            ),
        )

    def __post_init__(self) -> None:
        # perform checks for dataclass here

        # check there must be at least one index column associated with the dataframe
        assert (
            self.num_index_columns >= 1
        ), "At least 1 index column should be presented for the dataframe"

        # the ordering_columns_tuple cannot be empty, because we guarantee the determinism
        # for the data order of the dataframe,
        assert len(self.ordering_columns) > 0, "ordering_columns cannot be empty"

        # validate data columns
        self._validate_data_column_pandas_index_names()

        # make sure that all names required in metadata are present within snowpark_dataframe
        # so that the internal frame represents a valid state.
        snowflake_quoted_identifiers = (
            self.ordered_dataframe.projected_column_snowflake_quoted_identifiers
        )

        def validate_snowflake_quoted_identifier(
            quoted_identifier: str,
            column_category: str,
            hashable_label: Hashable = None,
        ) -> None:
            """
            validation for the snowflake quoted identifier, which performs two checks:
            1) the identifier is quoted 2) the identifier exists in the underlying snowpark dataframe

            Returns:
                None. Assertion is raised if any check fails.
            """
            # generate a properly quoted escaped_name for the error message below.
            escaped_name = quoted_identifier.replace("'", "\\'")
            assert is_valid_snowflake_quoted_identifier(
                quoted_identifier
            ), f"Found not-quoted identifier for '{column_category}':'{escaped_name}'"

            assert quoted_identifier in snowflake_quoted_identifiers, (
                f"{column_category}={escaped_name} not found in snowpark dataframe "
                f"schema {snowflake_quoted_identifiers}, pandas_label={hashable_label}"
            )

        # validate the snowflake quoted identifier data + index columns
        for (
            label,
            snowflake_quoted_identifier,
        ) in self.label_to_snowflake_quoted_identifier:
            validate_snowflake_quoted_identifier(
                snowflake_quoted_identifier,
                "dataframe column",
                to_pandas_label(label),
            )

        # check that snowflake quoted identifier is duplicate free
        assert_duplicate_free(
            self.index_column_snowflake_quoted_identifiers
            + self.data_column_snowflake_quoted_identifiers,
            "dataframe columns",
        )

    def _validate_data_column_pandas_index_names(self) -> None:
        # the index on column (df.columns) must have a name (can be None)
        assert (
            len(self.data_column_pandas_index_names) >= 1
        ), "data_column_pandas_index_names cannot be empty"

        # validate all labels are tuples with the same length
        num_levels = len(self.data_column_pandas_index_names)
        for label, _ in self.label_to_snowflake_quoted_identifier[
            self.num_index_columns :
        ]:
            assert num_levels == len(
                label
            ), f"All tuples in data_column_pandas_labels must have the same length {num_levels}, but got {label}"

    @property
    def index_column_snowflake_quoted_identifiers(self) -> list[str]:
        """
        Get snowflake quoted identifier for all index columns
        Returns:
            List of snowflake quoted identifiers for index columns
        """
        return [
            col.snowflake_quoted_identifier
            for col in self.label_to_snowflake_quoted_identifier[
                : self.num_index_columns
            ]
        ]

    @property
    def data_column_snowflake_quoted_identifiers(self) -> list[str]:
        """
        Get snowflake quoted identifier for all data columns
        Returns:
            List of snowflake quoted identifiers for data columns
        """
        return [
            col.snowflake_quoted_identifier
            for col in self.label_to_snowflake_quoted_identifier[
                self.num_index_columns :
            ]
        ]

    def quoted_identifier_to_snowflake_type(self) -> dict[str, DataType]:
        return {
            f.column_identifier.quoted_name: f.datatype
            for f in self.ordered_dataframe.schema.fields
        }

    @property
    def index_column_pandas_labels(self) -> list[Hashable]:
        """
        Get pandas labels for all index columns
        Returns:
            List of pandas labels for index columns
        """
        return [
            to_pandas_label(col.label)
            for col in self.label_to_snowflake_quoted_identifier[
                : self.num_index_columns
            ]
        ]

    @property
    def data_column_pandas_labels(self) -> list[Hashable]:
        """
        Get pandas labels for all data columns
        Returns:
            List of pandas labels for data columns
        """
        return [
            to_pandas_label(col.label)
            for col in self.label_to_snowflake_quoted_identifier[
                self.num_index_columns :
            ]
        ]

    @property
    def ordering_column_snowflake_quoted_identifiers(self) -> list[str]:
        """
        Get snowflake quoted identifier for ordering columns
        Return:
            List of snowflake quoted identifier for the ordering columns
        """

        return self.ordered_dataframe.ordering_column_snowflake_quoted_identifiers

    @property
    def ordering_columns(self) -> list[OrderingColumn]:
        """
        Get list of ordering columns.
        Returns:
            List of OrderingColumn.
        """
        return self.ordered_dataframe.ordering_columns

    @property
    def row_position_snowflake_quoted_identifier(self) -> Optional[str]:
        return self.ordered_dataframe.row_position_snowflake_quoted_identifier

    @property
    def row_count_snowflake_quoted_identifier(self) -> Optional[str]:
        return self.ordered_dataframe.row_count_snowflake_quoted_identifier

    @property
    def data_column_pandas_index_names(self) -> list[Hashable]:
        """Returns pandas labels from column index (df.columns.names)."""
        return [to_pandas_label(name) for name in self.data_column_index_names]

    def num_index_levels(self, *, axis: int = 0) -> int:
        """
        Returns number of index levels for given `axis`.

        Args:
            axis: If axis=0, return number of levels in row labels.
                If axis=1, return number of levels in columns labels.

        Returns:
            number of index levels for given `axis`

        Raises:
            ValueError if `axis` is not valid.
        """
        if axis == 0:
            return self.num_index_columns
        elif axis == 1:
            return len(self.data_column_pandas_index_names)
        else:
            raise ValueError("'axis' can only be 0 or 1")

    def is_multiindex(self, *, axis: int = 0) -> bool:
        """
        Returns whether the InternalFrame has a MultiIndex along `axis`.
        Args:
            axis: If axis=0, return whether the InternalFrame has a MultiIndex as df.index.
                If axis=1, return whether the InternalFrame has a MultiIndex as df.columns.
        """
        return self.num_index_levels(axis=axis) > 1

    def is_unnamed_series(self) -> bool:
        """
        Check if the InternalFrame is a representation for an unnamed series. An InternalFrame represents an
        unnamed series if there is only one data column and the data column has label name MODIN_UNNAMED_SERIES_LABEL.
        """
        return (
            len(self.data_column_pandas_labels) == 1
            and self.data_column_pandas_labels[0] == MODIN_UNNAMED_SERIES_LABEL
        )

    @property
    def data_columns_index(self) -> "pd.Index":
        """
        Returns Snowpark pandas Index object for column index (df.columns).
        Note this object will still hold an internal pandas index (i.e., not lazy) to avoid unnecessary pulling data from Snowflake.
        """
        if self.is_multiindex(axis=1):
            return native_pd.MultiIndex.from_tuples(
                self.data_column_pandas_labels,
                names=self.data_column_pandas_index_names,
            )
        else:
            return pd.Index(
                self.data_column_pandas_labels,
                name=self.data_column_pandas_index_names[0],
                # setting tupleize_cols=False to avoid creating a MultiIndex
                # otherwise, when labels are tuples (e.g., [("A", "a"), ("B", "b")]),
                # a MultiIndex will be created incorrectly
                tupleize_cols=False,
                # setting is_lazy as false because we want to store the columns locally
                convert_to_lazy=False,
            )

    @property
    def index_columns_pandas_index(self) -> native_pd.Index:
        """
        Get pandas index. The method eagerly pulls the values from Snowflake because index requires the values to be
        filled

        Returns:
            The index (row labels) of the DataFrame.
        """
        index_values = snowpark_to_pandas_helper(
            self.ordered_dataframe.select(
                self.index_column_snowflake_quoted_identifiers
            )
        ).values
        if self.is_multiindex(axis=0):
            value_tuples = [tuple(row) for row in index_values]
            return native_pd.MultiIndex.from_tuples(
                value_tuples, names=self.index_column_pandas_labels
            )
        else:
            # We have one index column. Fill in the type correctly.
            index_identifier = self.index_column_snowflake_quoted_identifiers[0]
            index_type = TypeMapper.to_pandas(
                self.quoted_identifier_to_snowflake_type()[index_identifier]
            )
            ret = native_pd.Index(
                [row[0] for row in index_values],
                name=self.index_column_pandas_labels[0],
                # setting tupleize_cols=False to avoid creating a MultiIndex
                # otherwise, when labels are tuples (e.g., [("A", "a"), ("B", "b")]),
                # a MultiIndex will be created incorrectly
                tupleize_cols=False,
            )
            # When pd.Index() failed to reduce dtype to a numpy or pandas extension type, it will be object type. For
            # example, an empty dataframe will be object dtype by default, or a variant, or a timestamp column with
            # multiple timezones. So here we cast the index to the index_type when ret = pd.Index(...) above cannot
            # figure out a non-object dtype. Note that the index_type is a logical type may not be 100% accurate.
            if is_object_dtype(ret.dtype) and not is_object_dtype(index_type):
                ret = ret.astype(index_type)
            return ret

    def get_snowflake_quoted_identifiers_group_by_pandas_labels(
        self,
        pandas_labels: list[Hashable],
        include_index: bool = True,
    ) -> list[tuple[str, ...]]:
        """
        Map given pandas labels to names in underlying snowpark dataframe. Given labels can be data or index labels.
        Single label can map to multiple snowpark names from underlying dataframe. Which is represented by tuples.
        We return the result in the same order as input pandas_labels.

        Args:
            pandas_labels: A list of pandas labels.
            include_index: Include the index columns in addition to data columns, default is True.

        Returns:
            A list of tuples for matched identifiers. Each element of list is a tuple of str containing matched
            snowflake quoted identifiers for corresponding pandas label in 'pandas_labels'.
            Length and order of this list is same as length of given 'pandas_labels'.
        """

        snowflake_quoted_identifiers = []
        for label in pandas_labels:
            matched_columns = list(
                filter(
                    lambda col: to_pandas_label(col.label) == label,
                    self.label_to_snowflake_quoted_identifier[
                        (0 if include_index else self.num_index_columns) :
                    ],
                )
            )
            snowflake_quoted_identifiers.append(
                tuple(col.snowflake_quoted_identifier for col in matched_columns)
            )

        return snowflake_quoted_identifiers

    def parse_levels_to_integer_levels(
        self, levels: IndexLabel, allow_duplicates: bool, axis: int = 0
    ) -> list[int]:
        """
        Returns a list of integers representing levels in Index object on given axis.

        Args:
            levels: IndexLabel, can be int, level name, or sequence of such.
            allow_duplicates: whether allow duplicated levels in the result. When False, the result will not
                contain any duplicated levels. Otherwise, the result will contain duplicated level number if
                different level value is mapped to the same level number.
            axis: DataFrame axis, given levels belong to. Defaults to 0. Allowed values
                are 0 or 1.
        Returns:
            List[int]
                A list of integers corresponding to the index levels for the given level, and in the same
                order as given level
        """
        num_level = self.num_index_levels(axis=axis)
        if levels is not None:
            if not isinstance(levels, (tuple, list)):
                levels = [levels]
            result = []
            for key in levels:
                if isinstance(key, int):
                    error_message = f"Too many levels: Index has only {num_level} level{'s' if num_level > 1 else ''}"
                    # when key < 0, raise IndexError if key < -num_level as native pandas does
                    # set key to a positive number as native pandas does
                    if key < 0:
                        key = key + num_level
                        if key < 0:
                            raise IndexError(
                                f"{error_message}, {key - num_level} is not a valid level number"
                            )
                    # when key > num_level - 1, raise IndexError as native pandas does
                    elif key > num_level - 1:  # level starts from 0
                        raise IndexError(f"{error_message}, not {key + 1}")
                elif isinstance(key, str):  # get level number from label
                    try:
                        if axis == 0:
                            key = self.index_column_pandas_labels.index(key)
                        else:
                            key = self.data_column_pandas_index_names.index(key)
                    # if key doesn't exist, a ValueError will be raised
                    except ValueError:
                        if num_level > 1:
                            raise KeyError(f"Level {key} not found")
                        else:
                            raise KeyError(
                                f"Requested level ({key}) does not match index name ({self.index_column_pandas_labels[0]})"
                            )
                # do not add key in the result if the key is already in the result and duplication is not allowed
                if (key not in result) or allow_duplicates:
                    result.append(key)
        else:
            result = list(range(num_level))
        return result

    def get_pandas_labels_for_levels(self, levels: list[int]) -> list[Hashable]:
        """
        Get the list of corresponding pandas labels for a list of given integer
        Index levels.
        Note: duplication in levels is allowed.
        """
        return [self.index_column_pandas_labels[level] for level in levels]

    def get_snowflake_identifiers_for_levels(self, levels: list[int]) -> list[str]:
        """
        Get the list of corresponding Snowflake identifiers for a list of given integer index levels.

        Note: duplication in levels is allowed.
        """
        return [
            self.index_column_snowflake_quoted_identifiers[level] for level in levels
        ]

    def get_snowflake_identifiers_and_pandas_labels_from_levels(
        self, levels: list[int]
    ) -> tuple[list[Hashable], list[str], list[Hashable], list[str]]:
        """
        Selects snowflake identifiers and pandas labels from index columns in `levels`.
        Also returns snowflake identifiers and pandas labels not in `levels`.

        Args:
            levels: A list of integers represents levels in pandas Index.

        Returns:
            A tuple contains 4 lists:
            1. The first list contains snowflake identifiers of index columns in `levels`.
            2. The second list contains pandas labels of index columns in `levels`.
            3. The third list contains snowflake identifiers of index columns not in `levels`.
            4. The fourth list contains pandas labels of index columns not in `levels`.
        """
        index_column_pandas_labels_in_levels = []
        index_column_snowflake_quoted_identifiers_in_levels = []
        index_column_pandas_labels_not_in_levels = []
        index_column_snowflake_quoted_identifiers_not_in_levels = []
        for idx, (identifier, label) in enumerate(
            zip(
                self.index_column_snowflake_quoted_identifiers,
                self.index_column_pandas_labels,
            )
        ):
            if idx in levels:
                index_column_pandas_labels_in_levels.append(label)
                index_column_snowflake_quoted_identifiers_in_levels.append(identifier)
            else:
                index_column_pandas_labels_not_in_levels.append(label)
                index_column_snowflake_quoted_identifiers_not_in_levels.append(
                    identifier
                )

        return (
            index_column_pandas_labels_in_levels,
            index_column_snowflake_quoted_identifiers_in_levels,
            index_column_pandas_labels_not_in_levels,
            index_column_snowflake_quoted_identifiers_not_in_levels,
        )

    @functools.cached_property
    def num_rows(self) -> int:
        """
        Returns:
            Number of rows in this frame.
        """
        return count_rows(self.ordered_dataframe)

    def has_unique_index(self, axis: Optional[int] = 0) -> bool:
        """
        Returns true if index has unique values on specified axis.
        Args:
            axis: {0, 1} defaults to 0

        Returns:
            True if index has unique values on specified axis, otherwise returns False.

        """
        if axis == 1:
            return self.data_columns_index.is_unique
        else:
            # Note: We can't use 'count_distinct' because it ignores null values.
            total_rows = self.num_rows
            distinct_rows = count_rows(
                get_distinct_rows(
                    self.ordered_dataframe.select(
                        self.index_column_snowflake_quoted_identifiers
                    )
                )
            )
            return total_rows == distinct_rows

    def validate_no_duplicated_data_columns_mapped_for_labels(
        self,
        pandas_labels: list[Hashable],
        user_frame_identifier: Optional[str] = None,
    ) -> None:
        """
        For a given set of pandas labels, verify that there are no multiple data columns in the frame
        mapped to the same label in the `pandas_labels`.

        Args:
            pandas_labels: set of pandas labels to check for duplicated column mappings
            user_frame_identifier: the identifier for the frame that is used in the error message to help user to
                    identify which input frame has error. For example, it can be 'condition' or 'other' frame for
                    where API.
        Raises:
            ValueError: if for a pandas label, there exists more than one data columns in the given frame mapped to the label.
        """
        label_identifiers_list = (
            self.get_snowflake_quoted_identifiers_group_by_pandas_labels(
                pandas_labels=pandas_labels, include_index=False
            )
        )
        labels_with_duplication = [
            pandas_labels[i]
            for (i, label_identifiers_tuple) in enumerate(label_identifiers_list)
            if len(label_identifiers_tuple) > 1
        ]
        if len(labels_with_duplication) > 0:
            # The error message raised under duplication cases is different from native pandas.
            # Native pandas raises ValueError with message "cannot reindex on an axis with duplicate labels"
            # for duplication occurs in the condition frame, and raises InvalidIndexError with no message for
            # duplication occurs in other frame.
            # Snowpark pandas gives a clear message to the customer about what is the problem with the dataframe.
            message = f"Multiple columns are mapped to each label in {labels_with_duplication} in DataFrame"
            if user_frame_identifier is not None:
                message += f" {user_frame_identifier}"
            raise ValueError(message)

    ###########################################################################
    # START: Internal Frame mutation APIs.
    # APIs that creates a new InternalFrame instance, should only be added below
    def ensure_row_position_column(self) -> "InternalFrame":
        """
        Ensure row position column is computed for given internal frame.

        Returns:
            A new InternalFrame instance with computed virtual index.
        """
        return InternalFrame.create(
            ordered_dataframe=self.ordered_dataframe.ensure_row_position_column(),
            data_column_pandas_labels=self.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=self.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
        )

    def ensure_row_count_column(self) -> "InternalFrame":
        """
        Ensure row position column is computed for given internal frame.

        Returns:
            A new InternalFrame instance with computed virtual index.
        """
        return InternalFrame.create(
            ordered_dataframe=self.ordered_dataframe.ensure_row_count_column(),
            data_column_pandas_labels=self.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=self.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
        )

    def persist_to_temporary_table(self) -> "InternalFrame":
        """
        Persists the OrderedDataFrame backing this InternalFrame to a temporary table for the duration of the session.

        Returns:
            A new InternalFrame with the backing OrderedDataFrame persisted to a temporary table.
        """
        return InternalFrame.create(
            ordered_dataframe=cache_result(self.ordered_dataframe),
            data_column_pandas_labels=self.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=self.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
        )

    def append_column(
        self, pandas_label: Hashable, value: SnowparkColumn
    ) -> "InternalFrame":
        """
        Append a column to this frame. The column is added at the end. For a frame with multiindex column, it
        automatically fills the missing levels with None. For example, in a table with MultiIndex columns like
        ("A", "col1"), ("A", "col2"), ("B", "col1"), ("B", "col2"), appending a count column "cnt" will produce
        a column labelled ("cnt", None).

        Args:
            pandas_label: pandas label for column to be inserted.
            value: SnowparkColumn.

        Returns:
            A new InternalFrame with new column.
        """
        # +---------------+---------------+---------------+---------------+       +---------------+
        # | ("A", "col1") | ("A", "col2") | ("B", "col1") | ("B", "col2") |       | "cnt"         |
        # +---------------+---------------+---------------+---------------+   +   +---------------+
        # | . . .         | . . .         | . . .         | . . .         |       | . . .         |
        # +---------------+---------------+---------------+---------------+       +---------------+
        #
        # Appending a column "cnt" to the table below will produce the following table:
        # +---------------+---------------+---------------+---------------+---------------+
        # | ("A", "col1") | ("A", "col2") | ("B", "col1") | ("B", "col2") | ("cnt", None) |
        # +---------------+---------------+---------------+---------------+---------------+
        # | . . .         | . . .         | . . .         | . . .         | . . .         |
        # +---------------+---------------+---------------+---------------+---------------+

        # Generate label for the column to be appended.
        nlevels = self.num_index_levels(axis=1)
        pandas_label = fill_missing_levels_for_pandas_label(
            pandas_label, nlevels, 0, None
        )

        # Generate snowflake quoted identifier for new column to be added.
        new_column_identifier = (
            self.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[pandas_label],
            )[0]
        )
        new_ordered_dataframe = append_columns(
            self.ordered_dataframe, new_column_identifier, value
        )
        return InternalFrame.create(
            ordered_dataframe=new_ordered_dataframe,
            data_column_pandas_labels=self.data_column_pandas_labels + [pandas_label],
            data_column_snowflake_quoted_identifiers=self.data_column_snowflake_quoted_identifiers
            + [new_column_identifier],
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
        )

    def project_columns(
        self,
        pandas_labels: list[Hashable],
        column_objects: list[SnowparkColumn],
    ) -> "InternalFrame":
        """
        Project new columns with column_objects as the new data columns for the new Internal Frame.
        The original index columns, ordering columns and row position columns are still used as the
        index columns, ordering columns and row position columns for the new Internal Frame.

        * Note that this is different with append column in the sense that the data columns of the
        original data frame will not be part of the data columns of the result dataframe. The data
        column of the result dataframe only contains the new projected data columns.

        Args:
            pandas_labels: The pandas labels for the newly projected data columns
            column_objects: the Snowpark columns used to project the new data columns

        Returns:
            A new InternalFrame with the newly projected columns as data column
        """
        new_column_identifiers = (
            self.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=pandas_labels,
            )
        )
        new_ordered_dataframe = append_columns(
            self.ordered_dataframe, new_column_identifiers, column_objects
        )
        return InternalFrame.create(
            ordered_dataframe=new_ordered_dataframe,
            data_column_pandas_labels=pandas_labels,
            data_column_snowflake_quoted_identifiers=new_column_identifiers,
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
        )

    def rename_snowflake_identifiers(
        self, old_to_new_identifiers: dict[str, str]
    ) -> "InternalFrame":
        """
        Rename columns for underlying ordered dataframe.

        Args:
            old_to_new_identifiers: A dictionary from old to new identifiers name.
              Identifiers which do not occur in dictionary are not renamed.

        Returns:
            A new InternalFrame instance after rename.

        Raises:
            KeyError if columns are not index or data column of the current internal frame.
        """
        if not old_to_new_identifiers:
            return self

        ordered_dataframe = self.ordered_dataframe
        internal_frame_column_quoted_identifiers = (
            self.index_column_snowflake_quoted_identifiers
            + self.data_column_snowflake_quoted_identifiers
        )
        for old_id in old_to_new_identifiers:
            if old_id not in internal_frame_column_quoted_identifiers:
                raise KeyError(
                    f"Column not found: '{old_id}'."
                    f" Internal frame has following data and index columns: {internal_frame_column_quoted_identifiers}"
                )
        select_list = []
        any_column_to_rename = False
        ordering_and_row_position_columns = (
            ordered_dataframe.ordering_column_snowflake_quoted_identifiers
            + [ordered_dataframe.row_position_snowflake_quoted_identifier]
            if ordered_dataframe.row_position_snowflake_quoted_identifier is not None
            else []
        )
        for old_id in ordered_dataframe.projected_column_snowflake_quoted_identifiers:
            # Alias to new identifier name if present in 'old_to_new_identifiers',
            # otherwise leave unchanged.
            new_id = old_to_new_identifiers.get(old_id, old_id)
            if old_id == new_id:
                # retain the original column
                select_list.append(old_id)
            else:
                select_list.append(col(old_id).as_(new_id))
                # if the old column is part of the ordering or row position columns, retains the column
                # as part of the projected columns.
                if old_id in ordering_and_row_position_columns:
                    select_list.append(old_id)

            any_column_to_rename = any_column_to_rename or new_id != old_id
        if not any_column_to_rename:
            # This is possible when values in 'old_to_new_identifiers' are same as keys.
            return self
        ordered_dataframe = ordered_dataframe.select(select_list)

        def get_updated_identifiers(identifiers: list[str]) -> list[str]:
            """
            Get the new identifier after rename, and if not exist in the rename map,
            no rename happens, the original name is returned

            Args:
                identifiers: List of identifiers to get updated identifiers.

            Returns:
                A list of identifiers after rename, if not exist in the rename map,
                original name is returned.
            """
            return [old_to_new_identifiers.get(i, i) for i in identifiers]

        return InternalFrame.create(
            ordered_dataframe=ordered_dataframe,
            data_column_pandas_labels=self.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=get_updated_identifiers(
                self.data_column_snowflake_quoted_identifiers
            ),
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=get_updated_identifiers(
                self.index_column_snowflake_quoted_identifiers
            ),
        )

    def update_snowflake_quoted_identifiers_with_expressions(
        self,
        quoted_identifier_to_column_map: dict[str, SnowparkColumn],
    ) -> UpdatedInternalFrameResult:
        """
        Points Snowflake quoted identifiers to column expression given by `quoted_identifier_to_column_map`.

        This function takes a mapping from existing snowflake quoted identifiers to
        new Snowpark column expressions and points the existing quoted identifiers to the
        column expressions provided by the mapping. For optimization purposes,
        existing expressions are kept as columns. This does not change pandas labels.

        The process involves the following steps:

        1. Create a list of new snowflake quoted column identifiers from existing snowflake quoted
           column identifiers (keys of `quoted_identifier_to_column_map`) to prevent naming conflicts.
        2. Append new Snowpark columns (values of `quoted_identifier_to_column_map`)
           to the end of the Snowpark DataFrame with new snowflake quoted column identifiers
           generated at step 1.
        3. Update index and data column identifiers in the internal frame, by replacing existing
           snowflake quoted identifiers (keys of `quoted_identifier_to_column_map`)
           with new snowflake quoted column identifiers created in step 1

        Args:
            quoted_identifier_to_column_map (Dict[str, SnowparkColumn]): A dictionary mapping
                existing snowflake quoted identifiers to new Snowpark columns.
                As keys of a dictionary, all snowflake column identifiers are unique here and
                must be index columns and data columns in the original internal frame.

        Returns:
            UpdatedInternalFrameResult: A tuple contaning the new InternalFrame with updated column references, and a mapping
                                        of the old column ids to the new column ids.

        Raises:
            ValueError if any key of quoted_identifier_to_column_map is not in the data or index columns of the internal frame.

        Example:
            `update_snowflake_quoted_identifiers_with_expressions(quoted_identifier_to_column_map={'"A"' : lit(10), '"B"': col('"A"') + col('"B"')}).frame`
            The internal frame has pandas labels ['pd_a', 'pd_b', 'pd_a'] (there can be duplicates),
            mapping to the snowflake quoted identifiers ['"A"', '"B"', '"C"'], i.e. 'pd_a' -> "A", 'pd_b' -> "B", 'pd_a' -> "C".
            Index column identifiers are ['"A"'] and data column identifiers are ['"B"', '"C"'].
            Calling this function will now create new identifiers (and keep the old ones), so that
            'pd_a' -> lit(10), 'pd_b' -> col('"A"') + col('"B"'), 'pd_a' -> "C".
            For this, the function generates new aliases, e.g. '"A2"' for lit(10), and '"B2"' for col('"A"') + col('"B"').
            Thus, after applying this function the snowpark dataframe backing this internal frame up has
            ['"A"', '"B"', '"C"', '"A2"', '"B2"'] as quoted identifiers.
            Index column identifiers become ['"A2"'] and data column identifiers are still ['"B2"', '"C"'].
        """
        # no-op
        if not quoted_identifier_to_column_map:
            return UpdatedInternalFrameResult(self, {})

        all_data_index_identifiers = set(
            self.data_column_snowflake_quoted_identifiers
            + self.index_column_snowflake_quoted_identifiers
        )
        for identifier in quoted_identifier_to_column_map:
            if identifier not in all_data_index_identifiers:
                raise ValueError(f"{identifier} is not in {all_data_index_identifiers}")

        existing_id_to_new_id_mapping = {}
        new_columns = []
        for (
            existing_identifier,
            column_expression,
        ) in quoted_identifier_to_column_map.items():
            new_identifier = (
                self.ordered_dataframe.generate_snowflake_quoted_identifiers(
                    pandas_labels=[
                        extract_pandas_label_from_snowflake_quoted_identifier(
                            existing_identifier
                        )
                    ],
                )[0]
            )
            existing_id_to_new_id_mapping[existing_identifier] = new_identifier
            new_columns.append(column_expression)
        new_ordered_dataframe = append_columns(
            self.ordered_dataframe,
            list(existing_id_to_new_id_mapping.values()),
            new_columns,
        )
        # update index_column_snowflake_quoted_identifiers and data_column_snowflake_quoted_identifiers
        # the order of index/data_column_snowflake_quoted_identifiers is not changed so we can still
        # keep the correct mapping between quoted identifiers and pandas labels
        new_index_column_snowflake_quoted_identifiers = [
            existing_id_to_new_id_mapping.get(identifier, identifier)
            for identifier in self.index_column_snowflake_quoted_identifiers
        ]
        new_data_column_snowflake_quoted_identifiers = [
            existing_id_to_new_id_mapping.get(identifier, identifier)
            for identifier in self.data_column_snowflake_quoted_identifiers
        ]

        return UpdatedInternalFrameResult(
            InternalFrame.create(
                ordered_dataframe=new_ordered_dataframe,
                data_column_pandas_labels=self.data_column_pandas_labels,
                data_column_snowflake_quoted_identifiers=new_data_column_snowflake_quoted_identifiers,
                data_column_pandas_index_names=self.data_column_pandas_index_names,
                index_column_pandas_labels=self.index_column_pandas_labels,
                index_column_snowflake_quoted_identifiers=new_index_column_snowflake_quoted_identifiers,
            ),
            existing_id_to_new_id_mapping,
        )

    def apply_snowpark_function_to_data_columns(
        self, snowpark_func: Callable[[Any], SnowparkColumn]
    ) -> "InternalFrame":
        """
        Apply snowpark function callable to data columns of an InternalFrame.  The snowflake quoted identifiers
        are preserved.

        Arguments:
            snowpark_func: Snowpark function to apply to data columns of underlying snowpark df.

        Returns:
            InternalFrame with snowpark_func applies to data columns of original frame, all other columns remain unchanged.
        """
        new_internal_frame = self.update_snowflake_quoted_identifiers_with_expressions(
            {
                snowflake_quoted_identifier: snowpark_func(snowflake_quoted_identifier)
                for snowflake_quoted_identifier in self.data_column_snowflake_quoted_identifiers
            }
        ).frame
        return new_internal_frame

    def select_active_columns(self) -> "InternalFrame":
        """
        Select active columns of the current internal frame, the active columns include index + data columns,
        ordering columns and row position column if exists. This function is used to re-project all active columns
        in the ordered dataframe, and drop off unnecessary columns from the projected columns of the ordered dataframe.

        Returns:
            A new InternalFrame with the associated ordered dataframe contains the following projected columns:
                1) index + data columns
                2) ordering columns
                3) row position column if exists

        """
        active_column_quoted_identifiers = (
            self.index_column_snowflake_quoted_identifiers
            + self.data_column_snowflake_quoted_identifiers
        )
        # add the missing ordering columns
        active_column_quoted_identifiers += [
            quoted_identifier
            for quoted_identifier in self.ordering_column_snowflake_quoted_identifiers
            if quoted_identifier not in active_column_quoted_identifiers
        ]

        if (
            self.row_position_snowflake_quoted_identifier is not None
            and self.row_position_snowflake_quoted_identifier
            not in active_column_quoted_identifiers
        ):
            active_column_quoted_identifiers.append(
                self.row_position_snowflake_quoted_identifier
            )

        return InternalFrame.create(
            ordered_dataframe=self.ordered_dataframe.select(
                active_column_quoted_identifiers
            ),
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
            data_column_pandas_labels=self.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=self.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self.data_column_pandas_index_names,
        )

    def strip_duplicates(
        self: "InternalFrame", quoted_identifiers: list[str]
    ) -> "InternalFrame":
        """
        When assigning frames via index operations for duplicates only the last entry is used, as entries are repeatedly overwritten.
        For example writing a series to a key [0, 1, 0] with values [1,2,3] will put value 2 to position 1, and value 3 to position 0.
        This function strips the preceding index/value rows to emulate repeated writes.

        Args:
            quoted_identifiers: the column identifiers to use for creating individual groups from which to take the last element.

        Returns:
            new internal frame with unique index.
        """

        frame = self.ensure_row_position_column()

        # To remove the duplicates, first compute via windowing over index columns the value of the last row position.
        # with this join then select only the relevant rows. Note that an EXISTS subquery doesn't work here because
        # Snowflake fails with a non-supported subquery expression error for LAST_VALUE.
        # SELECT a.* EXCLUDE (pos) FROM df a JOIN (SELECT DISTINCT LAST_VALUE(pos) OVER
        # (PARTITION BY (idx, other_idx) ORDER BY pos) AS pos FROM df) b ON a.pos = b.pos;

        assert len(quoted_identifiers) == len(
            set(quoted_identifiers)
            & set(frame.ordered_dataframe.projected_column_snowflake_quoted_identifiers)
        ), "could not find all quoted identifiers in frame"

        relevant_last_value_row_positions_quoted_identifier = (
            frame.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=[ROW_POSITION_COLUMN_LABEL],
            )[0]
        )

        relevant_last_value_row_positions = get_distinct_rows(
            frame.ordered_dataframe.select(
                last_value(col(frame.row_position_snowflake_quoted_identifier))
                .over(
                    Window.partition_by(quoted_identifiers).order_by(
                        frame.row_position_snowflake_quoted_identifier
                    )
                )
                .as_(relevant_last_value_row_positions_quoted_identifier)
            )
        )

        joined_ordered_dataframe = frame.ordered_dataframe.join(
            right=relevant_last_value_row_positions,
            left_on_cols=[frame.row_position_snowflake_quoted_identifier],
            right_on_cols=[relevant_last_value_row_positions_quoted_identifier],
            how="inner",
        )

        # Because we reuse row position to select the relevant columns, we need to
        # generate a new row position column here so locational indexing after this operation
        # continues to work correctly.
        new_ordered_dataframe = joined_ordered_dataframe.ensure_row_position_column()
        return InternalFrame.create(
            ordered_dataframe=new_ordered_dataframe,
            data_column_pandas_labels=frame.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=frame.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=frame.data_column_pandas_index_names,
            index_column_pandas_labels=frame.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=frame.index_column_snowflake_quoted_identifiers,
        )

    def filter(
        self: "InternalFrame", expr: Union[SnowparkColumn, str]
    ) -> "InternalFrame":
        """
        A helper method to apply filter on the internal frame
        Args:
            expr: the expression of the filter

        Returns:
            The internal frame after filtering
        """
        return InternalFrame.create(
            ordered_dataframe=self.ordered_dataframe.filter(expr),
            data_column_pandas_labels=self.data_column_pandas_labels,
            data_column_snowflake_quoted_identifiers=self.data_column_snowflake_quoted_identifiers,
            data_column_pandas_index_names=self.data_column_pandas_index_names,
            index_column_pandas_labels=self.index_column_pandas_labels,
            index_column_snowflake_quoted_identifiers=self.index_column_snowflake_quoted_identifiers,
        )

    def normalize_snowflake_quoted_identifiers_with_pandas_label(
        self,
    ) -> "InternalFrame":
        """
        Normalize snowflake quoted identifiers for index and data columns based on the pandas label to make sure
        the quoted identifier is in format of <label> or <label>_<postfix>.

        Returns:
            A new internalFrame with the snowflake quoted identifiers for index and data columns all in
            the normalized format.
        """

        def is_quoted_identifier_normalized(
            pandas_label: Hashable, quoted_identifier: str
        ) -> bool:
            # a quoted identifier is viewed as normalized if its prefix is the quoted pandas label string
            quoted_label = quote_name_without_upper_casing(f"{pandas_label}")
            return quoted_identifier.startswith(quoted_label)

        # record all columns where snowflake quoted identifiers are not in normalized form.
        columns_to_rename: list[PandasLabelToSnowflakeIdentifierPair] = []
        for pandas_label, snowflake_quoted_identifier in zip(
            self.index_column_pandas_labels + self.data_column_pandas_labels,
            self.index_column_snowflake_quoted_identifiers
            + self.data_column_snowflake_quoted_identifiers,
        ):
            if pandas_label is None:
                # Replace empty/None labels with INDEX_LABEL or DEFAULT_DATA_COLUMN_LABEL before
                # generating snowflake identifiers.
                if (
                    snowflake_quoted_identifier
                    in self.index_column_snowflake_quoted_identifiers
                ):
                    pandas_label = INDEX_LABEL
                else:
                    pandas_label = DEFAULT_DATA_COLUMN_LABEL
            if not is_quoted_identifier_normalized(
                pandas_label, snowflake_quoted_identifier
            ):
                columns_to_rename.append(
                    PandasLabelToSnowflakeIdentifierPair(
                        pandas_label, snowflake_quoted_identifier
                    )
                )

        if len(columns_to_rename) == 0:
            # no columns to rename, return
            return self

        rename_column_labels, original_quoted_identifiers = tuple(
            zip(*columns_to_rename)
        )
        # generate normalized snowflake quoted identifiers based on pandas labels and
        # call rename_snowflake_identifiers to rename the columns.
        new_snowflake_quoted_identifiers = (
            self.ordered_dataframe.generate_snowflake_quoted_identifiers(
                pandas_labels=list(rename_column_labels),
            )
        )
        renamed_quoted_identifier_mapping = dict(
            zip(
                list(original_quoted_identifiers),
                new_snowflake_quoted_identifiers,
            )
        )
        return self.rename_snowflake_identifiers(renamed_quoted_identifier_mapping)

    # END: Internal Frame mutation APIs.
    ###########################################################################
