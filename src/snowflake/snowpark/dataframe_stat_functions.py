#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from functools import reduce
from typing import Dict, List, Optional, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.ast_pb2 as proto
from snowflake.snowpark import Column
from snowflake.snowpark._internal.ast_utils import (
    FAIL_ON_MISSING_AST,
    build_expr_from_snowpark_column_or_col_name,
    with_src_position,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import adjust_api_subcalls
from snowflake.snowpark._internal.type_utils import ColumnOrName, LiteralType
from snowflake.snowpark.functions import (
    _to_col_if_str,
    approx_percentile_accumulate,
    approx_percentile_estimate,
    corr as corr_func,
    count,
    count_distinct,
    covar_samp,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

from logging import getLogger

_logger = getLogger(__name__)

_MAX_COLUMNS_PER_TABLE = 1000


class DataFrameStatFunctions:
    """Provides computed statistical functions for DataFrames.
    To access an object of this class, use :attr:`DataFrame.stat`.
    """

    def __init__(
        self,
        df: "snowflake.snowpark.DataFrame",
        _ast_stmt: Optional[proto.Assign] = None,
    ) -> None:
        self._dataframe = df
        self._ast_stmt = _ast_stmt

    def approx_quantile(
        self,
        col: Union[ColumnOrName, Iterable[ColumnOrName]],
        percentile: Iterable[float],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        _emit_ast: bool = True,
    ) -> Union[List[float], List[List[float]]]:
        """For a specified numeric column and a list of desired quantiles, returns an approximate value for the column at each of the desired quantiles.
        This function uses the t-Digest algorithm.

        Examples::

            >>> df = session.create_dataframe([1, 2, 3, 4, 5, 6, 7, 8, 9, 0], schema=["a"])
            >>> df.stat.approx_quantile("a", [0, 0.1, 0.4, 0.6, 1])  # doctest: +SKIP

            >>> df2 = session.create_dataframe([[0.1, 0.5], [0.2, 0.6], [0.3, 0.7]], schema=["a", "b"])
            >>> df2.stat.approx_quantile(["a", "b"], [0, 0.1, 0.6])  # doctest: +SKIP

        Args:
            col: The name of the numeric column.
            percentile: A list of float values greater than or equal to 0.0 and less than 1.0.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
             A list of approximate percentile values if ``col`` is a single column name, or a matrix
             with the dimensions ``(len(col) * len(percentile)`` containing the
             approximate percentile values if ``col`` is a list of column names.
        """

        if not percentile or not col:
            return []

        if _emit_ast:
            # Add an assign node that applies SpDataframeStatsApproxQuantile() to the input, followed by its Eval.
            repr = self._dataframe._session._ast_batch.assign()
            expr = with_src_position(repr.expr.sp_dataframe_stats_approx_quantile, repr)

            if self._ast_stmt is None and FAIL_ON_MISSING_AST:
                raise NotImplementedError(
                    f"DataFrame with API usage {self._dataframe._plan.api_calls} is missing complete AST logging."
                )

            expr.id.bitfield1 = self._ast_stmt.var_id.bitfield1

            if isinstance(col, Iterable) and not isinstance(col, str):
                for c in col:
                    build_expr_from_snowpark_column_or_col_name(expr.cols.add(), c)
            else:
                build_expr_from_snowpark_column_or_col_name(expr.cols.add(), col)

            # Because we build AST at beginning, error out if not iterable.
            if not isinstance(percentile, Iterable):
                raise ValueError(
                    f"percentile is of type {type(percentile)}, but expected Iterable."
                )

            expr.percentile.extend(percentile)

            if statement_params is not None:
                for k, v in statement_params.items():
                    t = expr.statement_params.add()
                    t._1 = k
                    t._2 = v

            self._dataframe._session._ast_batch.eval(repr)

        if self._dataframe._session._conn.is_phase1_enabled():
            # TODO: Logic here should be
            # ast = self._dataframe._session._ast_batch.flush()
            # res = self._dataframe._session._conn.ast_query(ast)
            raise NotImplementedError(
                "TODO: Implement approx_quantile() with EvalResult in Phase1."
            )

        # Phase 0 flushes AST and encodes it as part of the query.
        kwargs = {}
        _, kwargs["_dataframe_ast"] = self._dataframe._session._ast_batch.flush()

        temp_col_name = "t"
        if isinstance(col, (Column, str)):
            df = self._dataframe.select(
                approx_percentile_accumulate(col).as_(temp_col_name), _emit_ast=False
            ).select(
                [approx_percentile_estimate(temp_col_name, p) for p in percentile],
                _emit_ast=False,
            )
            adjust_api_subcalls(
                df, "DataFrameStatFunctions.approx_quantile", len_subcalls=2
            )
            res = df._internal_collect_with_tag(
                statement_params=statement_params, **kwargs
            )
            return list(res[0])
        elif isinstance(col, (list, tuple)):
            accumate_cols = [
                approx_percentile_accumulate(col_i).as_(f"{temp_col_name}_{i}")
                for i, col_i in enumerate(col)
            ]
            output_cols = [
                approx_percentile_estimate(f"{temp_col_name}_{i}", p)
                for i in range(len(accumate_cols))
                for p in percentile
            ]
            percentile_len = len(output_cols) // len(accumate_cols)
            df = self._dataframe.select(accumate_cols, _emit_ast=False).select(
                output_cols, _emit_ast=False
            )
            adjust_api_subcalls(
                df, "DataFrameStatFunctions.approx_quantile", len_subcalls=2
            )
            res = df._internal_collect_with_tag(
                statement_params=statement_params, **kwargs
            )
            return [
                [x for x in res[0][j * percentile_len : (j + 1) * percentile_len]]
                for j in range(len(accumate_cols))
            ]
        else:
            raise TypeError(  # pragma: no cover
                "'col' must be a column name, a column object, or a list of them."
            )

    def corr(
        self,
        col1: ColumnOrName,
        col2: ColumnOrName,
        *,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Optional[float]:
        """Calculates the correlation coefficient for non-null pairs in two numeric columns.

        Example::

            >>> df = session.create_dataframe([[0.1, 0.5], [0.2, 0.6], [0.3, 0.7]], schema=["a", "b"])
            >>> df.stat.corr("a", "b")
            0.9999999999999991

        Args:
            col1: The name of the first numeric column to use.
            col2: The name of the second numeric column to use.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Return:
            The correlation of the two numeric columns.
            If there is not enough data to generate the correlation, the method returns ``None``.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """
        df = self._dataframe.select(corr_func(col1, col2))
        adjust_api_subcalls(df, "DataFrameStatFunctions.corr", len_subcalls=1)
        res = df._internal_collect_with_tag(statement_params=statement_params)
        return res[0][0] if res[0] is not None else None

    def cov(
        self,
        col1: ColumnOrName,
        col2: ColumnOrName,
        *,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> Optional[float]:
        """Calculates the sample covariance for non-null pairs in two numeric columns.

        Example::

           >>> df = session.create_dataframe([[0.1, 0.5], [0.2, 0.6], [0.3, 0.7]], schema=["a", "b"])
           >>> df.stat.cov("a", "b")
           0.010000000000000037

        Args:
            col1: The name of the first numeric column to use.
            col2: The name of the second numeric column to use.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Return:
            The sample covariance of the two numeric columns.
            If there is not enough data to generate the covariance, the method returns None.
        """
        df = self._dataframe.select(covar_samp(col1, col2))
        adjust_api_subcalls(df, "DataFrameStatFunctions.corr", len_subcalls=1)
        res = df._internal_collect_with_tag(statement_params=statement_params)
        return res[0][0] if res[0] is not None else None

    def crosstab(
        self,
        col1: ColumnOrName,
        col2: ColumnOrName,
        *,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> "snowflake.snowpark.DataFrame":
        """Computes a pair-wise frequency table (a ``contingency table``) for the specified columns.
        The method returns a DataFrame containing this table.

        In the returned contingency table:
            - The first column of each row contains the distinct values of ``col1``.
            - The name of the first column is the name of ``col1``.
            - The rest of the column names are the distinct values of ``col2``.
            - For pairs that have no occurrences, the contingency table contains 0 as the count.

        Note:
            The number of distinct values in ``col2`` should not exceed 1000.

        Example::

            >>> df = session.create_dataframe([(1, 1), (1, 2), (2, 1), (2, 1), (2, 3), (3, 2), (3, 3)], schema=["key", "value"])
            >>> ct = df.stat.crosstab("key", "value").sort(df["key"])
            >>> ct.show()
            ---------------------------------------------------------------------------------------------
            |"KEY"  |"CAST(1 AS NUMBER(38,0))"  |"CAST(2 AS NUMBER(38,0))"  |"CAST(3 AS NUMBER(38,0))"  |
            ---------------------------------------------------------------------------------------------
            |1      |1                          |1                          |0                          |
            |2      |2                          |0                          |1                          |
            |3      |0                          |1                          |1                          |
            ---------------------------------------------------------------------------------------------
            <BLANKLINE>

        Args:
            col1: The name of the first column to use.
            col2: The name of the second column to use.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
        """
        row_count = self._dataframe.select(
            count_distinct(col2)
        )._internal_collect_with_tag(statement_params=statement_params)[0][0]
        if row_count > _MAX_COLUMNS_PER_TABLE:
            raise SnowparkClientExceptionMessages.DF_CROSS_TAB_COUNT_TOO_LARGE(
                row_count, _MAX_COLUMNS_PER_TABLE
            )
        column_names = [
            row[0]
            for row in self._dataframe.select(col2)
            .distinct()
            ._internal_collect_with_tag(statement_params=statement_params)
        ]
        df = (
            self._dataframe.select(col1, col2)
            .pivot(col2, column_names)
            .agg(count(col2))
        )
        adjust_api_subcalls(df, "DataFrameStatFunctions.crosstab", len_subcalls=3)
        return df

    def sample_by(
        self, col: ColumnOrName, fractions: Dict[LiteralType, float]
    ) -> "snowflake.snowpark.DataFrame":
        """Returns a DataFrame containing a stratified sample without replacement, based on a ``dict`` that specifies the fraction for each stratum.

        Example::

            >>> df = session.create_dataframe([("Bob", 17), ("Alice", 10), ("Nico", 8), ("Bob", 12)], schema=["name", "age"])
            >>> fractions = {"Bob": 0.5, "Nico": 1.0}
            >>> sample_df = df.stat.sample_by("name", fractions)  # non-deterministic result

        Args:
            col: The name of the column that defines the strata.
            fractions: A ``dict`` that specifies the fraction to use for the sample for each stratum.
                If a stratum is not specified in the ``dict``, the method uses 0 as the fraction.
        """
        if not fractions:
            res_df = self._dataframe.limit(0)
            adjust_api_subcalls(
                res_df, "DataFrameStatFunctions.sample_by", len_subcalls=1
            )
            return res_df
        col = _to_col_if_str(col, "sample_by")
        res_df = reduce(
            lambda x, y: x.union_all(y),
            [self._dataframe.filter(col == k).sample(v) for k, v in fractions.items()],
        )
        adjust_api_subcalls(
            res_df,
            "DataFrameStatFunctions.sample_by",
            precalls=self._dataframe._plan.api_calls,
            subcalls=res_df._plan.api_calls.copy(),
        )
        return res_df

    approxQuantile = approx_quantile
    sampleBy = sample_by
