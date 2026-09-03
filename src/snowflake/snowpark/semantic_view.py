#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import TYPE_CHECKING, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.utils import quote_name

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session  # pragma: no cover

MemberRef = Union[str, Tuple[str, str]]


def _render_member(ref: MemberRef) -> str:
    if isinstance(ref, tuple):
        member, alias = ref
        return f"{member} AS {quote_name(alias)}"
    return ref


class SemanticViewQuery:
    """
    A query being built against one semantic view.

    Obtained by calling a builder method on :class:`SemanticView`. Each builder
    method returns a new :class:`SemanticViewQuery`, so calls can be chained;
    :meth:`to_df` turns the accumulated clauses into a
    :class:`~snowflake.snowpark.DataFrame`.
    """

    def __init__(
        self,
        session: "Session",
        fqn: str,
        metrics: Tuple[MemberRef, ...] = (),
        facts: Tuple[MemberRef, ...] = (),
        dimensions: Tuple[MemberRef, ...] = (),
        where: Tuple[str, ...] = (),
    ) -> None:
        self._session = session
        self._fqn = fqn
        self._metrics = metrics
        self._facts = facts
        self._dimensions = dimensions
        self._where = where

    def facts(self, *facts: MemberRef) -> "SemanticViewQuery":
        """Returns a new query with this ``FACTS`` clause.

        A later call replaces the previous facts; it does not append.
        Member strings are copied into the SQL text as-is.

        Combining ``facts()`` with ``dimensions()`` currently folds into
        ``ANY_VALUE()`` on the server (any row per group; five runs can
        disagree if the result cache is off). A BCR is turning that into
        an error. This method does not raise for that combination.
        """
        return SemanticViewQuery(
            self._session,
            self._fqn,
            metrics=self._metrics,
            facts=tuple(facts),
            dimensions=self._dimensions,
            where=self._where,
        )

    def metrics(self, *metrics: MemberRef) -> "SemanticViewQuery":
        """Returns a new query with this ``METRICS`` clause.

        A later call replaces the previous metrics; it does not append.
        Member strings are copied into the SQL text as-is.
        """
        return SemanticViewQuery(
            self._session,
            self._fqn,
            metrics=tuple(metrics),
            facts=self._facts,
            dimensions=self._dimensions,
            where=self._where,
        )

    def dimensions(self, *dimensions: MemberRef) -> "SemanticViewQuery":
        """Returns a new query with this ``DIMENSIONS`` clause.

        A later call replaces the previous dimensions; it does not append.
        Member strings are copied into the SQL text as-is.
        """
        return SemanticViewQuery(
            self._session,
            self._fqn,
            metrics=self._metrics,
            facts=self._facts,
            dimensions=tuple(dimensions),
            where=self._where,
        )

    def where(self, condition: str) -> "SemanticViewQuery":
        """Returns a new query with this pre-aggregation ``WHERE`` condition.

        A later call is AND-ed with the previous conditions; it does not
        replace them. The condition string is copied into the SQL text as-is.
        """
        return SemanticViewQuery(
            self._session,
            self._fqn,
            metrics=self._metrics,
            facts=self._facts,
            dimensions=self._dimensions,
            where=self._where + (condition,),
        )

    def to_df(self) -> "snowflake.snowpark.dataframe.DataFrame":
        """Returns the query as a :class:`~snowflake.snowpark.DataFrame`.

        Raises:
            ValueError: if no clause is set. No query is sent.
        """
        if not (self._metrics or self._facts or self._dimensions or self._where):
            raise ValueError(
                "SemanticViewQuery.to_df() requires at least one of "
                "facts, metrics, dimensions, or where"
            )
        parts = [self._fqn]
        if self._metrics:
            parts.append(
                "METRICS " + ", ".join(_render_member(m) for m in self._metrics)
            )
        if self._facts:
            parts.append("FACTS " + ", ".join(_render_member(f) for f in self._facts))
        if self._dimensions:
            parts.append(
                "DIMENSIONS " + ", ".join(_render_member(d) for d in self._dimensions)
            )
        if self._where:
            parts.append("WHERE " + " AND ".join(self._where))
        sql = f"SELECT * FROM SEMANTIC_VIEW({' '.join(parts)})"
        return self._session.sql(sql)


class SemanticView:
    """
    A handle to one semantic view.

    Constructing a handle does not reach the server; the semantic view is resolved when a method on the handle is called.
    """

    def __init__(self, session: "Session", fqn: str) -> None:
        self._session = session
        self._fqn = fqn

    @property
    def fqn(self) -> str:
        """The fully qualified name (``database.schema.name``) of this semantic view."""
        return self._fqn

    def facts(self, *facts: MemberRef) -> SemanticViewQuery:
        """Starts a query on this semantic view with a ``FACTS`` clause."""
        return SemanticViewQuery(self._session, self._fqn).facts(*facts)

    def metrics(self, *metrics: MemberRef) -> SemanticViewQuery:
        """Starts a query on this semantic view with a ``METRICS`` clause."""
        return SemanticViewQuery(self._session, self._fqn).metrics(*metrics)

    def dimensions(self, *dimensions: MemberRef) -> SemanticViewQuery:
        """Starts a query on this semantic view with a ``DIMENSIONS`` clause."""
        return SemanticViewQuery(self._session, self._fqn).dimensions(*dimensions)

    def where(self, condition: str) -> SemanticViewQuery:
        """Starts a query on this semantic view with a ``WHERE`` clause."""
        return SemanticViewQuery(self._session, self._fqn).where(condition)

    def ddl(self) -> str:
        """Returns the ``CREATE SEMANTIC VIEW`` statement for this semantic view."""
        raise NotImplementedError("SemanticView.ddl is not implemented yet.")
