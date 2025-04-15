#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import base64
import itertools
import sys
import uuid
from collections import namedtuple
from dataclasses import dataclass
from typing import Callable, Optional

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto

from snowflake.snowpark.version import VERSION

# TODO(SNOW-1791994): Enable pyright type checks for this file.


# The current AST version number (generated by the DSL).
CLIENT_AST_VERSION = proto.__Version__.MAX_VERSION


@dataclass
class TrackedCallable:
    """
    Several Snowpark APIs that deal with stored procedures and user-defined functions accept callables as arguments.
    This class is a pair of a callable and an ID that is used to reference it in the AST. Distinct objects get distinct IDs.
    It is undesirable for the same callable to have multiple IDs due to constraints in other parts of the system.
    """

    bind_id: int
    func: Callable


SerializedBatch = namedtuple("SerializedBatch", ["request_id", "batch"])


# AstBatch is not thread safe by itself, but is thread compatible. All access to AstBatch should be synchronized through
# external means.
class AstBatch:
    """
    A batch of AST statements. This class is used to generate AST requests.

    The core statement types are:
    - Bind: Creates a new variable and assigns a value to it.
    - Eval: Evaluates a variable.
    """

    # Function used to generate request IDs. This is overridden in some tests.
    generate_request_id = uuid.uuid4

    def __init__(
        self,
        session: "snowflake.snowpark.Session",  # type: ignore[name-defined]  # noqa: F821
    ) -> None:
        """
        Initializes a new AST batch.

        Args:
            session: The Snowpark session.
        """
        self._session = session
        self.reset_id_gen()
        self._init_batch()

        # Track callables in this dict (memory id -> TrackedCallable).
        self._callables: dict[int, TrackedCallable] = {}

    def reset_id_gen(self) -> None:
        """Resets the ID generator."""
        self._id_gen = itertools.count(start=1)

    def bind(self, symbol: Optional[str] = None) -> proto.Bind:
        """
        Creates a new assignment statement.

        Args:
            symbol: An optional symbol to name the new variable.
        """
        stmt = self._request.body.add()
        # TODO: extended BindingId spec from the branch snowpark-ir.
        stmt.bind.first_request_id = (
            self._request_id.bytes
        )  # Set the first batch ID this stmt will be flushed with.
        stmt.bind.uid = self._get_next_id()
        stmt.bind.symbol.value = symbol if isinstance(symbol, str) else ""
        return stmt.bind

    def eval(self, target: proto.Bind) -> None:
        """
        Creates a new evaluation statement.

        Args:
            target: The variable to evaluate.
        """
        stmt = self._request.body.add()
        stmt.eval.bind_id = target.uid

    def flush(self) -> SerializedBatch:
        """Ties off a batch and starts a new one. Returns the tied-off batch."""
        req_id: str = str(self._request_id)

        # Only filenames are interned, flush the lookup table as part of the request.
        from snowflake.snowpark._internal.ast.utils import fill_interned_value_table

        fill_interned_value_table(self._request.interned_value_table)

        batch = str(base64.b64encode(self._request.SerializeToString()), "utf-8")
        self._init_batch()
        return SerializedBatch(req_id, batch)

    def _init_batch(self) -> None:
        # Reset the AST batch by initializing a new request.
        self._request_id = AstBatch.generate_request_id()  # Generate a new unique ID.
        self._request = proto.Request()
        self._request.id = (
            self._request_id.bytes
        )  # Convert UUID to bytes for the request.

        (major, minor, patch) = VERSION
        self._request.client_version.major = major
        self._request.client_version.minor = minor
        self._request.client_version.patch = patch

        (major, minor, micro, releaselevel, serial) = sys.version_info
        self._request.client_language.python_language.version.major = major
        self._request.client_language.python_language.version.minor = minor
        self._request.client_language.python_language.version.patch = micro
        self._request.client_language.python_language.version.label = releaselevel

        self._request.client_ast_version = CLIENT_AST_VERSION

    # TODO(SNOW-1491199) - This method is not covered by tests until the end of phase 0. Drop the pragma when it is covered.
    def register_callable(self, func: Callable) -> int:  # pragma: no cover
        """Tracks client-side an actual callable and returns an ID."""
        k = id(func)

        if k in self._callables.keys():
            return self._callables[k].bind_id

        next_id = len(self._callables)
        self._callables[k] = TrackedCallable(bind_id=next_id, func=func)
        return next_id

    def _get_next_id(self) -> int:
        """Returns the next ID from the generator."""
        return next(self._id_gen)
