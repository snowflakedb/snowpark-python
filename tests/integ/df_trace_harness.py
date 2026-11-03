#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Harness that computes the debug output of the AST backend (frozen oracle) and of the
lightweight trace backend for the same error and plan."""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import snowflake.snowpark.context as context
from snowflake.snowpark._internal import debug_utils as lite
from snowflake.snowpark._internal.utils import set_ast_state, AstFlagSource
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.utils_debug import debug_ast_reference as oracle

TRACE_LENGTH_ENV = "SNOWPARK_PYTHON_DATAFRAME_TRANSFORM_TRACE_LENGTH"

# The path of the test files is part of the output; keep a stable way to hide the machine specific part.
_LOCATION = re.compile(r"\|\d+(:\d+-\d+:\d+)?")


@dataclass
class DebugOutput:
    lineage: str = ""
    lineage_lines: List[str] = field(default_factory=list)
    mapped: str = ""
    missing_object: str = ""
    existing_object: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "lineage": self.lineage,
            "mapped": self.mapped,
            "missing_object": self.missing_object,
            "existing_object": self.existing_object,
        }


@dataclass
class Observed:
    exception: SnowparkSQLException
    top_plan: Any
    oracle: DebugOutput
    lite: DebugOutput


def find_top_plan(exc: BaseException):
    """The plan wrap_exception was called with, found in the traceback frames."""
    tb = exc.__traceback__
    top_plan = None
    while tb is not None:
        frame = tb.tb_frame
        if frame.f_code.co_name == "wrap" and "top_plan" in frame.f_locals:
            plan = frame.f_locals["top_plan"]
            if plan is not None:
                top_plan = plan
        tb = tb.tb_next
    return top_plan


def _safe(fn, *args):
    try:
        return fn(*args)
    except Exception as e:  # noqa: BLE001
        return f"<<raised {type(e).__name__}: {e}>>"


def compute_oracle(top_plan, message: str) -> DebugOutput:
    out = DebugOutput()
    ids = top_plan.df_ast_ids
    if ids:
        out.lineage = _safe(
            oracle.get_df_transform_trace_message,
            ids[-1],
            top_plan.session._ast_batch._bind_stmt_cache,
        )
    if "error line" in message:
        out.mapped = _safe(oracle.get_python_source_from_sql_error, top_plan, message)
    if "does not exist or not authorized" in message:
        out.missing_object = _safe(oracle.get_missing_object_context, top_plan, message)
    if "already exists" in message:
        out.existing_object = _safe(
            oracle.get_existing_object_context, top_plan, message
        )
    return out


def compute_lite(top_plan, message: str) -> DebugOutput:
    out = DebugOutput()
    nodes = top_plan.df_trace_nodes
    if nodes:
        out.lineage = _safe(lite.get_df_transform_trace_message, nodes[-1])
    if "error line" in message:
        out.mapped = _safe(lite.get_python_source_from_sql_error, top_plan, message)
    if "does not exist or not authorized" in message:
        out.missing_object = _safe(lite.get_missing_object_context, top_plan, message)
    if "already exists" in message:
        out.existing_object = _safe(lite.get_existing_object_context, top_plan, message)
    return out


def observe(workload_fn, session, ctx, action) -> Observed:
    """Runs the workload with AST and the tracing features enabled and returns both backends' output
    for the error it raises."""
    try:
        obj = workload_fn(session, ctx)
        action(obj)
    except SnowparkSQLException as exc:  # the expected failure
        top_plan = find_top_plan(exc)
        if top_plan is None:
            raise AssertionError(
                "could not find the plan of the failure in the traceback"
            ) from exc
        message = exc.raw_message or exc.message or ""
        return Observed(
            exception=exc,
            top_plan=top_plan,
            oracle=compute_oracle(top_plan, message),
            lite=compute_lite(top_plan, message),
        )
    raise AssertionError("the workload did not fail")


class DebugFeatures:
    """Context manager enabling the tracing features and AST (for the oracle) with a long trace."""

    def __init__(self, ast: bool = True, trace_length: Optional[int] = 1000) -> None:
        self.ast = ast
        self.trace_length = trace_length

    def __enter__(self):
        import os

        self._old_env = os.environ.get(TRACE_LENGTH_ENV)
        if self.trace_length is not None:
            os.environ[TRACE_LENGTH_ENV] = str(self.trace_length)
        context.configure_development_features(
            enable_dataframe_trace_on_error=True,
            enable_trace_sql_errors_to_dataframe=True,
        )
        from snowflake.snowpark._internal.utils import is_ast_enabled

        self._old_ast = is_ast_enabled()
        set_ast_state(AstFlagSource.TEST, self.ast)
        return self

    def __exit__(self, *exc):
        import os

        context.configure_development_features()
        set_ast_state(AstFlagSource.TEST, self._old_ast)
        if self._old_env is None:
            os.environ.pop(TRACE_LENGTH_ENV, None)
        else:
            os.environ[TRACE_LENGTH_ENV] = self._old_env
        return False
