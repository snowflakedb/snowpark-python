#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import base64
import os
import subprocess
import traceback
from typing import List, Optional, Union

import pytest

import snowflake.snowpark._internal.proto.ast_pb2 as proto
import snowflake.snowpark.functions
import snowflake.snowpark.types
from snowflake.snowpark import Session
from snowflake.snowpark._internal.ast_utils import base64_lines_to_request
from snowflake.snowpark.query_history import AstListener, QueryRecord


def render(ast_base64: Union[str, List[str]], unparser_jar: Optional[str]) -> str:
    """Uses the unparser to render the AST."""
    assert (
        unparser_jar
    ), "A valid Unparser JAR path must be supplied either via --unparser-jar=<path> or the environment variable SNOWPARK_UNPARSER_JAR"

    if isinstance(ast_base64, str):
        ast_base64 = [ast_base64]

    res = subprocess.run(
        [
            "java",
            "-cp",
            unparser_jar,
            "com.snowflake.snowpark.experimental.unparser.UnparserCli",
            ",".join(
                ast_base64
            ),  # base64 strings will not contain , so pass multiple batches comma-separated.
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    return res.stdout


def generate_error_trace_info(python_text):
    error_msg = "\nOriginal stack trace:\n" + "".join(
        filter(
            lambda s: "site-package" not in s and "ast_test_utils" not in s,
            traceback.format_stack(),
        )
    )

    # Include the generated code in the result.
    error_msg = "*" * 80 + "\n" + python_text + "\n" + "*" * 80 + "\n" + error_msg

    return error_msg


def notify_compare_ast_validation_with_session(
    validation_session: Session, query_record: QueryRecord, *args, **kwargs
):
    # Get query_id for the original and generated code python dataframe code.
    qid_result1 = validation_session._ast_full_validation_result
    qid_result2 = query_record.query_id

    # Compare the results from both queries, if 0 then equal otherwise not equal.
    comparison_sql = f"""
        select sum(*) from (
            select sum(*) as result1 from (select hash(*) from table(result_scan('{qid_result1}')))
            union all
            select -sum(*) as result2 from (select hash(*) from table(result_scan('{qid_result2}')))
        )
    """

    error_msg = ""
    success = False
    try:
        results_cursor = validation_session._conn._cursor.execute(comparison_sql)
        compare_results = validation_session._conn._to_data_or_iter(
            results_cursor, False, False
        )
        compare_diff = compare_results["data"][0][0]
        success = compare_diff == 0
        if not success:
            error_msg = f"Full AST validation results differed.\n\nResult 1 Query Id: {qid_result1}\nResult 2 Query Id: {qid_result2}\n"
    except Exception as ex:
        error_msg = str(ex) + "\n"
        success = False

    if not success:
        error_msg = error_msg + generate_error_trace_info(
            validation_session._debug_python_code_output
        )

    pytest.assume(
        success,
        f"""Full AST validation failed.\n{error_msg}""",
    )


def notify_full_ast_validation_with_listener(
    full_ast_validation_listener: AstListener,
    query_record: QueryRecord,
    *args,
    **kwargs,
):
    # For tests that contain multiple actions, it's possible a later action depends on some dataframe code that
    # was provided as part of an earlier action.  Therefore we must keep all the previous statements and replay
    # them (excluding prior actions that already executed) in case there is a dependency.  This ensures the unparser
    # can reconstruct the full python code for execution.

    # Call the original listener notify method
    full_ast_validation_listener._original_notify(query_record, *args, **kwargs)

    # Obtain the test name from the scope of the test method.
    test_name = os.environ.get("PYTEST_CURRENT_TEST")

    # If this is part of the setup then we can skip, as we don't want to run twice in validation mode.  Some setup
    # code directly calls internal functions that circumvent the AST path.
    if "(setup)" in test_name:
        return

    base64_batches = full_ast_validation_listener.base64_batches
    cur_request = base64_lines_to_request(base64_batches[0])
    first_stmt = cur_request.body[0]
    first_uid = (
        first_stmt.eval.uid if first_stmt.eval.uid != 0 else first_stmt.assign.uid
    )

    if test_name != full_ast_validation_listener._current_test:
        full_ast_validation_listener._prev_stmts = {}
        full_ast_validation_listener._current_test = test_name

    # Create list of previous statements that may be required for this request.  To keep it simple, we just include
    # all prior statements (except for actions) that occurred previously in this action.  This may create dataframes
    # that are not used, but there shouldn't be a downside to that.  This can be improved further to walk the request
    # and only include the subset of statements for which a dependency exists for this test method (future improvement)
    prev_stmts = [
        stmt_val
        for stmt_id, stmt_val in full_ast_validation_listener._prev_stmts.items()
        if stmt_id < first_uid
    ]

    # Create new request including dependencies.
    request = proto.Request()
    request.client_language.python_language.version.major = 3
    request.client_language.python_language.version.minor = 9
    request.client_language.python_language.version.patch = 1
    request.client_language.python_language.version.label = "final"

    for next_stmt in prev_stmts + list(cur_request.body):
        stmt = request.body.add()
        stmt.CopyFrom(next_stmt)

    base64_batches = str(base64.b64encode(request.SerializeToString()), "utf-8")

    eval_stmt = request.body[len(request.body) - 1]
    prev_stmts = [
        stmt
        for stmt in request.body
        if stmt.eval.uid != eval_stmt.eval.uid
        and stmt.assign.uid != eval_stmt.eval.var_id.bitfield1
    ]
    full_ast_validation_listener._prev_stmts = {
        stmt.eval.uid if stmt.eval.uid != 0 else stmt.assign.uid: stmt
        for stmt in prev_stmts
    }

    # Unparse the AST into python code and execute in the validation_session.  This will notify the validation
    # session query listener which will compute the diff and assert equals original.
    try:
        python_code_output = render(
            base64_batches, full_ast_validation_listener._unparser_jar
        )
    except Exception as ex:
        error_msg = (
            str(ex)
            + "\n"
            + generate_error_trace_info("Java unparser execution failed.")
        )
        pytest.assume(
            False,
            f"""Full AST validation failed, could not run java unparser.\n{error_msg}""",
        )
        full_ast_validation_listener._ast_batches.clear()
        return

    full_ast_validation_listener._validation_session._ast_full_validation_result = (
        query_record.query_id
    )
    full_ast_validation_listener._validation_session._debug_python_code_output = (
        python_code_output
    )

    globals_dict = full_ast_validation_listener._globals
    globals_dict["session"] = full_ast_validation_listener._validation_session
    try:
        exec(python_code_output, globals_dict, globals_dict)
    except Exception as ex:
        error_msg = str(ex) + "\n" + generate_error_trace_info(python_code_output)
        pytest.assume(
            False,
            f"""Full AST validation failed, could not run unparser generated python code.\n{error_msg}""",
        )

    # This batch has been processed so let's clear.
    full_ast_validation_listener._ast_batches.clear()


def setup_full_ast_validation_mode(session, db_parameters, unparser_jar):
    validation_session = (
        Session.builder.configs(db_parameters).config("local_testing", False).create()
    )
    validation_session.sql_simplifier_enabled = session._sql_simplifier_enabled
    validation_session._cte_optimization_enabled = session.cte_optimization_enabled
    validation_session.ast_enabled = False
    validation_session.full_ast_validation = False

    compare_ast_validation_listener = validation_session.ast_listener()

    def notify_compare_ast_with_session(query_record: QueryRecord, *args, **kwargs):
        notify_compare_ast_validation_with_session(
            validation_session, query_record, *args, **kwargs
        )

    compare_ast_validation_listener._notify = notify_compare_ast_with_session

    full_ast_validation_listener = session.ast_listener()
    full_ast_validation_listener._original_notify = full_ast_validation_listener._notify

    def notify_full_ast_validation(query_record: QueryRecord, *args, **kwargs):
        notify_full_ast_validation_with_listener(
            full_ast_validation_listener, query_record, *args, **kwargs
        )

    full_ast_validation_listener._notify = notify_full_ast_validation
    full_ast_validation_listener._validation_session = validation_session
    full_ast_validation_listener._unparser_jar = unparser_jar
    full_ast_validation_listener._globals = vars(snowflake.snowpark.functions) | vars(
        snowflake.snowpark.types
    )
    full_ast_validation_listener._current_test = ""
    full_ast_validation_listener._prev_stmts = {}

    return full_ast_validation_listener


def close_full_ast_validation_mode(full_ast_validation_listener):
    # Remove the test hook for full ast validation so does not run for any clean up work.
    full_ast_validation_listener._notify = full_ast_validation_listener._original_notify
    full_ast_validation_listener._validation_session.close()
