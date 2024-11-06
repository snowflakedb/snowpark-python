#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import base64
import os
import subprocess
import traceback
from typing import List, Optional, Union

import pytest

import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
import snowflake.snowpark.functions
import snowflake.snowpark.types
import tests.integ.utils.sql_counter
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


def generate_error_trace_info(python_text, exception=None):
    error_msg = "\nOriginal stack trace:\n" + "".join(
        filter(
            lambda s: "site-package" not in s and "ast_test_utils" not in s,
            traceback.format_stack()
            if exception is None
            else "\n".join(traceback.format_tb(exception.__traceback__)),
        )
    )

    # Include the generated code in the result.
    error_msg = "*" * 80 + "\n" + python_text + "\n" + "*" * 80 + "\n" + error_msg

    return error_msg


def notify_compare_ast_validation(
    compare_ast_validation_listener: AstListener,
    query_record: QueryRecord,
    *args,
    **kwargs,
):
    # For multi-query, we only want to do the comparison query after the *last* query.
    if "dataframeAst" not in kwargs:
        return

    validation_session = compare_ast_validation_listener._validation_session

    # Get query_id for the original and generated code python dataframe code.
    qid_result1 = validation_session._ast_full_validation_result
    qid_result2 = query_record.query_id

    error_msg = ""
    success = False

    # If the original query failed, then we expect the validation query to also fail.  We do not bother checking
    # the specific exception here as that is checked later.
    if qid_result1 is None and qid_result2 is None:
        return
    elif qid_result1 is None or qid_result2 is None:
        # If only one result is None, that implies that one of them failed and the other succeeded.
        success = False
    else:
        # Compare the results from both queries, if 0 then equal otherwise not equal.
        comparison_sql = f"""
            select sum(*) from (
                select sum(*) as result1 from (select hash(*) from table(result_scan('{qid_result1}')))
                union all
                select -sum(*) as result2 from (select hash(*) from table(result_scan('{qid_result2}')))
            )
        """

        try:
            results_cursor = validation_session._conn._cursor.execute(comparison_sql)

            # Ensure we are using a new cursor to avoid side effects on the connection reusing cursor state with
            # the validation query.
            validation_cursor = results_cursor.connection.cursor()
            validation_cursor.get_results_from_sfqid(results_cursor.sfqid)

            compare_results = validation_cursor.fetchall()
            compare_diff = compare_results[0][0]

            # The results can be None if the query result is an empty set.
            success = compare_diff is None or compare_diff == 0
        except Exception as ex:
            error_msg = str(ex) + "\n"
            success = False

    if not success:
        if error_msg == "":
            error_msg = f"Full AST validation results differed.\n\nResult 1 Query Id: {qid_result1}\nResult 2 Query Id: {qid_result2}\n"

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
    """
    The full ast validation mode works by using the query listener to capture the AST that would be sent with the query.
    The captured AST is run through the AST -> python code unparser.  The generated python code is then executed in a
    separate validation snowpark session.  The results of the two query executions are compared to see if they are
    equal.  Any failure during this process (other than an intentional bad api call) or different in results indicates
    a failure in the end to end query => AST => unparser => execution path.

    Args:
        full_ast_validation_listener: The query listener object that contains the contextual information for session.
        query_record: The query record containing the AST payload.
        args, kwargs: Extra arguments that may be passed throuhg to the underlying listener's notify method.
    """

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
    if test_name is not None and "(setup)" in test_name:
        return

    base64_batches = full_ast_validation_listener.base64_batches
    # Some queries won't have AST attached because they are part of a multi-query action.  Only the last query should
    # send dataframe AST to ensure it is executed only once.
    if not base64_batches:
        return

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
        if isinstance(ex, subprocess.CalledProcessError):
            error_details = "REPRO CMD:\n" + " ".join(ex.cmd)
        else:
            error_details = str(ex)
        error_msg = "Java unparser execution failed.\n" + generate_error_trace_info(
            error_details
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
        if "exception" in kwargs:
            validation_ex = ex.conn_error if hasattr(ex, "conn_error") else ex
            if not isinstance(kwargs["exception"], type(validation_ex)):
                error_msg = (
                    f"Original exception: {kwargs['exception']}\nValidation exception: {validation_ex}\n"
                    + generate_error_trace_info(python_code_output)
                )
                pytest.assume(
                    False,
                    f"""Full AST validation failed, failure exceptions do not match.\n{error_msg}""",
                )
            else:
                # The original query failed with same exception as validation query, so this is expected.
                pass
        else:
            error_msg = (
                str(ex) + "\n" + generate_error_trace_info(python_code_output, ex)
            )
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
    validation_session.ast_enabled = True
    validation_session.full_ast_validation = False

    compare_ast_validation_listener = validation_session.ast_listener(True)

    def notify_compare_ast(query_record: QueryRecord, *args, **kwargs):
        notify_compare_ast_validation(
            compare_ast_validation_listener, query_record, *args, **kwargs
        )

    compare_ast_validation_listener._original_notify = (
        compare_ast_validation_listener._notify
    )
    compare_ast_validation_listener._notify = notify_compare_ast
    compare_ast_validation_listener._validation_session = validation_session

    full_ast_validation_listener = session.ast_listener(True)
    full_ast_validation_listener._original_notify = full_ast_validation_listener._notify

    def notify_full_ast_validation(query_record: QueryRecord, *args, **kwargs):
        notify_full_ast_validation_with_listener(
            full_ast_validation_listener, query_record, *args, **kwargs
        )

    full_ast_validation_listener._compare_ast_validation_listener = (
        compare_ast_validation_listener
    )
    full_ast_validation_listener._notify = notify_full_ast_validation
    full_ast_validation_listener._validation_session = validation_session
    full_ast_validation_listener._unparser_jar = unparser_jar
    full_ast_validation_listener._globals = vars(snowflake.snowpark.functions) | vars(
        snowflake.snowpark.types
    )
    full_ast_validation_listener._current_test = ""
    full_ast_validation_listener._prev_stmts = {}

    # Ensure sql counter uses the original session and not the validation session.
    tests.integ.utils.sql_counter._active_session = session

    return full_ast_validation_listener


def close_full_ast_validation_mode(full_ast_validation_listener):
    # Remove the test hook for full ast validation so does not run for any clean up work.
    compare_ast_validation_listener = (
        full_ast_validation_listener._compare_ast_validation_listener
    )
    compare_ast_validation_listener._notify = (
        compare_ast_validation_listener._original_notify
    )
    full_ast_validation_listener._notify = full_ast_validation_listener._original_notify
    full_ast_validation_listener._validation_session.close()
    tests.integ.utils.sql_counter._active_session = None
