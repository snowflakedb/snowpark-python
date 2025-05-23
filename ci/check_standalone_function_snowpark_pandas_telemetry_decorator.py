#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import ast


class DecoratorError(Exception):
    pass


def check_standalone_function_snowpark_pandas_telemetry_decorator(
    target_file: str,
    telemetry_decorator_name: str,
) -> None:
    """
    Check if all standalone functions in the target file have been decorated by the decorator with
    name telemetry_decorator_name.
    Raises a DecoratorError if the decorator is missing.

    Args:
        target_file (str): Path to the target file.
        telemetry_decorator_name: Name of the telemetry decorator that is checked.
    """
    # Get the source code of the target file
    with open(target_file) as file:
        source_code = file.read()
    assert source_code.strip(), f"Source code in '{target_file}' is empty."
    # Parse the abstract syntax tree
    tree = ast.parse(source_code)

    # List of str: function names that need the decorator.
    failed_funcs = []

    # Apply the decorator to the functions with matching return types
    # Exclude sub-functions with iter_child_nodes which yields direct child nodes
    for node in ast.iter_child_nodes(tree):
        if (
            isinstance(node, ast.FunctionDef)  # Check if it is function type
            and not node.name.startswith(
                "_"
            )  # the function is not private (does not start with an underscore)
            and node.name
        ):
            has_telemetry_decorator = False
            for decorator in node.decorator_list:
                if (
                    hasattr(decorator, "id")
                    and decorator.id == telemetry_decorator_name
                ):
                    has_telemetry_decorator = True
                    break
            if not has_telemetry_decorator:
                failed_funcs.append(node.name)
    if len(failed_funcs) > 0:
        raise DecoratorError(
            f"functions {failed_funcs} should be decorated with {telemetry_decorator_name}"
        )


if __name__ == "__main__":
    check_standalone_function_snowpark_pandas_telemetry_decorator(
        target_file="src/snowflake/snowpark/modin/pandas/io.py",
        telemetry_decorator_name="snowpark_pandas_telemetry_standalone_function_decorator",
    )
    check_standalone_function_snowpark_pandas_telemetry_decorator(
        target_file="src/snowflake/snowpark/modin/plugin/extensions/general_overrides.py",
        telemetry_decorator_name="snowpark_pandas_telemetry_standalone_function_decorator",
    )
    check_standalone_function_snowpark_pandas_telemetry_decorator(
        target_file="src/snowflake/snowpark/modin/plugin/extensions/pd_extensions.py",
        telemetry_decorator_name="snowpark_pandas_telemetry_standalone_function_decorator",
    )
    check_standalone_function_snowpark_pandas_telemetry_decorator(
        target_file="src/snowflake/snowpark/modin/plugin/extensions/pd_overrides.py",
        telemetry_decorator_name="snowpark_pandas_telemetry_standalone_function_decorator",
    )
