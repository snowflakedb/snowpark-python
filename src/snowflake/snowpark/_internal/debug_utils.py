#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import cached_property
import os

from snowflake.snowpark._internal.ast.batch import get_dependent_bind_ids
from snowflake.snowpark._internal.ast.utils import __STRING_INTERNING_MAP__

UNKNOWN_FILE = "__UNKNOWN_FILE__"


class DataFrameLineageNode:
    """A node representing a dataframe operation in the DAG that represents the lineage of a DataFrame."""

    def __init__(self, batch_id: int, stmt_cache) -> None:
        self.batch_id = batch_id
        self.stmt_cache = stmt_cache

    @cached_property
    def children(self) -> set[int]:
        """Returns the batch_ids of the children of this node."""
        return get_dependent_bind_ids(self.stmt_cache[self.batch_id])

    def get_src(self):
        """The source Stmt of the DataFrame described by the batch_id."""
        stmt = self.stmt_cache[self.batch_id]
        api_call = stmt.bind.expr.WhichOneof("variant")
        return (
            getattr(stmt.bind.expr, api_call).src
            if api_call and getattr(stmt.bind.expr, api_call).HasField("src")
            else None
        )

    def _read_file(
        self, filename, start_line, end_line, start_column, end_column
    ) -> str:
        """Read the relevant code snippets of where the DataFrame was created. The filename given here
        must have read permissions for the executing user."""
        with open(filename) as f:
            code_lines = []
            for i, line in enumerate(f, 1):
                if end_line == 0:
                    if i == start_line:
                        code_lines.append(line)
                        break
                else:
                    if start_line <= i <= end_line:
                        code_lines.append(line)
                    elif i > end_line:
                        break

            if end_line != 0:
                # Should we make this inference based on python version?
                # If the end line is not 0, we need to trim the start and end columns
                if start_line == end_line:
                    code_lines[0] = code_lines[0][start_column:end_column]
                else:
                    code_lines[0] = code_lines[0][start_column:]
                    code_lines[-1] = code_lines[-1][:end_column]

            code_lines = [line.rstrip() for line in code_lines]
            return "\n".join(code_lines)

    def get_source_id(self) -> str:
        """Unique identifier of the location of the DataFrame creation in the source code."""
        src = self.get_src()
        if src is None:
            return ""

        fileno = src.file
        start_line = src.start_line
        start_column = src.start_column
        end_line = src.end_line
        end_column = src.end_column
        return f"{fileno}:{start_line}:{start_column}-{end_line}:{end_column}"

    def get_source_snippet(self) -> str:
        """Read the source file and extract the snippet where the dataframe is created."""
        src = self.get_src()
        if src is None:
            return "No source"

        # get the latest mapping of fileno to filename
        _fileno_to_filename_map = {v: k for k, v in __STRING_INTERNING_MAP__.items()}
        fileno = src.file
        filename = _fileno_to_filename_map.get(fileno, UNKNOWN_FILE)

        start_line = src.start_line
        end_line = src.end_line
        start_column = src.start_column
        end_column = src.end_column

        # Build the code identifier to find the operations where the DataFrame was created
        if end_column == 0:
            code_identifier = f"{filename}|{start_line}"
        else:
            code_identifier = (
                f"{filename}|{start_line}:{start_column}-{end_line}:{end_column}"
            )

        if filename != UNKNOWN_FILE and os.access(filename, os.R_OK):
            # If the file is readable, read the code snippet
            code = self._read_file(
                filename, start_line, end_line, start_column, end_column
            )
            return f"{code_identifier}: {code}"
        return code_identifier
