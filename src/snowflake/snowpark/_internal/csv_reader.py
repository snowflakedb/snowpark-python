#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import csv
import gzip
import io
import os
from typing import Dict, Iterator, List, Optional

from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark._internal.type_utils import type_string_to_type_object
from snowflake.snowpark.files import SnowflakeFile
from snowflake.snowpark.types import StructType

DEFAULT_CHUNK_SIZE: int = 1024


def get_file_size(filename: str) -> int:
    """Get the file size in bytes without reading the content."""
    with SnowflakeFile.open(filename, "rb", require_scoped_url=False) as file_obj:
        file_obj.seek(0, os.SEEK_END)
        return file_obj.tell()


def schema_string_to_struct_type(schema_string: str) -> Optional[StructType]:
    if schema_string == "":
        return None
    schema = type_string_to_type_object(schema_string)
    if not isinstance(schema, StructType):
        raise ValueError("CSV reader custom schema must be a StructType")
    return schema


def _align_start_to_next_line_break(
    file_obj: SnowflakeFile, approx_start: int, file_size: int
) -> int:
    """
    Align start position to the next line break to avoid returning partial rows.
    If approx_start is 0, keep it unchanged.
    """
    if approx_start <= 0:
        return 0
    if approx_start >= file_size:
        return file_size

    file_obj.seek(approx_start)
    while file_obj.tell() < file_size:
        byte = file_obj.read(1)
        if not byte:
            return file_size
        if byte == b"\n":
            return file_obj.tell()
    return file_size


def _align_end_to_line_break(
    file_obj: SnowflakeFile, approx_end: int, file_size: int
) -> int:
    """
    Align end position to the next line break so a worker can read full records.
    Last worker should pass file_size directly.
    """
    if approx_end >= file_size:
        return file_size
    if approx_end <= 0:
        return 0

    file_obj.seek(approx_end)
    while file_obj.tell() < file_size:
        byte = file_obj.read(1)
        if not byte:
            return file_size
        if byte == b"\n":
            return file_obj.tell()
    return file_size


def _row_to_result(
    row_values: List[str], schema: Optional[StructType]
) -> Dict[str, Optional[str]]:
    if schema is None:
        return {f"C{idx + 1}": value for idx, value in enumerate(row_values)}

    result: Dict[str, Optional[str]] = {}
    for idx, field in enumerate(schema.fields):
        name = unquote_if_quoted(field.name)
        result[name] = row_values[idx] if idx < len(row_values) else None
    return result


def process_csv_range(
    file_path: str,
    approx_start: int,
    approx_end: int,
    charset: str = "utf-8",
    schema: Optional[StructType] = None,
) -> Iterator[Dict[str, Optional[str]]]:
    """
    Process a byte range of a CSV file.

    POC constraints:
    - Minimal standard CSV only: delimiter ',', quote '\"'.
    - Newline is the record delimiter.
    - Quoted values are parsed within a single line (no multiline quoted records).
    - Parsing errors are skipped.
    """
    file_size = get_file_size(file_path)
    if approx_start >= file_size:
        return

    with SnowflakeFile.open(file_path, "rb", require_scoped_url=False) as file_obj:
        range_start = _align_start_to_next_line_break(file_obj, approx_start, file_size)
        range_end = _align_end_to_line_break(file_obj, approx_end, file_size)

        if range_start >= range_end:
            return

        file_obj.seek(range_start)
        raw_bytes = file_obj.read(range_end - range_start)

    content = raw_bytes.decode(charset, errors="replace")
    for line in content.splitlines():
        if line == "":
            continue
        try:
            parsed_row = next(
                csv.reader(
                    [line],
                    delimiter=",",
                    quotechar='"',
                    strict=True,
                )
            )
        except (csv.Error, StopIteration):
            continue

        if not parsed_row:
            continue
        yield _row_to_result(parsed_row, schema)


class CSVReader:
    def _process_gzip_file(
        self,
        filename: str,
        num_workers: int,
        worker_id: int,
        schema: Optional[StructType],
        charset: str = "utf-8",
    ) -> Iterator[Dict[str, Optional[str]]]:
        """
        Process a gzip-compressed CSV in a deterministic, non-overlapping way.

        Gzip data is not safely splittable by compressed byte offsets, so each worker
        streams rows and only emits rows assigned to it by row index modulo.
        """
        with SnowflakeFile.open(filename, "rb", require_scoped_url=False) as raw_file:
            with gzip.GzipFile(fileobj=raw_file, mode="rb") as gz_file:
                with gz_file:
                    text_stream = io.TextIOWrapper(gz_file, encoding=charset, newline="")
                    reader = csv.reader(
                        text_stream,
                        delimiter=",",
                        quotechar='"',
                    )
                    for row_idx, row_values in enumerate(reader):
                        if row_idx % num_workers != worker_id:
                            continue
                        if not row_values:
                            continue
                        yield _row_to_result(row_values, schema)

    def process(
        self,
        filename: str,
        num_workers: int,
        i: int,
        custom_schema: str,
    ):
        """
        Split the file into worker ranges and parse one range for this worker.

        The handler yields one tuple per CSV record, where the first field is a dict
        suitable for a VARIANT output column.
        """
        if num_workers <= 0:
            raise ValueError("num_workers must be a positive integer")

        file_size = get_file_size(filename)
        approx_chunk_size = file_size // num_workers
        approx_start = approx_chunk_size * i
        approx_end = approx_chunk_size * (i + 1) if i < num_workers - 1 else file_size
        schema = schema_string_to_struct_type(custom_schema)

        if filename.lower().endswith(".gz"):
            for row in self._process_gzip_file(
                filename=filename,
                num_workers=num_workers,
                worker_id=i,
                schema=schema,
            ):
                yield (row,)
            return

        for row in process_csv_range(
            filename,
            approx_start=approx_start,
            approx_end=approx_end,
            schema=schema,
        ):
            yield (row,)
