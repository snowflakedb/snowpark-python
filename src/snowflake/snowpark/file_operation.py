#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import gzip
import os
import sys
import tempfile
from typing import IO, Dict, List, NamedTuple, Optional

import snowflake.snowpark
from snowflake.connector import OperationalError, ProgrammingError
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.utils import (
    get_local_file_path,
    is_in_stored_procedure,
    is_single_quoted,
    normalize_local_file,
    normalize_remote_file_or_dir,
    result_set_to_rows,
    split_path,
)


def _validate_stage_location(stage_location: str) -> str:
    stage_location = stage_location.strip()
    if not stage_location:
        raise ValueError(
            "stage_location cannot be empty. It must be a full stage path with prefix and file name like @mystage/stage/prefix/filename"
        )
    if stage_location[-1] == "/":
        raise ValueError(
            "stage_location should end with target filename like @mystage/prefix/stage/filename"
        )
    return stage_location


class PutResult(NamedTuple):
    """Represents the results of uploading a local file to a stage location."""

    source: str  #: The source file path.
    target: str  #: The file path in the stage where the source file is uploaded.
    source_size: int  #: The size in bytes of the source file.
    target_size: int  #: The size in bytes of the target file.
    source_compression: str  #: The source file compression format.
    target_compression: str  #: The target file compression format.
    status: str  #: Status indicating whether the file was uploaded to the stage. Values can be 'UPLOADED' or 'SKIPPED'.
    message: str  #: The detailed message of the upload status.


class GetResult(NamedTuple):
    """Represents the results of downloading a file from a stage location to the local file system."""

    file: str  #: The downloaded file path.
    size: str  #: The size in bytes of the downloaded file.
    status: str  #: Indicates whether the download is successful.
    message: str  #: The detailed message about the download status.


class FileOperation:
    """Provides methods for working on files in a stage.
    To access an object of this class, use :attr:`Session.file`.
    """

    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session

    def put(
        self,
        local_file_name: str,
        stage_location: str,
        *,
        parallel: int = 4,
        auto_compress: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> List[PutResult]:
        """Uploads local files to the stage.

        References: `Snowflake PUT command <https://docs.snowflake.com/en/sql-reference/sql/put.html>`_.

        Example::

            >>> # Create a temp stage.
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> # Upload a file to a stage.
            >>> put_result = session.file.put("tests/resources/t*.csv", "@mystage/prefix1")
            >>> put_result[0].status
            'UPLOADED'

        Args:
            local_file_name: The path to the local files to upload. To match multiple files in the path,
                you can specify the wildcard characters ``*`` and ``?``.
            stage_location: The stage and prefix where you want to upload the files.
            parallel: Specifies the number of threads to use for uploading files. The upload process separates batches of data files by size:

                  - Small files (< 64 MB compressed or uncompressed) are staged in parallel as individual files.
                  - Larger files are automatically split into chunks, staged concurrently, and reassembled in the target stage. A single thread can upload multiple chunks.

                Increasing the number of threads can improve performance when uploading large files.
                Supported values: Any integer value from 1 (no parallelism) to 99 (use 99 threads for uploading files).
            auto_compress: Specifies whether Snowflake uses gzip to compress files during upload.
            source_compression: Specifies the method of compression used on already-compressed files that are being staged.
                Values can be 'AUTO_DETECT', 'GZIP', 'BZ2', 'BROTLI', 'ZSTD', 'DEFLATE', 'RAW_DEFLATE', 'NONE'.
            overwrite: Specifies whether Snowflake will overwrite an existing file with the same name during upload.
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
            A ``list`` of :class:`PutResult` instances, each of which represents the results of an uploaded file.
        """
        options = {
            "parallel": parallel,
            "source_compression": source_compression,
            "auto_compress": auto_compress,
            "overwrite": overwrite,
        }
        if is_in_stored_procedure():  # pragma: no cover
            try:
                cursor = self._session._conn._cursor
                cursor._upload(local_file_name, stage_location, options)
                result_meta = cursor.description
                result_data = cursor.fetchall()
                put_result = result_set_to_rows(result_data, result_meta)
            except ProgrammingError as pe:
                tb = sys.exc_info()[2]
                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                    pe
                )
                raise ne.with_traceback(tb) from None
        else:
            plan = self._session._analyzer.plan_builder.file_operation_plan(
                "put",
                normalize_local_file(local_file_name),
                normalize_remote_file_or_dir(stage_location),
                options,
            )
            put_result = snowflake.snowpark.dataframe.DataFrame(
                self._session, plan
            )._internal_collect_with_tag(statement_params=statement_params)
        return [PutResult(**file_result.asDict()) for file_result in put_result]

    def get(
        self,
        stage_location: str,
        target_directory: str,
        *,
        parallel: int = 10,
        pattern: Optional[str] = None,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> List[GetResult]:
        """Downloads the specified files from a path in a stage to a local directory.

        References: `Snowflake GET command <https://docs.snowflake.com/en/sql-reference/sql/get.html>`_.

        Examples:

            >>> # Create a temp stage.
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> # Upload a file to a stage.
            >>> _ = session.file.put("tests/resources/t*.csv", "@mystage/prefix1")
            >>> # Download one file from a stage.
            >>> get_result1 = session.file.get("@myStage/prefix1/test2CSV.csv", "tests/downloaded/target1")
            >>> assert len(get_result1) == 1
            >>> # Download all the files from @myStage/prefix.
            >>> get_result2 = session.file.get("@myStage/prefix1", "tests/downloaded/target2")
            >>> assert len(get_result2) > 1
            >>> # Download files with names that match a regular expression pattern.
            >>> get_result3 = session.file.get("@myStage/prefix1", "tests/downloaded/target3", pattern=".*test.*.csv.gz")
            >>> assert len(get_result3) > 1

        Args:
            stage_location: A directory or filename on a stage, from which you want to download the files.
            target_directory: The path to the local directory where the files should be downloaded.
                If ``target_directory`` does not already exist, the method creates the directory.
            parallel: Specifies the number of threads to use for downloading the files.
                The granularity unit for downloading is one file.
                Increasing the number of threads might improve performance when downloading large files.
                Supported values: Any integer value from 1 (no parallelism) to 99 (use 99 threads for downloading files).
            pattern: Specifies a regular expression pattern for filtering files to download.
                The command lists all files in the specified path and applies the regular expression pattern on each of the files found.
                Default: ``None`` (all files in the specified stage are downloaded).
            statement_params: Dictionary of statement level parameters to be set while executing this action.

        Returns:
            A ``list`` of :class:`GetResult` instances, each of which represents the result of a downloaded file.

        """
        options = {"parallel": parallel}
        if pattern is not None:
            if not is_single_quoted(pattern):
                pattern_escape_single_quote = pattern.replace("'", "\\'")
                pattern = f"'{pattern_escape_single_quote}'"  # snowflake pattern is a string with single quote
            options["pattern"] = pattern

        try:
            if is_in_stored_procedure():  # pragma: no cover
                try:
                    cursor = self._session._conn._cursor
                    cursor._download(stage_location, target_directory, options)
                    result_meta = cursor.description
                    result_data = cursor.fetchall()
                    get_result = result_set_to_rows(result_data, result_meta)
                except ProgrammingError as pe:
                    tb = sys.exc_info()[2]
                    ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                        pe
                    )
                    raise ne.with_traceback(tb) from None
            else:
                plan = self._session._plan_builder.file_operation_plan(
                    "get",
                    normalize_local_file(target_directory),
                    normalize_remote_file_or_dir(stage_location),
                    options,
                )
                # This is not needed for stored proc because sp connector already fixed it
                # JDBC auto-creates directory but python-connector doesn't. So create the folder here.
                os.makedirs(get_local_file_path(target_directory), exist_ok=True)
                get_result = snowflake.snowpark.dataframe.DataFrame(
                    self._session, plan
                )._internal_collect_with_tag(statement_params=statement_params)
            return [GetResult(**file_result.asDict()) for file_result in get_result]
        except IndexError:
            return []

    def put_stream(
        self,
        input_stream: IO[bytes],
        stage_location: str,
        *,
        parallel: int = 4,
        auto_compress: bool = True,
        source_compression: str = "AUTO_DETECT",
        overwrite: bool = False,
    ) -> PutResult:
        """Uploads local files to the stage via a file stream.

        Args:
            input_stream: The input stream from which the data will be uploaded.
            stage_location: The full stage path with prefix and file name where you want the file to be uploaded.
            parallel: Specifies the number of threads to use for uploading files. The upload process separates batches of data files by size:

                  - Small files (< 64 MB compressed or uncompressed) are staged in parallel as individual files.
                  - Larger files are automatically split into chunks, staged concurrently, and reassembled in the target stage. A single thread can upload multiple chunks.

                Increasing the number of threads can improve performance when uploading large files.
                Supported values: Any integer value from 1 (no parallelism) to 99 (use 99 threads for uploading files).
                Defaults to 4.
            auto_compress: Specifies whether Snowflake uses gzip to compress files during upload. Defaults to True.
            source_compression: Specifies the method of compression used on already-compressed files that are being staged.
                Values can be 'AUTO_DETECT', 'GZIP', 'BZ2', 'BROTLI', 'ZSTD', 'DEFLATE', 'RAW_DEFLATE', 'NONE'. Defaults to "AUTO_DETECT".
            overwrite: Specifies whether Snowflake will overwrite an existing file with the same name during upload. Defaults to False.

        Returns:
            An object of :class:`PutResult` which represents the results of an uploaded file.
        """
        stage_location = _validate_stage_location(stage_location)
        cursor = self._session._conn._cursor
        if is_in_stored_procedure():  # pragma: no cover
            try:
                options = {
                    "parallel": parallel,
                    "source_compression": source_compression,
                    "auto_compress": auto_compress,
                    "overwrite": overwrite,
                }
                cursor._upload_stream(input_stream, stage_location, options)
                result_data = cursor.fetchall()
            except ProgrammingError as pe:
                tb = sys.exc_info()[2]
                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                    pe
                )
                raise ne.with_traceback(tb) from None
        else:
            stage_with_prefix, dest_filename = split_path(stage_location)
            put_result = self._session._conn.upload_stream(
                input_stream=input_stream,
                stage_location=stage_with_prefix,
                dest_filename=dest_filename,
                parallel=parallel,
                compress_data=auto_compress,
                source_compression=source_compression,
                overwrite=overwrite,
            )
            result_data = put_result["data"]

        result_meta = cursor.description
        put_result = result_set_to_rows(result_data, result_meta)[0]
        return PutResult(**put_result.asDict())

    def get_stream(
        self,
        stage_location: str,
        *,
        parallel: int = 10,
        decompress: bool = False,
        statement_params: Optional[Dict[str, str]] = None,
    ) -> IO[bytes]:
        """Downloads the specified files from a path in a stage and expose it through a stream.

        Args:
            stage_location: The full stage path with prefix and file name, from which you want to download the file.
            parallel: Specifies the number of threads to use for downloading the files.
                The granularity unit for downloading is one file.
                Increasing the number of threads might improve performance when downloading large files.
                Supported values: Any integer value from 1 (no parallelism) to 99 (use 99 threads for downloading files). Defaults to 10.
            statement_params: Dictionary of statement level parameters to be set while executing this action. Defaults to None.
            decompress: Specifies whether to use gzip to decompress file after download. Defaults to False.

        Examples:

            >>> # Create a temp stage.
            >>> _ = session.sql("create or replace temp stage mystage").collect()
            >>> # Upload a file to a stage.
            >>> _ = session.file.put("tests/resources/testCSV.csv", "@mystage/prefix1")
            >>> # Download one file from a stage.
            >>> fd = session.file.get_stream("@myStage/prefix1/testCSV.csv.gz", decompress=True)
            >>> assert fd.read(5) == b"1,one"
            >>> fd.close()

        Returns:
            An ``BytesIO`` object which points to the downloaded file.
        """
        # check stage location has a file name
        stage_location = _validate_stage_location(stage_location)
        if is_in_stored_procedure():  # pragma: no cover
            try:
                return self._session._conn._cursor._download_stream(
                    stage_location, decompress
                )
            except ProgrammingError as pe:
                tb = sys.exc_info()[2]
                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_PROGRAMMING_ERROR(
                    pe
                )
                raise ne.with_traceback(tb) from None
        else:
            options = {"parallel": parallel}
            tmp_dir = tempfile.gettempdir()
            src_file_name = split_path(stage_location)[1]
            local_file_name = os.path.join(tmp_dir, src_file_name)
            plan = self._session._plan_builder.file_operation_plan(
                "get",
                normalize_local_file(tmp_dir),
                normalize_remote_file_or_dir(stage_location),
                options=options,
            )
            try:
                snowflake.snowpark.dataframe.DataFrame(
                    self._session, plan
                )._internal_collect_with_tag(statement_params=statement_params)
            except OperationalError as oe:
                tb = sys.exc_info()[2]
                ne = SnowparkClientExceptionMessages.SQL_EXCEPTION_FROM_OPERATIONAL_ERROR(
                    oe
                )
                raise ne.with_traceback(tb) from None

            return (
                gzip.open(local_file_name, "rb")
                if decompress
                else open(local_file_name, "rb")
            )
