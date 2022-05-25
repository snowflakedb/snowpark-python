#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import os
import sys
from typing import List, NamedTuple, Optional

import snowflake.snowpark
from snowflake.connector import ProgrammingError
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.utils import (
    get_local_file_path,
    is_in_stored_procedure,
    is_single_quoted,
    normalize_local_file,
    normalize_remote_file_or_dir,
    result_set_to_rows,
)


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

        Returns:
            A ``list`` of :class:`PutResult` instances, each of which represents the results of an uploaded file.
        """
        options = {
            "parallel": parallel,
            "source_compression": source_compression,
            "auto_compress": auto_compress,
            "overwrite": overwrite,
        }
        if is_in_stored_procedure():
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
            plan = self._session._plan_builder.file_operation_plan(
                "put",
                normalize_local_file(local_file_name),
                normalize_remote_file_or_dir(stage_location),
                options,
            )
            put_result = snowflake.snowpark.dataframe.DataFrame(
                self._session, plan
            )._internal_collect_with_tag()
        return [PutResult(**file_result.asDict()) for file_result in put_result]

    def get(
        self,
        stage_location: str,
        target_directory: str,
        *,
        parallel: int = 10,
        pattern: Optional[str] = None,
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
            if is_in_stored_procedure():
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
                )._internal_collect_with_tag()
            return [GetResult(**file_result.asDict()) for file_result in get_result]
        # connector raises IndexError when no file is downloaded from python connector.
        except IndexError:
            return []
