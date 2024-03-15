#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import glob
import os
import shutil
import tempfile
import uuid
from typing import IO, TYPE_CHECKING, Dict

from snowflake.snowpark._internal.utils import unwrap_stage_location_single_quote
from snowflake.snowpark.exceptions import (
    SnowparkSQLException,
    SnowparkUploadFileException,
)
from snowflake.snowpark.mock._snowflake_data_type import ColumnType, TableEmulator
from snowflake.snowpark.types import DecimalType, StringType

if TYPE_CHECKING:
    from snowflake.snowpark.mock._connection import MockServerConnection

PUT_RESULT_KEYS = [
    "source",
    "target",
    "source_size",
    "target_size",
    "source_compression",
    "target_compression",
    "status",
    "message",
]


def extract_stage_name_and_prefix(stage_location):
    """
    extract the stage name and dir path in the stage_location
    inspired by utils.get_stage_file_prefix_length

    currently we don't support fully qualified namespace stage
    TODO: https://snowflakecomputing.atlassian.net/browse/SNOW-1235144 for fully qualified namespace support
    """
    normalized = unwrap_stage_location_single_quote(stage_location)
    if not normalized.endswith("/"):
        normalized = f"{normalized}/"

    normalized = normalized[1:]  # remove the beginning '@'

    if normalized.startswith("~/"):
        return "~", normalized[3:]  # skip '/'

    is_quoted = False
    stage_name_start_idx, stage_name_end_idx = 0, None
    prefix_start_idx = None

    if normalized[0] == '"':
        stage_name_start_idx = 1
        for i, c in enumerate(normalized):
            if c == '"':
                is_quoted = (
                    not is_quoted
                )  # this handles escaping consecutive double quotes
            elif c == "/" and not is_quoted:
                # all chars prior to ith char is part of the stage name
                stage_name_end_idx = i - 1
                prefix_start_idx = i + 1
                break

        if not stage_name_end_idx:
            raise SnowparkSQLException(f"Invalid stage_location {stage_location}.")
    else:
        stage_name_end_idx = normalized.find("/")
        prefix_start_idx = stage_name_end_idx + 1
    stage_name = normalized[stage_name_start_idx:stage_name_end_idx]
    dir_path = normalized[prefix_start_idx:-1]  # remove the first and last '/'
    return stage_name, dir_path


class StageEntity:
    def __init__(self, root_dir_path: str, stage_name: str) -> None:
        self._stage_name = stage_name
        # stage name might contain special chars which can not be used as dir name
        # so we generate uuid as name
        self._dir_name = str(uuid.uuid4())
        self._working_directory = os.path.join(root_dir_path, self._dir_name)

        if os.path.exists(self._working_directory):
            shutil.rmtree(self._working_directory)

        os.mkdir(self._working_directory)
        self._files = set()

    def put_file(
        self, local_file_name: str, stage_prefix: str, overwrite: bool = False
    ) -> TableEmulator:
        local_file_name = local_file_name[
            len("`file://") : -1
        ]  # skip normalized prefix `file:// and suffix `
        # glob supports wildcard '?' and '*' searching
        list_of_files = glob.glob(local_file_name)
        result_df = TableEmulator(
            columns=PUT_RESULT_KEYS,
            sf_types={
                "source": ColumnType(StringType(), True),
                "target": ColumnType(StringType(), True),
                "source_size": ColumnType(DecimalType(10, 0), True),
                "target_size": ColumnType(DecimalType(10, 0), True),
                "source_compression": ColumnType(StringType(), True),
                "target_compression": ColumnType(StringType(), True),
                "status": ColumnType(StringType(), True),
                "message": ColumnType(StringType(), True),
            },
            dtype=object,
        )

        if not list_of_files:
            raise SnowparkSQLException(f"File doesn't exist: {local_file_name}")

        for local_file_name in list_of_files:

            file_name = os.path.basename(local_file_name)
            stage_target_dir_path = os.path.join(self._working_directory, stage_prefix)

            if not os.path.exists(stage_target_dir_path):
                os.makedirs(stage_target_dir_path)

            if (
                os.path.isfile(os.path.join(stage_target_dir_path, file_name))
                and not overwrite
            ):
                status = "SKIPPED"
            else:
                shutil.copy(local_file_name, stage_target_dir_path)
                status = "UPLOADED"

            file_size = os.path.getsize(local_file_name)

            values = [
                file_name,
                file_name,
                file_size,
                file_size,
                "NONE",
                "NONE",
                status,
                "",
            ]
            result_df.loc[len(result_df)] = dict(zip(PUT_RESULT_KEYS, values))
        return result_df

    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_prefix: str,
        file_name: str,
        overwrite: bool = False,
    ) -> Dict:
        stage_target_dir_path = os.path.join(self._working_directory, stage_prefix)

        if not os.path.exists(stage_target_dir_path):
            os.makedirs(stage_target_dir_path)

        status = "UPLOADED"
        if (
            os.path.isfile(os.path.join(stage_target_dir_path, file_name))
            and not overwrite
        ):
            status = "SKIPPED"
        else:
            with open(os.path.join(stage_target_dir_path, file_name), "wb") as f:
                try:
                    f.write(input_stream.read())
                except ValueError as exc:
                    raise SnowparkUploadFileException(
                        message="[Local Testing] Reading closed file while uploading stream",
                        error_code="1408",
                    ) from exc

        file_size = os.path.getsize(os.path.join(stage_target_dir_path, file_name))
        return {
            "data": [
                (file_name, file_name, file_size, file_size, "NONE", "NONE", status, "")
            ],
            "sfqid": None,
        }


class StageEntityRegistry:
    # Registry to store tables and views.
    def __init__(self, conn: "MockServerConnection") -> None:
        self._root_dir = tempfile.TemporaryDirectory()
        self._stage_registry = {}

    def create_or_replace_stage(self, stage_name):
        self._stage_registry[stage_name] = StageEntity(self._root_dir.name, stage_name)

    def __getitem__(self, stage_name: str):
        # the assumption here is that stage always exists
        if stage_name not in self._stage_registry:
            self.create_or_replace_stage(stage_name)
        return self._stage_registry[stage_name]

    def put(
        self, local_file_name: str, stage_location: str, overwrite: bool = False
    ) -> TableEmulator:
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        # the assumption here is that stage always exists
        if stage_name not in self._stage_registry:
            self.create_or_replace_stage(stage_name)
        return self._stage_registry[stage_name].put_file(
            local_file_name=local_file_name,
            stage_prefix=stage_prefix,
            overwrite=overwrite,
        )

    def upload_stream(
        self,
        input_stream: IO[bytes],
        stage_location: str,
        file_name: str,
        overwrite: bool = False,
    ) -> Dict:
        stage_name, stage_prefix = extract_stage_name_and_prefix(stage_location)
        # the assumption here is that stage always exists
        if stage_name not in self._stage_registry:
            self.create_or_replace_stage(stage_name)
        return self._stage_registry[stage_name].upload_stream(
            input_stream=input_stream,
            stage_prefix=stage_prefix,
            file_name=file_name,
            overwrite=overwrite,
        )
