#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import io
import os
import platform
import random
import re
import zipfile
from typing import IO, List

from snowflake.connector.version import VERSION as connector_version
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.version import VERSION as snowpark_version


class Utils:
    @staticmethod
    def validate_object_name(name: str):
        # Valid name can be:
        #   identifier,
        #   identifier.identifier,
        #   identifier.identifier.identifier
        #   identifier..identifier
        unquoted_id_pattern = r"([a-zA-Z_][\w$]*)"
        quoted_id_pattern = '("([^"]|"")+")'
        id_pattern = f"({unquoted_id_pattern}|{quoted_id_pattern})"
        pattern = re.compile(
            f"^(({id_pattern}\\.){{0,2}}|({id_pattern}\\.\\.)){id_pattern}$$"
        )
        if not pattern.match(name):
            raise SnowparkClientException(f"The object name {name} is invalid.")

    @staticmethod
    def get_version() -> str:
        return ".".join([str(d) for d in snowpark_version if d is not None])

    @staticmethod
    def get_python_version() -> str:
        return platform.python_version()

    @staticmethod
    def get_connector_version() -> str:
        return ".".join([str(d) for d in connector_version if d is not None])

    @staticmethod
    def get_os_name() -> str:
        return platform.system()

    @staticmethod
    def normalize_stage_location(name: str) -> str:
        """Get the normalized name of a stage."""
        trim_name = name.strip()
        return trim_name if trim_name.startswith("@") else f"@{trim_name}"

    @staticmethod
    def get_udf_upload_prefix(udf_name: str) -> str:
        """Get the valid stage prefix when uploading a UDF."""
        if re.match("[\\w]+", udf_name):
            return udf_name
        else:
            return "{}_{}".format(re.sub("\\W", "", udf_name), abs(hash(udf_name)))

    @staticmethod
    def random_number() -> int:
        """Get a random unsigned integer."""
        return random.randint(0, 2 ** 31)

    @staticmethod
    def zip_file_or_directory_to_stream(path: str) -> IO[bytes]:
        """Compress the file or directory as a zip file to a binary stream."""
        input_stream = io.BytesIO()
        parent_path = os.path.join(path, "..")
        with zipfile.ZipFile(
            input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            if os.path.isdir(path):
                for dirname, _, files in os.walk(path):
                    zf.write(dirname, os.path.relpath(dirname, parent_path))
                    for file in files:
                        filename = os.path.join(dirname, file)
                        zf.write(filename, os.path.relpath(filename, parent_path))
            else:
                zf.write(path, os.path.relpath(path, parent_path))

        return input_stream

    @staticmethod
    def parse_positional_args_to_list(*inputs) -> List:
        """Convert the positional arguments to a list."""
        if len(inputs) == 1:
            return [*inputs[0]] if isinstance(inputs[0], (list, tuple)) else [inputs[0]]
        else:
            return [*inputs]
