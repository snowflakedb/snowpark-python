#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import hashlib
import io
import os
import platform
import random
import re
import string
import zipfile
from typing import IO, List, Optional, Tuple

from snowflake.connector.version import VERSION as connector_version
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.version import VERSION as snowpark_version


class Utils:
    _TEMP_OBJECT_PREFIX = "SNOWPARK_TEMP"

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
            raise SnowparkClientException(f"The object name '{name}' is invalid.")

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
        if re.match("[\\w]+$", udf_name):
            return udf_name
        else:
            return "{}_{}".format(re.sub("\\W", "", udf_name), abs(hash(udf_name)))

    @staticmethod
    def random_number() -> int:
        """Get a random unsigned integer."""
        return random.randint(0, 2 ** 31)

    @staticmethod
    def generated_py_file_ext() -> Tuple[str, ...]:
        # ignore byte-compiled (.pyc), optimized (.pyo), DLL (.pyd)
        # and interface (.pyi) python files
        return ".pyc", ".pyo", ".pyd", ".pyi"

    @staticmethod
    def zip_file_or_directory_to_stream(
        path: str,
        leading_path: Optional[str] = None,
        add_init_py: bool = False,
        ignore_generated_py_file: bool = True,
    ) -> IO[bytes]:
        """Compresses the file or directory as a zip file to a binary stream.
        Args:
            path: The absolute path to a file or directory.
            leading_path: This argument is used to determine where directory should
                start in the zip file. Basically, this argument works as the role
                of `start` argument in os.path.relpath(path, start), i.e.,
                absolute path = [leading path]/[relative path]. For example,
                when the path is "/tmp/dir1/dir2/test.py", and the leading path
                is "/tmp/dir1", the generated filesystem structure in the zip file
                will be "dir2/test.py".
            add_init_py: Whether to add __init__.py along the compressed path.
            ignore_generated_py_file: Whether to ignore some generated python files
                in the directory.

        Returns:
            A byte stream.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} is not found")
        if leading_path and not path.startswith(leading_path):
            raise ValueError(f"{leading_path} doesn't lead to {path}")
        # if leading_path is not provided, just use the parent path,
        # and the compression will start from the parent directory
        start_path = leading_path if leading_path else os.path.join(path, "..")

        input_stream = io.BytesIO()
        with zipfile.ZipFile(
            input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            if os.path.isdir(path):
                for dirname, _, files in os.walk(path):
                    # ignore __pycache__
                    if ignore_generated_py_file and "__pycache__" in dirname:
                        continue
                    zf.write(dirname, os.path.relpath(dirname, start_path))
                    for file in files:
                        # ignore generated python files
                        if ignore_generated_py_file and file.endswith(
                            Utils.generated_py_file_ext()
                        ):
                            continue
                        filename = os.path.join(dirname, file)
                        zf.write(filename, os.path.relpath(filename, start_path))
            else:
                zf.write(path, os.path.relpath(path, start_path))

            # __init__.py is needed for all directories along the import path
            # when importing a module as a zip file
            if add_init_py:
                relative_path = os.path.relpath(path, start_path)
                head, _ = os.path.split(relative_path)
                while head and head != os.sep:
                    zf.writestr(os.path.join(head, "__init__.py"), "")
                    head, _ = os.path.split(head)

        return input_stream

    @staticmethod
    def parse_positional_args_to_list(*inputs) -> List:
        """Convert the positional arguments to a list."""
        if len(inputs) == 1:
            return [*inputs[0]] if isinstance(inputs[0], (list, tuple)) else [inputs[0]]
        else:
            return [*inputs]

    @staticmethod
    def calculate_md5(
        path: str,
        chunk_size: int = 8192,
        ignore_generated_py_file: bool = True,
        additional_info: Optional[str] = None,
    ) -> str:
        """Calculates the checksum (md5) of a file or a directory.

        Args:
            path: the path to a local file or directory.
                If it points to a file, we read a small chunk from the file and
                calculate the checksum based on it.
                If it points to a directory, the names of all files and subdirectories
                in this directory will also be included for checksum computation.
            chunk_size: The size in byte we will read from the file/directory for
                checksum computation.
            ignore_generated_py_file: Whether to ignore some generated python files
                in the directory.
            additional_info: Any additional information we might want to include
                for checksum computation.

        Returns:
            The result checksum (md5).
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} is not found")

        hash_md5 = hashlib.md5()
        if os.path.isfile(path):
            with open(path, "rb") as f:
                hash_md5.update(f.read(chunk_size))
        elif os.path.isdir(path):
            current_size = 0
            for dirname, dirs, files in os.walk(path):
                # ignore __pycache__
                if ignore_generated_py_file and "__pycache__" in dirname:
                    continue
                # sort dirs and files so the result is consistent across different os
                for dir in sorted(dirs):
                    if ignore_generated_py_file and dir == "__pycache__":
                        continue
                    hash_md5.update(dir.encode("utf8"))
                for file in sorted(files):
                    # ignore generated python files
                    if ignore_generated_py_file and file.endswith(
                        Utils.generated_py_file_ext()
                    ):
                        continue
                    hash_md5.update(file.encode("utf8"))
                    if current_size < chunk_size:
                        filename = os.path.join(dirname, file)
                        file_size = os.path.getsize(filename)
                        read_size = min(file_size, chunk_size - current_size)
                        current_size += read_size
                        with open(filename, "rb") as f:
                            hash_md5.update(f.read(read_size))
        else:
            raise ValueError("md5 can only be calculated for a file or directory")

        if additional_info:
            hash_md5.update(additional_info.encode("utf8"))

        return hash_md5.hexdigest()

    class TempObjectType:
        pass

    class Table(TempObjectType):
        pass

    class Stage(TempObjectType):
        pass

    class View(TempObjectType):
        pass

    class Function(TempObjectType):
        pass

    class FileFormat(TempObjectType):
        def __repr__(self):
            return "File Format"

    @classmethod
    def random_name_for_temp_object(cls, temp_object_type: TempObjectType) -> str:
        # Replace white space in the type name with `_`
        type_str = temp_object_type.__class__.__name__.replace(" ", "_")
        # Making random digits to 15 to match the length of session id.
        alphanumeric = string.ascii_lowercase + string.digits
        rand_str = ("".join(random.choice(alphanumeric) for _ in range(10))).upper()
        return f"{cls._TEMP_OBJECT_PREFIX}_{type_str}_{rand_str}"
