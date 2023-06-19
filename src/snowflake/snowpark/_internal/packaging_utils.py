#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import io
import os
import subprocess
import sys
import zipfile
from logging import getLogger
from types import ModuleType
from typing import Callable, Dict, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.udf_utils import (
    _DEFAULT_HANDLER_NAME,
    _MAX_INLINE_CLOSURE_SIZE_BYTES,
    generate_python_code,
    get_error_message_abbr,
)
from snowflake.snowpark._internal.utils import (
    STAGE_PREFIX,
    TempObjectType,
    get_udf_upload_prefix,
    is_single_quoted,
    normalize_remote_file_or_dir,
    random_number,
    unwrap_stage_location_single_quote,
)

_logger = getLogger(__name__)
PIP_ENVIRONMENT_VARIABLE = "PIP_NAME"
IMPLICIT_ZIP_FILE_NAME = "unsupported_packages"


def resolve_imports_and_packages(
    session: "snowflake.snowpark.Session",
    object_type: TempObjectType,
    func: Union[Callable, Tuple[str, str]],
    arg_names: List[str],
    udf_name: str,
    stage_location: Optional[str],
    imports: Optional[List[Union[str, Tuple[str, str]]]],
    packages: Optional[List[Union[str, ModuleType]]],
    parallel: int = 4,
    is_pandas_udf: bool = False,
    is_dataframe_input: bool = False,
    force_push: bool = True,
    max_batch_size: Optional[int] = None,
    *,
    statement_params: Optional[Dict[str, str]] = None,
    source_code_display: bool = False,
    skip_upload_on_content_match: bool = False,
) -> Tuple[str, str, str, str, str]:
    upload_stage = (
        unwrap_stage_location_single_quote(stage_location)
        if stage_location
        else session.get_session_stage()
    )

    # resolve imports
    if imports:
        udf_level_imports = {}
        for udf_import in imports:
            if isinstance(udf_import, str):
                resolved_import_tuple = session._resolve_import_path(udf_import)
            elif isinstance(udf_import, tuple) and len(udf_import) == 2:
                resolved_import_tuple = session._resolve_import_path(
                    udf_import[0], udf_import[1]
                )
            else:
                raise TypeError(
                    f"{get_error_message_abbr(object_type).replace(' ', '-')}-level import can only be a file path (str) "
                    "or a tuple of the file path (str) and the import path (str)."
                )
            udf_level_imports[resolved_import_tuple[0]] = resolved_import_tuple[1:]
        all_urls = session._resolve_imports(
            upload_stage, udf_level_imports, statement_params=statement_params
        )
    elif imports is None:
        all_urls = session._resolve_imports(
            upload_stage, statement_params=statement_params
        )
    else:
        all_urls = []

    # resolve packages
    resolved_packages = (
        session._resolve_packages(
            packages, include_pandas=is_pandas_udf, force_push=force_push
        )
        if packages is not None
        else session._resolve_packages(
            [],
            session._packages,
            validate_package=False,
            include_pandas=is_pandas_udf,
            force_push=force_push,
        )
    )

    dest_prefix = get_udf_upload_prefix(udf_name)

    # Upload closure to stage if it is beyond inline closure size limit
    if isinstance(func, Callable):
        # generate a random name for udf py file
        # and we compress it first then upload it
        udf_file_name_base = f"udf_py_{random_number()}"
        udf_file_name = f"{udf_file_name_base}.zip"
        code = generate_python_code(
            func,
            arg_names,
            object_type,
            is_pandas_udf,
            is_dataframe_input,
            max_batch_size,
            source_code_display=source_code_display,
        )
        if len(code) > _MAX_INLINE_CLOSURE_SIZE_BYTES:
            dest_prefix = get_udf_upload_prefix(udf_name)
            upload_file_stage_location = normalize_remote_file_or_dir(
                f"{upload_stage}/{dest_prefix}/{udf_file_name}"
            )
            udf_file_name_base = os.path.splitext(udf_file_name)[0]
            with io.BytesIO() as input_stream:
                with zipfile.ZipFile(
                    input_stream, mode="w", compression=zipfile.ZIP_DEFLATED
                ) as zf:
                    zf.writestr(f"{udf_file_name_base}.py", code)
                session._conn.upload_stream(
                    input_stream=input_stream,
                    stage_location=upload_stage,
                    dest_filename=udf_file_name,
                    dest_prefix=dest_prefix,
                    parallel=parallel,
                    source_compression="DEFLATE",
                    compress_data=False,
                    overwrite=True,
                    is_in_udf=True,
                    skip_upload_on_content_match=skip_upload_on_content_match,
                )
            all_urls.append(upload_file_stage_location)
            inline_code = None
            handler = f"{udf_file_name_base}.{_DEFAULT_HANDLER_NAME}"
        else:
            inline_code = code
            upload_file_stage_location = None
            handler = _DEFAULT_HANDLER_NAME
    else:
        udf_file_name = os.path.basename(func[0])
        # for a compressed file, it might have multiple extensions
        # and we should remove all extensions
        udf_file_name_base = udf_file_name.split(".")[0]
        inline_code = None
        handler = f"{udf_file_name_base}.{func[1]}"

        if func[0].startswith(STAGE_PREFIX):
            upload_file_stage_location = None
            all_urls.append(func[0])
        else:
            upload_file_stage_location = normalize_remote_file_or_dir(
                f"{upload_stage}/{dest_prefix}/{udf_file_name}"
            )
            session._conn.upload_file(
                path=func[0],
                stage_location=upload_stage,
                dest_prefix=dest_prefix,
                parallel=parallel,
                compress_data=False,
                overwrite=True,
                skip_upload_on_content_match=skip_upload_on_content_match,
            )
            all_urls.append(upload_file_stage_location)

    # build imports and packages string
    all_imports = ",".join(
        [url if is_single_quoted(url) else f"'{url}'" for url in all_urls]
    )
    all_packages = ",".join([f"'{package}'" for package in resolved_packages])
    return handler, inline_code, all_imports, all_packages, upload_file_stage_location


def get_package_name_from_metadata(metadata_file_path: str) -> Optional[str]:
    """Loads a METADATA file from the dist-info directory of an installed
    Python package, finds the name of the package.
    This is found on a line containing "Name: my_package".

    Args:
        metadata_file_path (str): The path to the METADATA file

    Returns:
        str: the name of the package.
    """
    import re

    with open(metadata_file_path, encoding="utf-8") as metadata_file:
        contents = metadata_file.read()
        results = re.search("^Name: (.*)$", contents, flags=re.MULTILINE)
        if results is None:
            return None
        requirement_line = results.group(1)
        results = re.search("^Version: (.*)$", contents, flags=re.MULTILINE)
        if results is not None:
            version = results.group(1)
            requirement_line += f"=={version}"
        return requirement_line


def get_downloaded_packages(directory: str) -> Dict[str, List[str]]:
    import glob
    import os

    metadata_files = glob.glob(f"{directory}/*dist-info/METADATA")
    return_dict: Dict[str] = {}
    for metadata_file in metadata_files:
        parent_folder = os.path.dirname(metadata_file)
        package = get_package_name_from_metadata(metadata_file)

        if package is not None:
            # Determine which folders or files belong to this package
            record_file_path = os.path.join(parent_folder, "RECORD")
            if os.path.exists(record_file_path):
                # Get unique root folder names
                with open(record_file_path, encoding="utf-8") as record_file:
                    # We want the part up until the first '/'.
                    # Sometimes it's a file with a trailing ",sha256=abcd....",
                    # so we trim that off too
                    record_entries = list(
                        {
                            line.split("/")[0].split(",")[0]
                            for line in record_file.readlines()
                        },
                    )
                    included_record_entries = []
                    for record_entry in record_entries:
                        record_entry_full_path = os.path.abspath(
                            os.path.join(directory, record_entry),
                        )
                        # it's possible for the RECORD file to contain relative
                        # paths to items outside target folder. (ignore these for now)
                        if (
                            os.path.exists(record_entry_full_path)
                            and directory in record_entry_full_path
                        ):
                            included_record_entries.append(record_entry)
                    return_dict[package] = included_record_entries
    return return_dict


def parse_anaconda_packages(packages: List, valid_packages: Dict[str, Tuple]):
    snowflake_packages: List = []
    # TODO: Check for version, store versioning and send it in with packages list
    for package in packages:
        if package.name.lower() in valid_packages:
            snowflake_packages.append(package)
    return snowflake_packages


def install_pip_packages_to_target_folder(packages: List[str], target: str):
    try:
        pip_executable = os.getenv(PIP_ENVIRONMENT_VARIABLE)
        pip_command = (
            [sys.executable, "-m", "pip"] if not pip_executable else [pip_executable]
        )
        process = subprocess.Popen(
            pip_command + ["install", "-t", target, *packages],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        process.wait()
        pip_install_result = process.returncode
        # process_logs = [line.strip() for line in process.stdout]
    except FileNotFoundError:
        raise ModuleNotFoundError(
            f"Pip not found. Please install pip in your environment or specify the "
            f"{PIP_ENVIRONMENT_VARIABLE} environment variable and try again!"
        )

    if pip_install_result is not None and pip_install_result != 0:
        # TODO: Are errors returned by pip stderr descriptive?
        raise Exception(
            f"Pip failed with return code {pip_install_result}.\n\n{process.stderr}"
        )


def detect_native_dependencies(target: str) -> None:
    import glob

    glob_output = glob.glob(f"{target}/**/*.so")
    if glob_output:
        for path in glob_output:
            _logger.warning(
                f"Potential native library: {os.path.relpath(path, target)}"
            )

            # TODO: Be more specific about which packages are causing errors
            raise ValueError(
                "Your code depends on native dependencies! Use `force_push` option if you wish to "
                "proceed with using them."
            )


def zip_directory_contents(directory_path: str, output_path: str) -> None:
    # TODO: Handle potential errors
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, directory_path)
                zipf.write(file_path, relative_path)
