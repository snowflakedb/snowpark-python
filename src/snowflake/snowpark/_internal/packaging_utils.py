#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# The code in this file is largely a copy of https://github.com/Snowflake-Labs/snowcli/blob/main/src/snowcli/utils.py

import glob
import os
import platform
import subprocess
import sys
import zipfile
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pkg_resources
from pkg_resources import Requirement

_logger = getLogger(__name__)
PIP_ENVIRONMENT_VARIABLE = "PIP_NAME"
IMPLICIT_ZIP_FILE_NAME = "zipped_packages"
SNOWPARK_PACKAGE_NAME = "snowflake-snowpark-python"

# Default that we do not want to upload because we wish to use stable versions instead
# TODO: This is very hacky, how can errors with default packages be avoided?
DEFAULT_PACKAGES = {
    "numpy",
    "pandas",
    "pyarrow",
    "python-dateutil",
    "pytz",
    "cloudpickle",
    "streamlit",  # not default, but is available on import
}


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
        return requirement_line.strip().lower()


def get_downloaded_packages(directory: str) -> Dict[Requirement, List[str]]:
    import glob
    import os

    metadata_files = glob.glob(os.path.join(directory, "*dist-info", "METADATA"))
    return_dict: Dict[Requirement, List[str]] = {}
    for metadata_file in metadata_files:
        parent_folder = os.path.dirname(metadata_file)
        package = get_package_name_from_metadata(metadata_file)

        if package is not None:
            # Determine which folders or files belong to this package
            record_file_path = os.path.join(parent_folder, "RECORD")
            if os.path.exists(record_file_path):
                # Get unique root folder names
                with open(record_file_path, encoding="utf-8") as record_file:
                    record_entries = list(
                        {
                            os.path.split(line)[0].split(",")[0]
                            for line in record_file.readlines()
                        },
                    )
                    included_record_entries = []
                    for record_entry in record_entries:
                        record_entry_full_path = os.path.abspath(
                            os.path.join(directory, record_entry),
                        )
                        # RECORD file might contain relative paths to items outside target folder. (ignore these)
                        if (
                            os.path.exists(record_entry_full_path)
                            and directory in record_entry_full_path
                            and len(record_entry) > 0
                        ):
                            included_record_entries.append(record_entry)
                    package_req = Requirement.parse(package)
                    return_dict[package_req] = included_record_entries
    return return_dict


def identify_supported_packages(
    packages: List[Requirement],
    valid_packages: Dict[str, List[str]],
    native_packages: Set[str],
) -> Tuple[List[Requirement], List[Requirement], List[Requirement]]:
    supported_dependencies: List = []
    dropped_dependencies: List = []
    new_dependencies: List = []
    for package in packages:
        package_name = package.name
        package_version_req = package.specs[0][1] if package.specs else None
        if package_name in valid_packages:
            if (package_version_req is None) or (
                package_version_req in valid_packages[package_name]
            ):
                supported_dependencies.append(package)
            elif package_name in native_packages:
                # Native packages should anaconda dependencies if possible, even if the version is not available
                _logger.warning(
                    f"Package {package_name}(version {package_version_req}) is an unavailable native "
                    f"dependency, switching to latest available version "
                    f"{valid_packages[package_name][-1]} instead."
                )
                dropped_dependencies.append(package)
                new_dependencies.append(Requirement.parse(package_name))
                native_packages.remove(package_name)
            elif package_name in DEFAULT_PACKAGES:
                _logger.warning(
                    f"Package {package_name}(version {package_version_req}) is an unavailable default "
                    f"dependency, switching to latest available version "
                    f"{valid_packages[package_name][-1]} instead."
                )
                dropped_dependencies.append(package)
                new_dependencies.append(Requirement.parse(package_name))

    return supported_dependencies, dropped_dependencies, new_dependencies


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
        process_output = "\n".join([line.strip() for line in process.stdout])
        _logger.debug(process_output)
    except FileNotFoundError:
        raise ModuleNotFoundError(
            f"Pip not found. Please install pip in your environment or specify the "
            f"{PIP_ENVIRONMENT_VARIABLE} environment variable and try again!"
        )

    if pip_install_result is not None and pip_install_result != 0:
        raise ValueError(f"Pip failed with return code {pip_install_result}.")


def detect_native_dependencies(
    target: str, downloaded_packages_dict: Dict[Requirement, List[str]]
) -> Set[str]:
    def invert_package_map(
        downloaded_packages_dict: Dict[Requirement, List[str]]
    ) -> Dict[str, Set[str]]:
        inverted_dictionary: Dict[str, Set[str]] = {}
        for requirement, root_folders in downloaded_packages_dict.items():
            for root_folder in root_folders:
                if root_folder not in inverted_dictionary:
                    inverted_dictionary[root_folder] = {requirement.name}
                else:
                    inverted_dictionary[root_folder].add(requirement.name)
        return inverted_dictionary

    native_libraries = set()
    native_extensions = (
        {".pyd", ".pxd", ".dll"} if platform.system() == "Windows" else {".so"}
    )
    for native_extension in native_extensions:
        glob_output = glob.glob(os.path.join(target, "**", f"*{native_extension}"))
        if glob_output:
            folder_to_package_map = invert_package_map(downloaded_packages_dict)
            for path in glob_output:
                relative_path = os.path.relpath(path, target)
                root_directory = os.path.split(relative_path)[0]
                if root_directory in folder_to_package_map:
                    library_set = folder_to_package_map[root_directory]
                    for library in library_set:
                        if library not in native_libraries:
                            _logger.info(f"Potential native library: {library}")
                            native_libraries.add(library)
    return native_libraries


def zip_directory_contents(directory_path: str, output_path: str) -> None:
    directory_path = Path(directory_path)
    output_path = Path(output_path)
    with zipfile.ZipFile(
        output_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True
    ) as zipf:
        for file in directory_path.rglob("*"):
            zipf.write(file, file.relative_to(directory_path))

        parent_directory = directory_path.parent

        for file in parent_directory.iterdir():
            if (
                file.is_file()
                and not file.match(".*")
                and file != output_path
                and file != directory_path
            ):
                zipf.write(file, file.relative_to(parent_directory))


def add_snowpark_package(
    result_dict: Dict[str, str], valid_packages: Dict[str, List[str]]
) -> None:
    if SNOWPARK_PACKAGE_NAME not in result_dict:
        result_dict[SNOWPARK_PACKAGE_NAME] = SNOWPARK_PACKAGE_NAME
        try:
            package_client_version = pkg_resources.get_distribution(
                SNOWPARK_PACKAGE_NAME
            ).version
            if package_client_version in valid_packages[SNOWPARK_PACKAGE_NAME]:
                result_dict[
                    SNOWPARK_PACKAGE_NAME
                ] = f"{SNOWPARK_PACKAGE_NAME}=={package_client_version}"
            else:
                _logger.warning(
                    f"The version of package '{SNOWPARK_PACKAGE_NAME}' in the local environment is "
                    f"{package_client_version}, which is not available in Snowflake. Your UDF might not work when "
                    f"the package version is different between the server and your local environment."
                )
        except pkg_resources.DistributionNotFound:
            _logger.warning(
                f"Package '{SNOWPARK_PACKAGE_NAME}' is not installed in the local environment. "
                f"Your UDF might not work when the package is installed on the server "
                f"but not on your local environment."
            )
        except Exception as ex:  # pragma: no cover
            _logger.warning(
                "Failed to get the local distribution of package %s: %s",
                SNOWPARK_PACKAGE_NAME,
                ex,
            )
