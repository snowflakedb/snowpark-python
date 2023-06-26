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
    """
    This function records correspondence between installed python packages and their folder structure.
    When redundant packages are detected, we can use this correspondence to delete installed python packages.

    :param directory: Target folder in which pip installed the packages
    :return: Mapping from package to a list of unique folder/file names that correspond to it.
    """
    import glob
    import os

    metadata_files = glob.glob(os.path.join(directory, "*dist-info", "METADATA"))
    package_name_to_record_entries_map: Dict[Requirement, List[str]] = {}
    for metadata_file in metadata_files:
        parent_folder = os.path.dirname(metadata_file)
        package = get_package_name_from_metadata(metadata_file)

        if package is not None:
            # Determine which folders or files belong to this package
            record_file_path = os.path.join(parent_folder, "RECORD")
            if os.path.exists(record_file_path):
                # Get unique root folder names

                with open(record_file_path, encoding="utf-8") as record_file:
                    record_entries = set()
                    for line in record_file.readlines():
                        entry = os.path.split(line)[0].split(",")[0]
                        if entry == "":
                            entry = line.split(",")[0]
                        record_entries.add(entry)

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
                    package_name_to_record_entries_map[
                        package_req
                    ] = included_record_entries
    return package_name_to_record_entries_map


def identify_supported_packages(
    packages: List[Requirement],
    valid_packages: Dict[str, List[str]],
    native_packages: Set[str],
) -> Tuple[List[Requirement], List[Requirement], List[Requirement]]:
    """
    This utility function detects which packages are present in Anaconda and which of them should be switched to an
    Anaconda supported version (due to being native-dependent or default).

    :param packages: List of python packages
    :param valid_packages: Mapping from package name to a list of versions available on the Anaconda channel
    :param native_packages: List of packages that have native dependencies

    :return: tuple containing dependencies that are present in anaconda, dependencies that should be dropped from package
     list and new dependencies that should be added.
    """
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

            if package_name in native_packages:
                native_packages.remove(package_name)
    return supported_dependencies, dropped_dependencies, new_dependencies


def pip_install_packages_to_target_folder(packages: List[str], target: str):
    """
    Use pip to install packages at a certain local directory.

    :param packages: List of pypi packages to be installed.
    :param target: Target folder (temporary) where they will be installed.
    :return:
    """
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
            f"Pip not found. Please install pip in your environment or specify the path to your pip executable as "
            f"'{PIP_ENVIRONMENT_VARIABLE}' environment variable and try again."
        )

    if pip_install_result is not None and pip_install_result != 0:
        raise RuntimeError(f"Pip failed with return code {pip_install_result}.")


def detect_native_dependencies(
    target: str, downloaded_packages_dict: Dict[Requirement, List[str]]
) -> Set[str]:
    """
    Native dependencies use C/C++ code that won't work when uploaded via zip.
    We detect these so that we can switch to Anaconda-supported versions of these packages, where possible (or warn
    the user if it is not possible).

    The detection method looks for file extensions that correspond to native code usage (Note that this method is
    not perfect and will result in false positives/negatives).

    :param target: Target folder where installed packages are present.
    :param downloaded_packages_dict: Mapping between package and a list of corresponding folders.
    :return: A list of unique package names, which contain native dependencies.
    """

    def invert_package_map(
        downloaded_packages_dict: Dict[Requirement, List[str]]
    ) -> Dict[str, Set[str]]:
        inverted_dictionary: Dict[str, Set[str]] = {}
        for requirement, record_entries in downloaded_packages_dict.items():
            for record_entry in record_entries:
                if record_entry not in inverted_dictionary:
                    inverted_dictionary[record_entry] = {requirement.name}
                else:
                    inverted_dictionary[record_entry].add(requirement.name)
        return inverted_dictionary

    native_libraries = set()
    native_extensions = {
        ".pyd",
        ".pyx",
        ".pxd",
        ".dll" if platform.system() == "Windows" else ".so",
    }
    for native_extension in native_extensions:
        glob_output = glob.glob(os.path.join(target, "**", f"*{native_extension}"))
        if glob_output:
            folder_to_package_map = invert_package_map(downloaded_packages_dict)
            for path in glob_output:
                relative_path = os.path.relpath(path, target)

                # Fetch record entry (either base directory or a file name if base directory is target)
                record_entry = os.path.split(relative_path)[0]
                if record_entry == "":
                    record_entry = relative_path

                # Check which package owns this record entry
                if record_entry in folder_to_package_map:
                    library_set = folder_to_package_map[record_entry]
                    for library in library_set:
                        if library not in native_libraries:
                            _logger.info(f"Potential native library: {library}")
                            native_libraries.add(library)

        glob_output = glob.glob(os.path.join(target, f"*{native_extension}"))
        if glob_output:
            folder_to_package_map = invert_package_map(downloaded_packages_dict)
            for path in glob_output:
                relative_path = os.path.relpath(path, target)

                # Fetch record entry (either base directory or a file name if base directory is target)
                record_entry = os.path.split(relative_path)[0]
                if record_entry == "":
                    record_entry = relative_path

                # Check which package owns this record entry
                if record_entry in folder_to_package_map:
                    library_set = folder_to_package_map[record_entry]
                    for library in library_set:
                        if library not in native_libraries:
                            _logger.info(f"Potential native library: {library}")
                            native_libraries.add(library)
    return native_libraries


def zip_directory_contents(directory_path: str, output_path: str) -> None:
    """
    All files/folders inside the directory path are copied over into the zip package.
    All files/folders installed outside the directory path, which are not the directory path or the output path or
    hidden are also copied over.

    :param directory_path: Folder containing installed packages
    :param output_path: Output zip path
    :return:
    """
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
