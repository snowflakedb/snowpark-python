#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
# The code in this file is largely a copy of https://github.com/Snowflake-Labs/snowcli/blob/main/src/snowcli/utils.py
import glob
import hashlib
import os
import platform
import re
import shutil
import subprocess
import sys
import zipfile
from logging import getLogger
from pathlib import Path
from typing import AnyStr, Dict, List, Optional, Set, Tuple

import pkg_resources
import yaml
from pkg_resources import Requirement

_logger = getLogger(__name__)
PIP_ENVIRONMENT_VARIABLE: str = "PIP_NAME"
IMPLICIT_ZIP_FILE_NAME: str = "zipped_packages"
ENVIRONMENT_METADATA_FILE_NAME: str = "environment_metadata"
SNOWPARK_PACKAGE_NAME: str = "snowflake-snowpark-python"
DEFAULT_PACKAGES = ["wheel", "pip", "setuptools"]
NATIVE_FILE_EXTENSIONS: Set[str] = {
    ".pyd",
    ".pyx",
    ".pxd",
    ".dylib",
    ".dll" if platform.system() == "Windows" else ".so",
}


def parse_requirements_text_file(file_path: str) -> Tuple[List[str], List[str]]:
    """
    Parses a requirements.txt file to obtain a list of packages and file/folder imports. Returns a tuple of packages
    and imports.
    Args:
        file_path (str): Local requirements file path (text file).
    Returns:
        Tuple[List[str], List[str]] - Packages and imports.
    """
    packages: List[str] = []
    imports: List[str] = []
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            if line and len(line) > 0:
                if os.path.exists(line) and ("\\" in line or "/" in line):
                    imports.append(line)
                else:
                    packages.append(line)
    return packages, imports


def parse_conda_environment_yaml_file(
    file_path: str,
) -> Tuple[List[str], Optional[str]]:
    """
    Parses a Conda environment file (see https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually)
    Python version passed in is used as the runtime version for sprocs/udfs.
    Conda-style dependencies (numpy=1.2.3) are converted to pip-style dependencies (numpy==1.2.3).
    Args:
        file_path (str): Local requirements file path (yaml file).
    Returns:
        Tuple[List[str], Optional[str]] - Packages and Python runtime version, if specified. (Note that you cannot
        specify local file or folder imports in a conda environment yaml file).
    """
    packages: List[str] = []
    runtime_version: Optional[str] = None
    with open(file_path) as f:
        try:
            environment_data = yaml.safe_load(f)
            dependencies = environment_data.get("dependencies", [])
            for dep in dependencies:
                if isinstance(dep, str):
                    dep = dep.strip()
                    if any(r in dep for r in (">", "<")):
                        raise ValueError(
                            f"Conda dependency with ranges '{dep}' is not supported! Please specify a single version."
                        )
                    tokens = dep.split("=")
                    name = tokens[0]
                    version = tokens[1] if len(tokens) > 1 else None
                    if name == "python":
                        version_tokens = version.split(".")
                        runtime_version = ".".join(
                            version_tokens[: min(len(version_tokens), 2)]
                        )  # Ignore micro version
                    elif name == "pip":
                        continue
                    else:
                        packages.append(
                            name if version is None else f"{name}=={version}"
                        )
                elif isinstance(dep, dict) and "pip" in dep:
                    packages.extend([package.strip() for package in dep["pip"]])
        except yaml.YAMLError as e:
            raise ValueError(
                f"Error while parsing YAML file, it may not be a valid Conda environment file: {e}"
            )
    return packages, runtime_version


def delete_files_belonging_to_packages(
    packages: List[Requirement],
    package_to_file_and_folder_mapping: Dict[Requirement, List[str]],
    target: str,
) -> None:
    """
    Deletes files and folders belonging to a list of given packages.

    Args:
        packages (List[Requirement]): List of package names that need to be deleted from the `target` folder.
        package_to_file_and_folder_mapping (Dict[Requirement, List[str]]): Mapping from package object to a list of file
        and  folder paths that belong to the package.
        target (str): Absolute path of local folder where the cleanup needs to be performed.
    """
    for package_req in packages:
        files = package_to_file_and_folder_mapping[package_req]
        for file in files:
            item_path = os.path.join(target, file)
            if os.path.exists(item_path):
                if os.path.isdir(item_path):  # Remove a directory
                    shutil.rmtree(item_path)
                else:  # Remove a file
                    os.remove(item_path)


def get_package_name_from_metadata(metadata_file_path: str) -> Optional[str]:
    """
    Loads a METADATA file from the dist-info directory of an installed Python package, finds the name and version of the
    package. The name is found on the line containing "Name: package_name" and version can be found on the line containing
    "Version: package_version".

    Args:
        metadata_file_path (str): The path to the METADATA file.

    Returns:
        Optional[str]: The name and (if present) version of the package formatted as f"{package}==[version]". Returns
        None if package name cannot be found.
    """

    with open(metadata_file_path, encoding="utf-8") as metadata_file:
        contents: AnyStr = metadata_file.read()
        regex_results = re.search("^Name: (.*)$", contents, flags=re.MULTILINE)
        if regex_results is None:
            return None
        requirement_line: str = regex_results.group(1)

        regex_results = re.search("^Version: (.*)$", contents, flags=re.MULTILINE)
        if regex_results is not None:
            version: str = regex_results.group(1)
            requirement_line += f"=={version}"

        return requirement_line.strip().lower()


def map_python_packages_to_files_and_folders(
    directory: str,
) -> Dict[Requirement, List[str]]:
    """
    Records correspondence between installed python packages and their folder structure, using the RECORD file present
    in most pypi packages. We use the METADATA file to deduce the package name and version, and RECORD file to map
    correspondence between package names and folders/files.

    Example RECORD file entry:
    numpy/polynomial/setup.py,sha256=dXQfzVUMP9OcB6iKv5yo1GLEwFB3gJ48phIgo4N-eM0,373

    Example METADATA file entry:
    Metadata-Version: 2.1
    Name: numpy
    Version: 1.24.3
    Summary: Fundamental package for array computing in Python

    Args:
        directory (str): Target folder in which pip installed the packages.

    Returns:
        Dict[Requirement, List[str]: Mapping from package to a list of unique folder/file names that correspond to it.
    """

    package_name_to_record_entries_map: Dict[Requirement, List[str]] = {}

    metadata_files: List[str] = glob.glob(
        os.path.join(directory, "*dist-info", "METADATA")
    )
    for metadata_file in metadata_files:
        parent_folder: str = os.path.dirname(metadata_file)
        package: Optional[str] = get_package_name_from_metadata(metadata_file)

        if package is not None:
            # Determine which folders or files belong to this package
            record_file_path = os.path.join(parent_folder, "RECORD")
            if os.path.exists(record_file_path):
                # Get unique root folder names
                with open(record_file_path, encoding="utf-8") as record_file:
                    record_entries = set()

                    # Read in all record entries
                    for line in record_file.readlines():
                        entry = os.path.split(line)[0].split(",")[0]
                        if entry == "":  # If true, a file present in the root folder
                            entry = line.split(",")[0]
                        record_entries.add(entry)

                    # Only select unique base folders or files
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

                    # Create Requirement objects and store in map
                    package_name_to_record_entries_map[
                        Requirement.parse(package)
                    ] = included_record_entries

    return package_name_to_record_entries_map


def identify_supported_packages(
    packages: List[Requirement],
    valid_packages: Dict[str, List[str]],
    native_packages: Set[str],
    package_dict: Dict[str, str],
) -> Tuple[List[Requirement], List[Requirement], List[Requirement]]:
    """
    Detects which `packages` are present in the Snowpark Anaconda channel using the `valid_packages` mapping.
    If a package is a native dependency (belongs to `native_packages` set) and supported in Anaconda, we switch to
    the latest available version in Anaconda.

    Note that we also update the `native_packages` set to reflect genuinely problematic native dependencies, i.e.
    packages that are not present in Anaconda and are likely to cause errors.

    Args:
        packages (List[Requirement]): List of python packages that are either requested by the user or a dependency of a requested package.
        valid_packages (Dict[str, List[str]): Mapping from package name to a list of versions available on the Anaconda
        channel.
        native_packages (Set[str]): Set of packages that contain native code. (either packages requested by users and
        unavailable in anaconda or dependencies of requested packages)
        package_dict (Dict[str, str]): A dictionary of package name -> package spec of packages that have
            been added explicitly so far using add_packages() or other such methods.

    Returns:
        Tuple[List[Requirement], List[Requirement], List[Requirement]]: Tuple containing dependencies that are present
        in Anaconda, dependencies that should be dropped from the package list and dependencies that should be added.
    """

    supported_dependencies: List[Requirement] = []
    dropped_dependencies: List[Requirement] = []
    new_dependencies: List[Requirement] = []
    packages_to_be_uploaded: List[str] = []

    for package in packages:
        package_name: str = package.name
        package_version_required: Optional[str] = (
            package.specs[0][1] if package.specs else None
        )
        version_text = (
            f"(version {package_version_required})"
            if package_version_required is not None
            else ""
        )

        if package_name in valid_packages:
            # Detect supported packages
            if (
                package_version_required is None
                or package_version_required in valid_packages[package_name]
            ):
                supported_dependencies.append(package)
                _logger.info(
                    f"Package {package_name}{version_text} is available in Snowflake! The package will not be uploaded."
                )

            # Native packages should be anaconda dependencies, even if the requested version is not available.
            elif package_name in native_packages:
                if package_name not in package_dict:
                    _logger.warning(
                        f"Package {package_name}{version_text} contains native code, switching to latest available version "
                        f"in Snowflake instead."
                    )
                    new_dependencies.append(Requirement.parse(package_name))
                dropped_dependencies.append(package)

            else:
                packages_to_be_uploaded.append(str(package))
            if package_name in native_packages:
                native_packages.remove(package_name)
        else:
            packages_to_be_uploaded.append(str(package))

    _logger.info(f"Packages that will be uploaded: {packages_to_be_uploaded}")

    return supported_dependencies, dropped_dependencies, new_dependencies


def pip_install_packages_to_target_folder(
    packages: List[str], target: str, timeout: int = 1200
) -> None:
    """
    Pip installs specified `packages` at folder specified as `target`. Pip executable can be specified using the
    environment variable PIP_PATH.

    Args:
        packages (List[str]): List of pypi packages.
        target (str): Target directory (absolute path).
        timeout (int): Seconds after which the pip install process will be killed.

    Raises:
        ModuleNotFoundError: If pip is not present.
        RuntimeError: If pip fails to install the packages.
    """
    _logger.debug(f"Using pip to install packages ({packages}), via subprocess...")
    try:
        pip_executable: str = os.getenv(PIP_ENVIRONMENT_VARIABLE)
        pip_command: List[str] = (
            [sys.executable, "-m", "pip"] if not pip_executable else [pip_executable]
        )

        process = subprocess.Popen(
            pip_command + ["install", "-t", target, *packages],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        stdout, stderr = process.communicate(timeout=timeout)

        pip_install_result: int = process.returncode
        if stdout:
            process_output: str = "\n".join(
                [line.strip() for line in stdout.split("\n")]
            )
            _logger.debug(process_output)

        if stderr:  # pragma: no cover
            error_output: str = "\n".join([line.strip() for line in stderr.split("\n")])
            _logger.warning(error_output)
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
    Detects files with native extensions present at the `target` folder, and deduces which packages own these files.
    Native dependencies use C/C++ code that won't work when uploaded via a zip file. We detect these so that we can
    switch to Anaconda-supported versions of these packages, where possible (or warn the user if it is not possible).

    We detect native dependency by looking for file extensions that correspond to native code usage (Note that this
    method is best-effort and will result in both false positives and negatives).

    Args:
        target (str): Target directory which contains packages installed by pip.
        downloaded_packages_dict (Dict[Requirement, List[str]]): Mapping between packages and a list of files or
        folders belonging to tht package.

    Returns:
        Set[str]: Set of packages that have native code. Note that we only return a set of strings here rather than Requirement
        objects because the specific version of a native package is irrelevant.
    """

    def invert_downloaded_package_to_entry_map(
        packages_dict: Dict[Requirement, List[str]]
    ) -> Dict[str, Set[str]]:
        """
        Invert dictionary mapping packages to files/folders. We need this dictionary to be inverted because we first
        discover files with native dependency extensions and then need to deduce the packages corresponding to these
        files.

        Args:
            packages_dict (Dict[Requirement, List[str]]): Mapping between packages and a list of files or folders
            corresponding to it.

        Returns:
            Dict[str, Set[str]]: The inverse mapping from a file or folder to the packages they belong to. Note that
            it is unlikely a file belongs to multiple packages (but we allow for the possibility). We only need
            to return a set of strings here rather than Requirement objects because the specific version of a native
            package is irrelevant.
        """
        record_entry_to_package_name_map: Dict[str, Set[str]] = {}
        for requirement, record_entries in packages_dict.items():
            for record in record_entries:
                record_entry_to_package_name_map.setdefault(record, set()).add(
                    requirement.name
                )

        return record_entry_to_package_name_map

    native_libraries: Set[str] = set()
    record_entries_to_package_map: Dict[
        str, Set[str]
    ] = invert_downloaded_package_to_entry_map(downloaded_packages_dict)

    for native_extension in NATIVE_FILE_EXTENSIONS:
        base_search_string: str = os.path.join(target, f"*{native_extension}")
        recursive_search_string: str = os.path.join(
            target, "**", f"*{native_extension}"
        )

        glob_output: List[str] = glob.glob(base_search_string) + glob.glob(
            recursive_search_string, recursive=True
        )
        if glob_output and len(glob_output) > 0:
            for path in glob_output:
                relative_path = os.path.relpath(path, target)

                # Fetch record entry (either base directory or a file name)
                record_entry = os.path.split(relative_path)[0]
                if (
                    record_entry == ""
                ):  # Implies the relative_path is a file name at the base directory
                    record_entry = relative_path

                if "\\" in record_entry:
                    record_entry = record_entry.replace("\\", "/")

                # Check which packages own this record entry
                if record_entry in record_entries_to_package_map:
                    package_set = record_entries_to_package_map[record_entry]
                    native_libraries.update(package_set)

    _logger.info(f"Potential native libraries: {native_libraries}")
    return native_libraries


def zip_directory_contents(target: str, output_path: str) -> None:
    """
    Zips all files/folders inside the directory path as well as those installed one level up from the directory path.

    Args:
        target (str): Target directory (absolute path) which contains packages installed by pip.
        output_path (str): Absolute path for output zip file.
    """
    target = Path(target)
    output_path = Path(output_path)
    with zipfile.ZipFile(
        output_path, "w", zipfile.ZIP_DEFLATED, allowZip64=True
    ) as zipf:
        for file in target.rglob("*"):
            zipf.write(file, file.relative_to(target))

        parent_directory = target.parent

        for file in parent_directory.iterdir():
            if (
                file.is_file()
                and not file.match(".*")
                and file != output_path
                and file != target
            ):
                zipf.write(file, file.relative_to(parent_directory))


def add_snowpark_package(
    package_dict: Dict[str, str], valid_packages: Dict[str, List[str]]
) -> None:
    """
    Adds the Snowpark Python package to package dictionary, if not present. We either choose the version available in
    the local environment or latest available on Anaconda.

    Args:
        package_dict (Dict[str, str]): Package dictionary passed in from Session object.
        valid_packages (Dict[str, List[str]]): Mapping from package name to a list of versions available on the Anaconda
        channel.

    Raises:
        pkg_resources.DistributionNotFound: If the Snowpark Python Package is not installed in the local environment.
    """
    if SNOWPARK_PACKAGE_NAME not in package_dict:
        package_dict[SNOWPARK_PACKAGE_NAME] = SNOWPARK_PACKAGE_NAME
        try:
            package_client_version = pkg_resources.get_distribution(
                SNOWPARK_PACKAGE_NAME
            ).version
            if package_client_version in valid_packages[SNOWPARK_PACKAGE_NAME]:
                package_dict[
                    SNOWPARK_PACKAGE_NAME
                ] = f"{SNOWPARK_PACKAGE_NAME}=={package_client_version}"
            else:
                _logger.warning(
                    f"The version of package '{SNOWPARK_PACKAGE_NAME}=={package_client_version}' in the local environment is "
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


def get_signature(packages: List[str]) -> str:
    """
    Create unique signature for a list of package names.
    Args:
        packages (List[str]) - A list of string package names.
    Returns:
        str - The signature.
    """
    return hashlib.sha1(str(tuple(sorted(packages))).encode()).hexdigest()
