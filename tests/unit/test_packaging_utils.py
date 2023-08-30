#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
import zipfile
from subprocess import TimeoutExpired
from unittest.mock import patch

import pkg_resources
import pytest
from pkg_resources import Requirement

from snowflake.snowpark._internal.packaging_utils import (
    SNOWPARK_PACKAGE_NAME,
    add_snowpark_package,
    detect_native_dependencies,
    get_package_name_from_metadata,
    get_signature,
    identify_supported_packages,
    map_python_packages_to_files_and_folders,
    pip_install_packages_to_target_folder,
    zip_directory_contents,
)
from tests.utils import IS_IN_STORED_PROC


@pytest.fixture(scope="function")
def temp_directory(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("temp_dir")
    return temp_dir


def test_get_downloaded_packages(temp_directory):
    """
    To test get_downloaded_packages(), we create two fake python packages with distribution information,
    metadata and record file. Metadata will contain information about the package name and version while
    record file contains information about the files corresponding to the package.

    We assert that the returned dictionary maps package names to unique folders relevant to the package.
    """
    package_names = ["package1", "package2"]
    for package in package_names:
        dist_info_folder = f"{package}-0.1.dist-info"
        dist_info_folder_path = os.path.join(temp_directory, dist_info_folder)
        os.makedirs(dist_info_folder_path)
        with open(os.path.join(dist_info_folder_path, "METADATA"), "w") as f:
            f.write(f"Name: {package}")
        with open(os.path.join(dist_info_folder_path, "RECORD"), "w") as f:
            f.write(f"{package}/file.py,sha256=hash,341243\n")
            f.write(f"folder2_{package}/file1.py,sha512=hash,2312312\n")
            f.write(f"folder2_{package}/nested_directory/file2.py,hash,22131312\n")
            f.write(
                f"folder_not_exist_{package}/nested_directory/file2.py,hash,22131312\n"
            )

        folder1_path = os.path.join(temp_directory, package)
        os.makedirs(folder1_path)
        with open(os.path.join(folder1_path, "file1.py"), "w") as f:
            f.write("content")

        folder2_path = os.path.join(
            temp_directory, f"folder2_{package}", "nested_directory"
        )
        os.makedirs(folder2_path)
        with open(os.path.join(folder2_path, "file2.py"), "w") as f:
            f.write("content")

        with open(os.path.join(folder1_path, "METADATA"), "w") as f:
            f.write(f"Name: {package}")

    downloaded_packages = map_python_packages_to_files_and_folders(str(temp_directory))
    assert {key.name for key in downloaded_packages.keys()} == set(package_names)
    for key in downloaded_packages.keys():
        assert set(downloaded_packages[key]) == {
            key.name,
            f"folder2_{key.name}",
            f"folder2_{key.name}/nested_directory",
        }


def test_get_downloaded_packages_malformed(temp_directory):
    """
    Assert that when packages are malformed, no errors are raised; instead, we proceed with processing non-malformed
    packages (in this case 'package03'). 'package01' is missing a RECORD file while 'package02' has a malformed
    METADATA file.
    """
    package_names = ["package01", "package02", "package03"]
    for package in package_names:
        dist_info_folder = f"{package}-0.1.dist-info"
        dist_info_folder_path = os.path.join(temp_directory, dist_info_folder)
        os.makedirs(dist_info_folder_path)
        with open(os.path.join(dist_info_folder_path, "METADATA"), "w") as f:
            f.write(f"Name: {package}" if package != package_names[1] else "Malformed")

        if package != package_names[0]:
            with open(os.path.join(dist_info_folder_path, "RECORD"), "w") as f:
                f.write(f"{package}/file.py,sha256=hash,341243\n")
                f.write(f"folder2_{package}/file1.py,sha512=hash,2312312\n")
                f.write(f"folder2_{package}/nested_directory/file2.py,hash,22131312\n")
                f.write(
                    f"folder_not_exist_{package}/nested_directory/file2.py,hash,22131312\n"
                )

        folder1_path = os.path.join(temp_directory, package)
        os.makedirs(folder1_path)
        with open(os.path.join(folder1_path, "file1.py"), "w") as f:
            f.write("content")

        folder2_path = os.path.join(
            temp_directory, f"folder2_{package}", "nested_directory"
        )
        os.makedirs(folder2_path)
        with open(os.path.join(folder2_path, "file2.py"), "w") as f:
            f.write("content")

        with open(os.path.join(folder1_path, "METADATA"), "w") as f:
            f.write(f"Name: {package}")

    downloaded_packages = map_python_packages_to_files_and_folders(str(temp_directory))
    print(downloaded_packages)
    assert {key.name for key in downloaded_packages.keys()} == {
        package_names[2]
    }  # Other two packages are malformed
    for key in downloaded_packages.keys():
        assert set(downloaded_packages[key]) == {
            key.name,
            f"folder2_{key.name}",
            f"folder2_{key.name}/nested_directory",
        }


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures.",
)
def test_get_downloaded_packages_for_real_python_packages(temp_directory):
    """
    Assert that for genuine pypi packages, get_downloaded_packages() actually picks up the correct package names.
    To avoid flakiness, we only check for package names.
    """
    packages = ["requests", "numpy", "pandas"]
    target_folder = os.path.join(temp_directory, "packages")
    pip_install_packages_to_target_folder(packages, target_folder)
    for package in packages:
        assert os.path.exists(os.path.join(target_folder, package))
    downloaded_packages_dict = map_python_packages_to_files_and_folders(target_folder)
    assert len(downloaded_packages_dict) > 0
    assert all(
        len(downloaded_packages_dict[key]) > 0 for key in downloaded_packages_dict
    )
    package_names = {package.name for package in downloaded_packages_dict.keys()}
    for package_name in packages + [
        "six",
        "pytz",
    ]:  # six and pytz are integral dependencies
        assert package_name in package_names


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures",
)
def test_pip_timeout(temp_directory):
    """
    Assert that timeout parameter works fine.
    """
    packages = ["requests", "numpy", "pandas"]
    target_folder = os.path.join(temp_directory, "packages")
    with pytest.raises(TimeoutExpired):
        pip_install_packages_to_target_folder(packages, target_folder, timeout=1)


def test_get_package_name_from_metadata(temp_directory):
    metadata_file_path = temp_directory.join("METADATA")
    metadata_file_path.write("Name: my_package\nVersion: 1.0.0")

    package_name = get_package_name_from_metadata(str(metadata_file_path))

    assert package_name == "my_package==1.0.0"


def test_zip_directory_contents(temp_directory):
    """
    Asserts that zip_directory_contents()  zips and stores the correct files.
    """
    with open(os.path.join(temp_directory, "file0.txt"), "w") as f:
        f.write("zero_content")

    folder_path = os.path.join(temp_directory, "to_be_zipped_folder")
    os.makedirs(folder_path)
    with open(os.path.join(folder_path, "file.txt"), "w") as f:
        f.write("content")

    zip_folder_path = os.path.join(temp_directory, "zip_folder.zip")
    zip_directory_contents(folder_path, zip_folder_path)
    assert os.path.isfile(zip_folder_path)

    extract_path = os.path.join(temp_directory, "extracted")
    with zipfile.ZipFile(zip_folder_path, "r") as zip_ref:
        zip_ref.extractall(extract_path)

    extract_file_path = os.path.join(temp_directory, "extracted", "file.txt")
    assert os.path.isfile(extract_file_path)
    with open(extract_file_path) as f:
        assert f.read() == "content"

    extract_zero_file_path = os.path.join(temp_directory, "extracted", "file0.txt")
    assert os.path.isfile(extract_zero_file_path)
    with open(extract_zero_file_path) as f:
        assert f.read() == "zero_content"


def test_identify_supported_packages_vanilla():
    """
    Assert that the most straightforward usage of identify_supported_packages() works
    """
    packages = [
        Requirement.parse("package1==1.0.0"),
        Requirement.parse("package2==2.0.0"),
        Requirement.parse("package3"),
        Requirement.parse("package4==2.1.2"),
    ]
    valid_packages = {
        "package1": ["1.0.0", "1.1.0"],
        "package2": ["2.0.0", "2.1.0"],
        "package4": ["2.1.0", "2.1.1"],
    }
    native_packages = {"package4"}

    supported_deps, dropped_deps, new_deps = identify_supported_packages(
        packages, valid_packages, native_packages, {}
    )

    assert len(supported_deps) == 2
    assert packages[0] in supported_deps
    assert packages[1] in supported_deps
    assert len(dropped_deps) == 1
    assert packages[3] in dropped_deps
    assert len(new_deps) == 1
    assert Requirement.parse("package4") in new_deps


def test_identify_supported_packages_all_cases():
    # Define the valid_packages and native_packages for our test
    valid_packages = {
        "numpy": ["1.0", "1.1", "1.2"],
        "pandas": ["1.0", "1.1", "1.2"],
    }

    # Case 1: All packages supported
    native_packages = {"pandas"}
    packages = [Requirement.parse("numpy==1.2"), Requirement.parse("pandas")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages, {}
    )
    assert supported == packages
    assert dropped == []
    assert new == []
    assert native_packages == set()

    # Case 2: One non-native package, version not supported
    native_packages = {"pandas"}
    packages = [Requirement.parse("numpy==10.0"), Requirement.parse("pandas")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages, {}
    )
    assert supported == [Requirement.parse("pandas")]
    assert dropped == []
    assert new == []
    assert native_packages == set()

    # Case 3: Native package version not available, should switch to latest available version
    native_packages = {"numpy", "pandas"}
    packages = [Requirement.parse("numpy==10.0"), Requirement.parse("pandas")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages, {}
    )
    assert supported == [Requirement.parse("pandas")]
    assert dropped == [Requirement.parse("numpy==10.0")]
    assert new == [Requirement.parse("numpy")]
    assert native_packages == set()

    # Case 4: Package not in valid_packages and not a native package either
    native_packages = {"numpy", "pandas"}
    packages = [Requirement.parse("somepackage")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages, {}
    )
    assert supported == []
    assert dropped == []
    assert new == []
    assert native_packages == {"numpy", "pandas"}


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures",
)
def test_valid_pip_install(temp_directory):
    packages = ["requests", "numpy", "pandas"]
    target_folder = os.path.join(temp_directory, "packages")
    pip_install_packages_to_target_folder(packages, target_folder)
    for package in packages:
        assert os.path.exists(os.path.join(target_folder, package))


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures",
)
def test_invalid_package_name(temp_directory):
    packages = ["some_invalid_package_name"]
    target_folder = os.path.join(temp_directory, "packages")
    with pytest.raises(RuntimeError):
        pip_install_packages_to_target_folder(packages, target_folder)


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures",
)
def test_no_pip(monkeypatch, temp_directory):
    packages = ["requests"]
    target_folder = os.path.join(temp_directory, "packages")
    monkeypatch.setenv("PIP_NAME", "/invalid/path/to/pip")

    with pytest.raises(ModuleNotFoundError):
        pip_install_packages_to_target_folder(packages, target_folder)


def test_detect_native_dependencies():
    target = "/path/to/target"
    downloaded_packages_dict = {
        Requirement.parse("numpy"): ["numpy"],
        Requirement.parse("pandas"): ["pandas"],
    }

    # Mock the glob.glob function to return specific paths
    with patch("glob.glob") as mock_glob:
        # Case 1: No .so files found
        mock_glob.return_value = []
        result = detect_native_dependencies(target, downloaded_packages_dict)
        assert result == set()

        # Case 2: .so files found, associated with a package
        mock_glob.return_value = ["/path/to/target/numpy/file.so"]
        result = detect_native_dependencies(target, downloaded_packages_dict)
        assert result == {"numpy"}

        # Case 3: .so files found, not associated with a package
        mock_glob.return_value = ["/path/to/target/unknown/file.so"]
        result = detect_native_dependencies(target, downloaded_packages_dict)
        assert result == set()


def test_add_snowpark_package():
    version = "1.3.0"
    valid_packages = {SNOWPARK_PACKAGE_NAME: [version]}
    result_dict = {}
    with patch("pkg_resources.get_distribution") as mock_get_distribution:
        mock_get_distribution.return_value.version = version
        add_snowpark_package(result_dict, valid_packages)
        assert result_dict == {SNOWPARK_PACKAGE_NAME: f"{SNOWPARK_PACKAGE_NAME}==1.3.0"}


def test_add_snowpark_package_if_missing():
    version = "1.3.0"
    valid_packages = {SNOWPARK_PACKAGE_NAME: [version]}
    result_dict = {}
    with patch("pkg_resources.get_distribution") as mock_get_distribution:
        mock_get_distribution.side_effect = pkg_resources.DistributionNotFound(
            "Package not found"
        )
        add_snowpark_package(result_dict, valid_packages)  # Should not raise any error
        assert result_dict == {SNOWPARK_PACKAGE_NAME: SNOWPARK_PACKAGE_NAME}


def test_get_signature():
    lists = [
        ["numpy", "pandas"],
        ["pandas", "numpy"],
        ["numpy==1.1.1", "pandas"],
        ["some_different_text"],
        ["pandas", "numpy==1.1.1"],
    ]
    signatures = [get_signature(package_list) for package_list in lists]

    assert signatures[0] == signatures[1]
    assert signatures[2] == signatures[4]
    assert signatures[0] != signatures[2]
    assert signatures[0] != signatures[3]
    assert signatures[2] != signatures[3]
