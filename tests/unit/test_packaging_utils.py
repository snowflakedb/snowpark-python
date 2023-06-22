#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
import zipfile
from unittest.mock import patch

import pytest
from pkg_resources import Requirement

from snowflake.snowpark._internal.packaging_utils import (
    detect_native_dependencies,
    get_downloaded_packages,
    get_package_name_from_metadata,
    identify_supported_packages,
    install_pip_packages_to_target_folder,
    zip_directory_contents,
)


@pytest.fixture(scope="function")
def temp_directory(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("temp_dir")
    return temp_dir


def test_get_downloaded_packages(temp_directory):
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

    downloaded_packages = get_downloaded_packages(str(temp_directory))
    assert {key.name for key in downloaded_packages.keys()} == set(package_names)
    for key in downloaded_packages.keys():
        assert set(downloaded_packages[key]) == {
            key.name,
            f"folder2_{key.name}",
            f"folder2_{key.name}/nested_directory",
        }


def test_get_downloaded_packages_malformed(temp_directory):
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

    downloaded_packages = get_downloaded_packages(str(temp_directory))
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


def test_get_downloaded_packages_for_real_python_packages(temp_directory):
    packages = ["requests", "numpy", "pandas"]
    target_folder = os.path.join(temp_directory, "packages")
    install_pip_packages_to_target_folder(packages, target_folder)
    for package in packages:
        assert os.path.exists(os.path.join(target_folder, package))
    downloaded_packages_dict = get_downloaded_packages(target_folder)
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


def test_get_package_name_from_metadata(temp_directory):
    metadata_file_path = temp_directory.join("METADATA")
    metadata_file_path.write("Name: my_package\nVersion: 1.0.0")

    package_name = get_package_name_from_metadata(str(metadata_file_path))

    assert package_name == "my_package==1.0.0"


def test_zip_directory_contents(temp_directory):
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
        packages, valid_packages, native_packages
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
        "streamlit": ["0.85.0", "0.86.0"],
    }
    native_packages = {"numpy", "pandas"}

    # Case 1: All packages supported
    packages = [Requirement.parse("numpy==1.2"), Requirement.parse("pandas")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages
    )
    assert supported == packages
    assert dropped == []
    assert new == []

    # Case 2: One package not supported
    packages = [Requirement.parse("numpy==1.3"), Requirement.parse("pandas")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages
    )
    assert supported == [Requirement.parse("pandas")]
    assert dropped == [Requirement.parse("numpy==1.3")]
    assert new == [Requirement.parse("numpy")]

    # Case 3: Native package version not available, should switch to latest available version
    packages = [Requirement.parse("numpy==2.0"), Requirement.parse("pandas")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages
    )
    assert supported == [Requirement.parse("pandas")]
    assert dropped == [Requirement.parse("numpy==2.0")]
    assert new == [Requirement.parse("numpy")]

    # Case 4: Default package version not available, should switch to latest available version
    packages = [Requirement.parse("streamlit==0.84.0")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages
    )
    assert supported == []
    assert dropped == [Requirement.parse("streamlit==0.84.0")]
    assert new == [Requirement.parse("streamlit")]

    # Case 5: Package not in valid_packages and not a native package or a default package
    packages = [Requirement.parse("somepackage")]
    supported, dropped, new = identify_supported_packages(
        packages, valid_packages, native_packages
    )
    assert supported == []
    assert dropped == []
    assert new == []


def test_valid_pip_install(temp_directory):
    packages = ["requests", "numpy", "pandas"]
    target_folder = os.path.join(temp_directory, "packages")
    install_pip_packages_to_target_folder(packages, target_folder)
    for package in packages:
        assert os.path.exists(os.path.join(target_folder, package))


def test_invalid_package_name(temp_directory):
    packages = ["some_invalid_package_name"]
    target_folder = os.path.join(temp_directory, "packages")
    with pytest.raises(ValueError):
        install_pip_packages_to_target_folder(packages, target_folder)


def test_no_pip(monkeypatch, temp_directory):
    packages = ["requests"]
    target_folder = os.path.join(temp_directory, "packages")
    monkeypatch.setenv("PIP_NAME", "/invalid/path/to/pip")

    with pytest.raises(ModuleNotFoundError):
        install_pip_packages_to_target_folder(packages, target_folder)


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
