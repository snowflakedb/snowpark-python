#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

# import pytest
# import os
# import io
#
# from pkg_resources import Requirement
#
# from snowflake.snowpark import Session
# from typing import List, Tuple
#
# from snowflake.snowpark._internal.packaging_utils import get_downloaded_packages, get_package_name_from_metadata, \
#     identify_supported_packages
#
#
# @pytest.fixture(scope="module")
# def temp_directory(tmpdir_factory):
#     temp_dir = tmpdir_factory.mktemp("temp_dir")
#     return temp_dir
#
#
# @pytest.fixture(scope="module")
# def sample_function():
#     def test_func(x: int) -> int:
#         return x + 1
#
#     return test_func
#
#
# def test_get_downloaded_packages(temp_directory):
#     metadata_dir = os.path.join(temp_directory, "test_package.dist-info")
#     os.makedirs(metadata_dir, exist_ok=True)
#
#     metadata_file1 = os.path.join(metadata_dir, "METADATA")
#     package_name1 = "test_package1"
#     version1 = "1.0.0"
#     metadata_content1 = f"Name: {package_name1}\nVersion: {version1}"
#     with open(metadata_file1, "w") as f:
#         f.write(metadata_content1)
#
#     metadata_dir = os.path.join(temp_directory, "test_package2.dist-info")
#     os.makedirs(metadata_dir, exist_ok=True)
#     metadata_file2 = os.path.join(metadata_dir, "METADATA")
#     package_name2 = "test_package2"
#     version2 = "2.0.0"
#     metadata_content2 = f"Name: {package_name2}\nVersion: {version2}"
#     with open(metadata_file2, "w") as f:
#         f.write(metadata_content2)
#     result = get_downloaded_packages(temp_directory)
#     assert len(result) == 2
#     assert f"{package_name1}=={version1}" in result
#     assert f"{package_name2}=={version2}" in result
#
#
# def test_get_package_name_from_metadata(temp_directory):
#     metadata_file_path = temp_directory.join("METADATA")
#     metadata_file_path.write("Name: my_package\nVersion: 1.0.0")
#
#     package_name = get_package_name_from_metadata(str(metadata_file_path))
#
#     assert package_name == "my_package==1.0.0"
#
#
# def test_identify_supported_packages():
#     packages = [
#         Requirement.parse("package1==1.0.0"),
#         Requirement.parse("package2==2.0.0"),
#         Requirement.parse("package3"),
#         Requirement.parse("package4==2.1.2"),
#     ]
#     valid_packages = {
#         "package1": ["1.0.0", "1.1.0"],
#         "package2": ["2.0.0", "2.1.0"],
#         "package4": ["2.1.0", "2.1.1"]
#     }
#     native_packages = {"package4"}
#
#     supported_deps, dropped_deps, new_deps = identify_supported_packages(
#         packages, valid_packages, native_packages
#     )
#
#     assert len(supported_deps) == 2
#     assert packages[0] in supported_deps
#     assert packages[1] in supported_deps
#     assert len(dropped_deps) == 1
#     assert packages[3] in dropped_deps
#     assert len(new_deps) == 1
#     assert Requirement.parse("package4") in new_deps
