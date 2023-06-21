#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os

import pytest

from snowflake.snowpark._internal.packaging_utils import (
    get_downloaded_packages,
    install_pip_packages_to_target_folder,
)


@pytest.fixture(scope="module")
def temp_directory(tmpdir_factory):
    temp_dir = tmpdir_factory.mktemp("temp_dir")
    return temp_dir


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
