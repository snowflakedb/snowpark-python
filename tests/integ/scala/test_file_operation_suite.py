#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
import random
import shutil
import string

import pytest

from snowflake.snowpark._internal.utils import is_in_stored_procedure
from snowflake.snowpark.exceptions import (
    SnowparkSQLException,
    SnowparkUploadFileException,
)
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils


def random_alphanumeric_name():
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(5)
    )


@pytest.fixture(scope="module")
def temp_source_directory(tmpdir_factory):
    directory = tmpdir_factory.mktemp("snowpark_test_source")
    yield directory
    if not is_in_stored_procedure():
        shutil.rmtree(str(directory))


@pytest.fixture(scope="module")
def temp_target_directory(tmpdir_factory):
    directory = tmpdir_factory.mktemp("snowpark_test_target")
    yield directory
    if not is_in_stored_procedure():
        shutil.rmtree(str(directory))


@pytest.fixture(scope="module")
def path1(temp_source_directory):
    file = temp_source_directory.join(f"file_1_{Utils.random_alphanumeric_str(10)}.csv")
    file.write_text("abc, 123,\n", encoding="UTF-8")
    yield str(file)


@pytest.fixture(scope="module")
def path2(temp_source_directory):
    file = temp_source_directory.join(f"file_2_{Utils.random_alphanumeric_str(10)}.csv")
    file.write_text("abc, 123,\n", encoding="UTF-8")
    yield str(file)


@pytest.fixture(scope="module")
def path3(temp_source_directory):
    file = temp_source_directory.join(f"file_3_{Utils.random_alphanumeric_str(10)}.csv")
    file.write_text("abc, 123,\n", encoding="UTF-8")
    yield str(file)


@pytest.fixture(scope="module")
def path4(temp_source_directory):
    import gzip

    file = temp_source_directory.join(
        f"file_4_{Utils.random_alphanumeric_str(10)}.csv.gz"
    )
    filename = str(file)
    with gzip.open(filename, "wb") as f:
        f.write(b"abc, 123,\n")
    yield filename


@pytest.fixture(scope="module")
def temp_stage(session, resources_path, local_testing_mode):
    tmp_stage_name = Utils.random_stage_name()
    test_files = TestFiles(resources_path)

    if not local_testing_mode:
        Utils.create_stage(session, tmp_stage_name, is_temporary=True)
    Utils.upload_to_stage(
        session, tmp_stage_name, test_files.test_file_parquet, compress=False
    )
    yield tmp_stage_name
    if not local_testing_mode:
        Utils.drop_stage(session, tmp_stage_name)


def test_put_with_one_file(session, temp_stage, path1, path2, path3):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"
    first_result = session.file.put(f"file://{path1}", stage_with_prefix)[0]
    first_result_with_statement_params = session.file.put(
        f"file://{path1}",
        stage_with_prefix,
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )[0]
    assert (
        first_result.source
        == first_result_with_statement_params.source
        == os.path.basename(path1)
    )
    assert (
        first_result.target
        == first_result_with_statement_params.target
        == os.path.basename(path1) + ".gz"
    )
    assert first_result.source_size in (
        10,
        11,
    ) and first_result_with_statement_params.source_size in (10, 11)
    assert first_result.target_size in (
        64,
        96,
    ) and first_result_with_statement_params.target_size in (0, 64)
    assert (
        first_result.source_compression
        == first_result_with_statement_params.source_compression
        == "NONE"
    )
    assert (
        first_result.target_compression
        == first_result_with_statement_params.target_compression
        == "GZIP"
    )
    assert (
        first_result.status == "UPLOADED"
        and first_result_with_statement_params.status in ("SKIPPED", "UPLOADED")
    )
    assert first_result.message == first_result_with_statement_params.message == ""
    # Scala has encryption but python doesn't
    # assert first_result.encryption == "DECRYPTED"

    second_result = session.file.put(
        f"file://{path2}", stage_with_prefix, auto_compress=False
    )[0]
    assert second_result.source == os.path.basename(path2)
    assert second_result.target == os.path.basename(path2)
    assert second_result.source_size in (10, 11)
    assert second_result.target_size in (16, 17)
    assert second_result.source_compression == "NONE"
    assert second_result.target_compression == "NONE"
    assert second_result.status == "UPLOADED"
    assert second_result.message == ""

    # PUT another file: without "file://" and "@" for localFileName and stageLocation
    # put() will add "file://" for localFileName, add "@" for stageLocation
    third_result = session.file.put(path3, f"{temp_stage}/{stage_prefix}/")[0]
    assert third_result.source == os.path.basename(path3)
    assert third_result.target == os.path.basename(path3) + ".gz"
    assert third_result.source_size in (10, 11)
    assert third_result.target_size in (64, 96)
    assert third_result.source_compression == "NONE"
    assert third_result.target_compression == "GZIP"
    assert third_result.status == "UPLOADED"
    assert third_result.message == ""


def test_put_with_one_file_twice(session, temp_stage, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"
    session.file.put(f"file://{path1}", stage_with_prefix)

    # put the same file again
    second_result = session.file.put(
        f"file://{path1}", stage_with_prefix, overwrite=False
    )[0]
    assert second_result.source == os.path.basename(path1)
    assert second_result.target == os.path.basename(path1) + ".gz"
    assert second_result.source_size in (10, 11)
    # On GCP, the files are not skipped if target file already exists
    assert second_result.target_size in (0, 64, 96)
    assert second_result.source_compression == "NONE"
    assert second_result.target_compression == "GZIP"
    assert second_result.status in ("SKIPPED", "UPLOADED")
    # Scala has "message" field. Python has an empty "message"
    # assert "File with same destination name and checksum already exists" in second_result.message


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="cannot write file to root directory in sandbox"
)
def test_put_with_one_relative_path_file(session, temp_stage, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"
    file_name = os.path.basename(path1)
    shutil.copyfile(path1, file_name)
    try:
        first_result = session.file.put(f"file://{file_name}", stage_with_prefix)[0]
        assert first_result.source == os.path.basename(path1)
        assert first_result.target == os.path.basename(path1) + ".gz"
        assert first_result.source_size in (10, 11)
        assert first_result.target_size in (64, 96)
        assert first_result.source_compression == "NONE"
        assert first_result.target_compression == "GZIP"
        assert first_result.status == "UPLOADED"
        assert first_result.message == ""
    finally:
        os.remove(file_name)


def test_put_with_multiple_files(
    session, temp_stage, temp_source_directory, path1, path2, path3
):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"
    first_result = session.file.put(
        f"file://{temp_source_directory}/*", stage_with_prefix
    )
    assert len(first_result) == 3
    assert all(row.status == "UPLOADED" for row in first_result)

    # Upload again.
    second_result = session.file.put(
        f"file://{temp_source_directory}/*", stage_with_prefix
    )
    assert len(second_result) == 3
    # On GCP, the files are not skipped if target file already exists
    assert all(row.status in ("UPLOADED", "SKIPPED") for row in second_result)


def test_put_negative(session, temp_stage, temp_source_directory, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    with pytest.raises(SnowparkSQLException) as file_not_exist_info:
        session.file.put(
            f"file://{temp_source_directory}/not_exists_file.txt", stage_with_prefix
        )
    assert "File doesn't exist" in str(file_not_exist_info)

    with pytest.raises(SnowparkSQLException) as stage_not_exist_info:
        session.file.put(f"file://{path1}", "@NOT_EXIST_STAGE_NAME_TEST")
    assert "does not exist or not authorized." in str(stage_not_exist_info)


def test_put_stream_with_one_file(session, temp_stage, path1, path2, path3, path4):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}"
    file_name = os.path.basename(path1)
    with open(path1, "rb") as fd:
        first_result = session.file.put_stream(fd, f"{stage_with_prefix}/{file_name}")
    assert first_result.source == file_name
    assert first_result.target == file_name + ".gz"
    assert first_result.source_size is not None
    assert first_result.target_size is not None
    assert first_result.source_compression == "NONE"
    assert first_result.target_compression == "GZIP"
    assert first_result.status == "UPLOADED"
    assert first_result.message == ""

    file_name = os.path.basename(path2)
    with open(path2, "rb") as fd:
        second_result = session.file.put_stream(
            fd, f"{stage_with_prefix}/{file_name}", auto_compress=False
        )
    assert second_result.source == file_name
    assert second_result.target == file_name
    assert second_result.source_size is not None
    assert second_result.target_size is not None
    assert second_result.source_compression == "NONE"
    assert second_result.target_compression == "NONE"
    assert second_result.status == "UPLOADED"
    assert second_result.message == ""

    # PUT file at path3 without "@" in stageLocation
    file_name = os.path.basename(path3)
    with open(path3, "rb") as fd:
        third_result = session.file.put_stream(
            fd, f"{temp_stage}/{stage_prefix}/{file_name}"
        )
    assert third_result.source == file_name
    assert third_result.target == file_name + ".gz"
    assert third_result.source_size is not None
    assert third_result.target_size is not None
    assert third_result.source_compression == "NONE"
    assert third_result.target_compression == "GZIP"
    assert third_result.status == "UPLOADED"
    assert third_result.message == ""

    # test auto_detect to gzip
    file_name = os.path.basename(path4)
    with open(path4, "rb") as fd:
        fourth_result = session.file.put_stream(fd, f"{stage_with_prefix}/{file_name}")
    assert fourth_result.source == file_name
    assert fourth_result.target == file_name
    assert fourth_result.source_size is not None
    assert fourth_result.target_size is not None
    assert fourth_result.source_compression == "GZIP"
    assert fourth_result.target_compression == "GZIP"
    assert fourth_result.status == "UPLOADED"
    assert fourth_result.message == ""


def test_put_stream_with_one_file_twice(session, temp_stage, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}"
    file_name = os.path.basename(path1)
    fd = open(path1, "rb")
    session.file.put_stream(fd, f"{stage_with_prefix}/{file_name}")

    # put same file again
    second_result = session.file.put_stream(
        fd, f"{stage_with_prefix}/{file_name}", overwrite=False
    )
    fd.close()
    assert second_result.source == os.path.basename(path1)
    assert second_result.target == os.path.basename(path1) + ".gz"
    # On GCP, the files are not skipped if target file already exists
    assert second_result.source_size in (10, 11)
    assert second_result.target_size in (0, 32)
    assert second_result.source_compression == "NONE"
    assert second_result.target_compression == "GZIP"
    assert second_result.status in ("SKIPPED", "UPLOADED")
    assert second_result.message == ""


def test_put_stream_negative(session, temp_stage, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}"
    file_name = os.path.basename(path1)
    fd = open(path1, "rb")

    with pytest.raises(ValueError) as ex_info:
        session.file.put_stream(fd, "")
    assert "stage_location cannot be empty" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.file.put_stream(fd, stage_with_prefix + "/")
    assert "stage_location should end with target filename" in str(ex_info)

    fd.close()
    if is_in_stored_procedure():
        with pytest.raises(ValueError) as ex_info:
            session.file.put_stream(fd, f"{stage_with_prefix}/{file_name}")
        assert "seek of closed file" in str(ex_info)
    else:
        with pytest.raises(SnowparkUploadFileException) as ex_info:
            session.file.put_stream(fd, f"{stage_with_prefix}/{file_name}")
        assert ex_info.value.error_code == "1408"


@pytest.mark.parametrize("with_file_prefix", [True, False])
def test_get_one_file(
    session, temp_stage, temp_target_directory, path1, with_file_prefix
):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    session.file.put(
        f"{'file://' if with_file_prefix else ''}{path1}", stage_with_prefix
    )
    results = session.file.get(
        f"{stage_with_prefix}{os.path.basename(path1)}.gz", str(temp_target_directory)
    )  # temp_target_directory
    results_with_statement_params = session.file.get(
        f"{stage_with_prefix}{os.path.basename(path1)}.gz",
        str(temp_target_directory),
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )  # temp_target_directory
    try:
        assert len(results) == len(results_with_statement_params) == 1
        assert (
            results[0].file
            == results_with_statement_params[0].file
            == f"{os.path.basename(path1)}.gz"
        )
        assert results[0].size in (54, 55) and results_with_statement_params[
            0
        ].size in (54, 55)
        assert (
            results[0].status == results_with_statement_params[0].status == "DOWNLOADED"
        )
        # Scala has encryption but python doesn't
        # assert results[0].encryption == "DECRYPTED"
        assert results[0].message == results_with_statement_params[0].message == ""
    finally:
        os.remove(f"{temp_target_directory}/{os.path.basename(path1)}.gz")


def test_get_multiple_files(
    session, temp_stage, temp_target_directory, path1, path2, path3
):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    session.file.put(f"file://{path1}", stage_with_prefix)
    session.file.put(f"file://{path2}", stage_with_prefix)
    session.file.put(f"file://{path3}", stage_with_prefix, auto_compress=False)

    results = session.file.get(stage_with_prefix, str(temp_target_directory))
    try:
        assert len(results) == 3
        assert results[0].file == os.path.basename(f"{path1}.gz")
        assert results[1].file == os.path.basename(f"{path2}.gz")
        assert results[2].file == os.path.basename(f"{path3}")

        assert results[0].size in (54, 55)
        assert results[1].size in (54, 55)
        assert results[2].size in (10, 11)
    finally:
        os.remove(f"{temp_target_directory}/{os.path.basename(path1)}.gz")
        os.remove(f"{temp_target_directory}/{os.path.basename(path2)}.gz")
        os.remove(f"{temp_target_directory}/{os.path.basename(path3)}")


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-570941: get with pattern is not supported"
)
def test_get_with_pattern_and_relative_target_directory(
    session, temp_stage, temp_target_directory, path1, path2, path3
):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    session.file.put(f"file://{path1}", stage_with_prefix)
    session.file.put(f"file://{path2}", stage_with_prefix)
    session.file.put(f"file://{path3}", stage_with_prefix, auto_compress=False)

    results = session.file.get(
        stage_with_prefix, str(temp_target_directory), pattern="'.*\\.csv\\.gz'"
    )

    try:
        assert len(results) == 2
        assert results[0].file == os.path.basename(f"{path1}.gz")
        assert results[1].file == os.path.basename(f"{path2}.gz")
    finally:
        os.remove(f"{temp_target_directory}/{os.path.basename(path1)}.gz")
        os.remove(f"{temp_target_directory}/{os.path.basename(path2)}.gz")


@pytest.mark.skip("Error 'max_workers must be greater than 0' on Azure and GCP")
def test_get_negative_test(session, temp_stage, temp_target_directory, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    # Stage name doesn't exist, raise exception.
    with pytest.raises(SnowparkSQLException) as exec_info:
        session.file.get("@NOT_EXIST_STAGE_NAME_TEST", str(temp_target_directory))
    assert "does not exist or not authorized." in str(exec_info)

    # If stage name exists but prefix doesn't exist, download nothing
    get_results = session.file.get(
        f"@{temp_stage}/not_exist_prefix_test/", str(temp_target_directory)
    )
    assert len(get_results) == 0

    put_results = session.file.put(path1, stage_with_prefix)
    assert len(put_results) == 1
    assert put_results[0].status == "UPLOADED"
    get_results = session.file.get(stage_with_prefix, "not_exist_target_test/test2")
    try:
        assert len(get_results) == 1
        assert get_results[0].status == "DOWNLOADED"
    finally:
        shutil.rmtree("not_exist_target_test")


@pytest.mark.skip(
    "Python connector doesn't have COLLISION in the result"
    "This error sometimes happen probably because python-connector doesn't handle file conflict well."
    "snowflake.connector.errors.OperationalError: 253002: FileNotFoundError(2, 'No such file or directory')"
)
def test_get_negative_test_file_name_collision(
    session, temp_stage, tmpdir_factory, path1
):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    session.file.put(f"file://{path1}", stage_with_prefix + "prefix_1")
    session.file.put(f"file://{path1}", stage_with_prefix + "prefix_2")
    target_directory = tmpdir_factory.mktemp("collision_test_target")
    try:
        results = session.file.get(stage_with_prefix, f"file://{str(target_directory)}")
        assert len(results) == 2
        assert results[0].status == "DOWNLOADED"
        # GCP doesn't detect download collision
        assert (
            results[1].status == "COLLISION"
            and "has same name as" in results[1].message
        ) or (results[1].status == "DOWNLOADED" and results[1].message == "")
    finally:
        if not is_in_stored_procedure():
            shutil.rmtree(target_directory)


@pytest.mark.parametrize("auto_compress", [True, False])
@pytest.mark.parametrize("with_file_prefix", [True, False])
def test_get_stream(session, temp_stage, with_file_prefix, auto_compress, path1):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    put_result = session.file.put(
        f"{'file://' if with_file_prefix else ''}{path1}",
        stage_with_prefix,
        auto_compress=auto_compress,
    )
    with open(path1, "rb") as fd:
        file_content = fd.read()

    fd = session.file.get_stream(
        f"{stage_with_prefix}{put_result[0].target}", decompress=auto_compress
    )
    assert fd.read() == file_content
    fd.close()


def test_get_stream_negative(session, temp_stage):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"@{temp_stage}/{stage_prefix}/"

    with pytest.raises(ValueError) as ex_info:
        session.file.get_stream("   ")
    assert "stage_location cannot be empty" in str(ex_info)

    with pytest.raises(ValueError) as ex_info:
        session.file.get_stream(stage_with_prefix)
    assert "stage_location should end with target file name"

    if is_in_stored_procedure():
        pytest.skip("Skip in stored-proc to prevent XP failing whole test")
    with pytest.raises(SnowparkSQLException) as ex_info:
        session.file.get_stream(f"{stage_with_prefix}non_existing_file")
    assert "the file does not exist" in str(ex_info)


def test_quoted_local_file_name(session, temp_stage, tmp_path_factory):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    stage_with_prefix = f"'@{temp_stage}/{stage_prefix}/'"
    special_directory = tmp_path_factory.mktemp("dir !_")
    try:
        special_path1 = special_directory.joinpath("file_!.txt")
        special_path1.write_text("aaa")
        special_path2 = special_directory.joinpath("file_.txt")
        special_path2.write_text("bbb")
        put1 = session.file.put(
            f"'file://{Utils.escape_path(str(special_path1))}'", stage_with_prefix
        )
        assert len(put1) == 1
        put2 = session.file.put(
            f"'file://{Utils.escape_path(str(special_path2))}'", stage_with_prefix
        )
        assert len(put2) == 1

        dest_directory = tmp_path_factory.mktemp("dir !_")
        get1 = session.file.get(
            stage_with_prefix, f"'file://{Utils.escape_path(str(dest_directory))}'"
        )
        assert len(get1) == 2
        assert len(list(dest_directory.iterdir())) == 2
    finally:
        if not is_in_stored_procedure():
            shutil.rmtree(special_directory)


def test_path_with_special_chars(session, tmp_path_factory):
    stage_prefix = f"prefix_{random_alphanumeric_name()}"
    temp_stage = "s peci'al chars"
    Utils.create_stage(session, f'"{temp_stage}"', is_temporary=False)
    stage_with_prefix = f'"{temp_stage}"/{stage_prefix}/"'
    special_directory = tmp_path_factory.mktemp("dir !_")
    try:
        special_path1 = special_directory.joinpath("file_!.txt")
        special_path1.write_text("aaa")
        special_path2 = special_directory.joinpath("file_.txt")
        special_path2.write_text("bbb")
        put1 = session.file.put(
            f"file://{Utils.escape_path(str(special_path1))}", stage_with_prefix
        )
        assert len(put1) == 1
        put2 = session.file.put(
            f"file://{Utils.escape_path(str(special_path2))}", stage_with_prefix
        )
        assert len(put2) == 1

        dest_directory = tmp_path_factory.mktemp("dir !_")
        get1 = session.file.get(
            stage_with_prefix, f"file://{Utils.escape_path(str(dest_directory))}"
        )
        assert len(get1) == 2
        assert len(list(dest_directory.iterdir())) == 2
    finally:
        Utils.drop_stage(session, temp_stage)
        if not is_in_stored_procedure():
            shutil.rmtree(special_directory)
