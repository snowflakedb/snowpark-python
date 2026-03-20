#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re


def test_pattern_with_infer_strips_leading_slash():
    """Regression test: PATTERN->FILES must strip the leading slash after
    removing the cloud prefix. Without lstrip('/'), INFER_SCHEMA fails with
    'Remote file not found' and silently falls back to unfiltered inference."""
    stage_locations_no_trailing_slash = ["s3://my-bucket"]
    stage_locations_with_trailing_slash = ["s3://my-bucket/"]

    list_results = [
        ("s3://my-bucket/subdir/data/part-00000.parquet", 1024, "md5", "2024-01-01"),
        ("s3://my-bucket/subdir/data/part-00001.parquet", 2048, "md5", "2024-01-01"),
    ]

    def strip_files(matches, locations):
        regexes = [re.compile(f"^{re.escape(loc)}", re.IGNORECASE) for loc in locations]
        files = []
        for match in matches:
            prefix = match[0]
            for regex in regexes:
                prefix = regex.sub("", prefix)
            files.append(prefix.lstrip("/"))
        return files

    files_no_slash = strip_files(list_results, stage_locations_no_trailing_slash)
    assert files_no_slash == [
        "subdir/data/part-00000.parquet",
        "subdir/data/part-00001.parquet",
    ]
    assert not any(f.startswith("/") for f in files_no_slash)

    files_with_slash = strip_files(list_results, stage_locations_with_trailing_slash)
    assert files_with_slash == [
        "subdir/data/part-00000.parquet",
        "subdir/data/part-00001.parquet",
    ]
