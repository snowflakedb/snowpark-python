#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
from typing import Optional, Tuple

_FILE_STAGE_MAP = {}


def put(
    local_file_name: str, stage_location: str
) -> Tuple[
    str, str, int, int, Optional[str], Optional[str], Optional[str], Optional[str]
]:
    """
    {
        '"source"': (StringType(), False),
        '"target"': (StringType(), False),
        '"source_size"': (DecimalType(10, 0), False),
        '"target_size"': (DecimalType(10, 0), False),
        '"source_compression"': (StringType(), False),
        '"target_compression"': (StringType(), False),
        '"status"': (StringType(), False),
        '"encryption"': (StringType(), False),
        '"message"': (StringType(), False)
    }
    [PutResult(source='test_data.csv', target='test_data.csv.gz', source_size=54, target_size=96,
    source_compression='NONE', target_compression='GZIP', status='UPLOADED', message='')]
    """
    local_file_name = local_file_name[
        8:-1
    ]  # skip normalized prefix `file:// and suffix `
    file_name = os.path.basename(local_file_name)
    remote_file_path = f"{stage_location}/{file_name}"
    _FILE_STAGE_MAP[remote_file_path] = local_file_name
    file_size = os.path.getsize(os.path.expanduser(local_file_name))
    return file_name, file_name, file_size, file_size, None, None, None, None
