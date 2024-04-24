#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
from typing import Optional, Tuple

from snowflake.snowpark.mock._stage_registry import (
    StageEntityRegistry,
    extract_stage_name_and_prefix,
)


def extract_import_dir_and_module_name(
    file_path: str,
    stage_registry: StageEntityRegistry,
    import_path: Optional[str] = None,
) -> Tuple[str, str]:
    file_name, file_extension = os.path.splitext(os.path.basename(file_path))
    is_on_stage = file_path.startswith("@")

    if is_on_stage:
        stage_registry = stage_registry
        stage_name, stage_prefix = extract_stage_name_and_prefix(file_path)
        local_path = stage_registry[stage_name]._working_directory + "/" + stage_prefix
    else:
        local_path = file_path

    is_python_import = file_extension in (
        ".py",
        ".zip",
        "",
    )  # directory is always considered as python module

    if not is_python_import:
        absolute_module_path = local_path
        module_name = ""
    else:
        if (
            import_path and not is_on_stage
        ):  # import_path is only considered for local python files
            module_root_dir = local_path[
                0 : local_path.rfind(import_path.replace(".", "/"))
            ]
        elif file_extension == ".py":
            module_root_dir = os.path.join(local_path, "..")
        elif file_extension == ".zip":
            module_root_dir = local_path
        else:  # directory
            module_root_dir = os.path.join(local_path, "..")

        absolute_module_path = os.path.abspath(module_root_dir)
        module_name = file_name.split(".")[
            0
        ]  # the split is for the edge case when the filename contains ., e.g. test.py.zip
    return absolute_module_path, module_name
