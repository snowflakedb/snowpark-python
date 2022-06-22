from typing import List, NamedTuple, Optional

import snowflake.snowpark

class PutResult(NamedTuple):
    source: str
    target: str
    source_size: int
    target_size: int
    source_compression: str
    target_compression: str
    status: str
    message: str

class GetResult(NamedTuple):
    file: str
    size: str
    status: str
    message: str

class FileOperation:
    def __init__(self, session: snowflake.snowpark.session.Session) -> None: ...
    def put(
        self,
        local_file_name: str,
        stage_location: str,
        *,
        parallel: int = ...,
        auto_compress: bool = ...,
        source_compression: str = ...,
        overwrite: bool = ...
    ) -> List[PutResult]: ...
    def get(
        self,
        stage_location: str,
        target_directory: str,
        *,
        parallel: int = ...,
        pattern: Optional[str] = ...
    ) -> List[GetResult]: ...
