#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Optional, Tuple

ENGINE_OPTION_PREFIX = "snowpark."
DEFAULT_BATCH_SIZE = 100000


class CaseInsensitiveDict(dict):
    """A dict whose string keys are case-insensitive. Keys are stored lower-cased, as in PySpark."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__()
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    @staticmethod
    def _key(key: Any) -> Any:
        return key.lower() if isinstance(key, str) else key

    def __setitem__(self, key: Any, value: Any) -> None:
        super().__setitem__(self._key(key), value)

    def __getitem__(self, key: Any) -> Any:
        return super().__getitem__(self._key(key))

    def __delitem__(self, key: Any) -> None:
        super().__delitem__(self._key(key))

    def __contains__(self, key: object) -> bool:
        return super().__contains__(self._key(key))

    def get(self, key: Any, default: Any = None) -> Any:
        return super().get(self._key(key), default)

    def pop(self, key: Any, *args: Any) -> Any:
        return super().pop(self._key(key), *args)

    def setdefault(self, key: Any, default: Any = None) -> Any:
        return super().setdefault(self._key(key), default)

    def update(self, *args: Any, **kwargs: Any) -> None:
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def copy(self) -> "CaseInsensitiveDict":
        return CaseInsensitiveDict(self)


def _to_option_string(value: Any) -> Optional[str]:
    # follows pyspark.sql.readwriter.to_str
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return None
    return str(value)


@dataclass
class EngineOptions:
    max_workers: Optional[int] = None
    fetch_with_process: bool = False
    batch_size: int = DEFAULT_BATCH_SIZE


def _parse_bool(key: str, value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise TypeError(f"Option '{key}' must be a boolean, got {value!r}.")


def _parse_positive_int(key: str, value: Any) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError
        result = int(value)
    except (TypeError, ValueError):
        raise TypeError(f"Option '{key}' must be an integer, got {value!r}.")
    if result <= 0:
        raise ValueError(f"Option '{key}' must be a positive integer, got {value!r}.")
    return result


# options of server-side reads, which are not supported yet
_SERVER_SIDE_OPTIONS = frozenset(
    [
        "external_access_integration",
        "packages",
        "imports",
        "artifact_repository",
        "secrets",
    ]
)

_ENGINE_OPTION_PARSERS = {
    "max_workers": _parse_positive_int,
    "fetch_with_process": _parse_bool,
    "batch_size": _parse_positive_int,
}


def split_options(
    options: Mapping[str, Any]
) -> Tuple[CaseInsensitiveDict, EngineOptions]:
    """Splits reader options into the user options passed to the data source and the
    ``snowpark.``-prefixed options that configure ingestion."""
    user_options = CaseInsensitiveDict()
    engine_kwargs = {}
    for key, value in _iter_options(options):
        lower_key = key.lower()
        if lower_key.startswith(ENGINE_OPTION_PREFIX):
            name = lower_key[len(ENGINE_OPTION_PREFIX) :]
            if name in _SERVER_SIDE_OPTIONS:
                raise NotImplementedError(
                    f"The option '{key}' configures server-side reads, which custom data sources "
                    "don't support yet."
                )
            if name not in _ENGINE_OPTION_PARSERS:
                raise ValueError(
                    f"Unknown option '{key}'. Options starting with '{ENGINE_OPTION_PREFIX}' are reserved "
                    f"for Snowpark. Supported options are "
                    f"{sorted(ENGINE_OPTION_PREFIX + n for n in _ENGINE_OPTION_PARSERS)}."
                )
            if value is not None:
                engine_kwargs[name] = _ENGINE_OPTION_PARSERS[name](key, value)
        else:
            user_options[key] = _to_option_string(value)
    engine_options = EngineOptions(**engine_kwargs)
    if engine_options.max_workers is None:
        engine_options.max_workers = os.cpu_count() or 1
    return user_options, engine_options


def _iter_options(options: Mapping[str, Any]) -> Iterator[Tuple[str, Any]]:
    for key, value in options.items():
        if not isinstance(key, str):
            raise TypeError(f"Option names must be strings, got {key!r}.")
        yield key.strip(), value
