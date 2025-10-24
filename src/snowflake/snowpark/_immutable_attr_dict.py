#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import keyword
import json

from collections.abc import Mapping

from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    Tuple,
    Optional,
    Union,
)


class ImmutableAttrDict(Mapping):
    """
    An immutable mapping whose (identifier-valid, non-keyword, non-private) keys
    are also available as read-only attributes. Nested mappings are recursively
    wrapped as ImmutableAttrDict instances.

    By default also recursively freezes:
        - dict / Mapping  -> ImmutableAttrDict
        - list / tuple    -> tuple
        - set             -> frozenset
        - other types     -> unchanged

    Access:
        d.foo   (if 'foo' is a valid identifier key)
        d['foo'] (always works)

    Invalid / reserved keys (not identifiers, keywords, or starting with '_')
    are still accessible via item lookup but not as attributes.
    """

    __slots__ = ("_data", "_frozen")

    # ------------- Construction / conversion helpers -----------------
    @classmethod
    def _convert(cls, value: Any) -> Any:
        """Recursively convert nested structures."""
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            # Ensure plain dict form for predictability
            return cls(value)
        if isinstance(value, list) or isinstance(value, tuple):
            return tuple(cls._convert(v) for v in value)
        if isinstance(value, set):
            return frozenset(cls._convert(v) for v in value)
        # For other iterables you could add handling if desired.
        return value

    @staticmethod
    def _is_exposable_attr_name(name: str) -> bool:
        """Return True if name can safely be set as an attribute."""
        return bool(
            name
            and name[0] != "_"
            and name.isidentifier()
            and not keyword.iskeyword(name)
        )

    def __init__(
        self,
        mapping: Optional[Union[Mapping[str, Any], Iterable[Tuple[str, Any]]]] = None,
        **kwargs: Any,
    ) -> None:
        combined: Dict[str, Any] = {}
        if mapping is not None:
            if isinstance(mapping, Mapping):
                combined.update(mapping)
            else:
                for k, v in mapping:
                    combined[k] = v
        combined.update(kwargs)

        # Deep-convert values first
        converted: Dict[str, Any] = {k: self._convert(v) for k, v in combined.items()}

        object.__setattr__(self, "_data", converted)

        # Expose attribute names only for "safe" keys
        for k, v in converted.items():
            if self._is_exposable_attr_name(k):
                object.__setattr__(self, k, v)

        object.__setattr__(self, "_frozen", True)

    # ------------- Factory methods -----------------
    @classmethod
    def from_json(cls, j: str) -> "ImmutableAttrDict":
        parsed = json.loads(j)
        if not isinstance(parsed, Mapping):
            raise TypeError("JSON root must be an object to build ImmutableAttrDict")
        return cls(parsed)

    # ------------- Mapping interface -----------------
    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    # ------------- Attribute fallback -----------------
    def __getattr__(self, name: str) -> Any:
        data = object.__getattribute__(self, "_data")
        if name in data and self._is_exposable_attr_name(name):
            return data[name]
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )

    # ------------- Immutability enforcement -----------------
    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
            return
        if object.__getattribute__(self, "_frozen"):
            raise AttributeError(
                f"{type(self).__name__} is immutable; cannot set attribute {name!r}"
            )
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        if name.startswith("_"):
            object.__delattr__(self, name)
            return
        if object.__getattribute__(self, "_frozen"):
            raise AttributeError(
                f"{type(self).__name__} is immutable; cannot delete attribute {name!r}"
            )
        object.__delattr__(self, name)

    # ------------- Utilities -----------------
    def to_dict(self) -> Dict[str, Any]:
        """
        Return a shallow copy of the underlying storage.
        Nested ImmutableAttrDict instances are preserved (not converted).
        For a fully "plain" structure you can use to_plain().
        """
        return dict(self._data)

    def to_plain(self) -> Dict[str, Any]:
        """
        Recursively convert back to plain Python types:
            ImmutableAttrDict -> dict
            tuple/frozenset   -> list (or list for frozenset)
        """

        def unwrap(v: Any) -> Any:
            if isinstance(v, ImmutableAttrDict):
                return {k: unwrap(v._data[k]) for k in v._data}
            if isinstance(v, tuple):
                return [unwrap(x) for x in v]
            if isinstance(v, frozenset):
                return [unwrap(x) for x in v]
            return v

        return {k: unwrap(v) for k, v in self._data.items()}

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data})"

    def __reduce__(self):
        return (type(self), (self._data,))
