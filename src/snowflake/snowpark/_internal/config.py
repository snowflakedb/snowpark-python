#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations
import pkg_resources

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TypeVar, Any, Callable, TYPE_CHECKING
from snowflake.snowpark._internal.utils import warning

from snowflake.snowpark._internal.utils import (
    warn_session_config_update_in_multithreaded_mode,
)

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session

SettingType = TypeVar("SettingType")


@dataclass
class Setting:
    name: str
    description: str | None = field(default=None)
    default: SettingType | None = field(default=None)
    read_only: bool = field(default=False)
    experimental_since: str | None = field(default=None)

    def __post_init__(self):
        self._value = None
        self._parent = None
        self._type = None if self.default is None else type(self.default)

    def _get(self) -> SettingType:
        if self._value is not None:
            return self._value
        elif (
            self._parent is not None
            and (parent_value := self._parent.value) is not None
        ):
            return parent_value
        else:
            return self.default

    @property
    def value(self) -> SettingType:
        return self._get()

    def _set(self, new_value: SettingType):
        if self.read_only:
            raise RuntimeError(
                f"setting {self.name} is read_only and cannot be changed at runtime."
            )
        if self._type and not isinstance(new_value, self._type):
            raise ValueError(
                f"Value for parameter {self.name} must be of type {self._type.__name__}."
            )
        if self.experimental_since and new_value != self.default:
            warning_text = f"Parameter {self.name} is experimental since {self.experimental_since}. Do not use it in production."
            warning(self.name, warning_text)
        self._value = new_value

    @value.setter
    def value(self, new_value: SettingType):
        self._set(new_value)


@dataclass
class SettingGroup(Setting):
    settings: list[Setting] = field(default_factory=list)

    def __post_init__(self):
        for setting in self.settings:
            setting._parent = self


@dataclass
class SessionParameter(Setting):
    session: Session = field(default=None)
    parameter_name: str = field(default=None)
    synchronize: bool = field(default=False)
    telemetry_hook: Callable = field(default=None)

    def __post_init__(self):
        super().__post_init__()
        # Inheritance is tricky with dataclasses until python 3.10.
        # All fields have to be optional if the parent class has any optional fields.
        if self.session is None:
            raise ValueError("session is a required parameter for SessionParameter")
        if self.parameter_name is None:
            raise ValueError(
                "parameter_name is a required parameter for SessionParameter"
            )

    def _get(self) -> SettingType:
        with self.session._lock:
            if self._value is None:
                self._value = self.session._conn._get_client_side_session_parameter(
                    self.parameter_name, self.default
                )
            return super()._get()

    def _set(self, new_value: SettingType):
        if self.read_only:
            raise RuntimeError(
                f"session parameter {self.name} cannot be changed at runtime."
            )
        warn_session_config_update_in_multithreaded_mode(
            self.name, self.session._conn._thread_safe_session_enabled
        )
        with self.session._lock:
            if self.telemetry_hook:
                self.telemetry_hook(self.session._session_id, new_value)
            if self.synchronize:
                self.session._conn._cursor.execute(
                    f"alter session set {self.parameter_name} = {new_value}"
                )
            super()._set(new_value)


@dataclass
class VersionedSessionParameter(SessionParameter):
    def _get(self) -> SettingType:
        with self.session._lock:
            if self._value is None:
                version = self.session._conn._get_client_side_session_parameter(
                    self.parameter_name, ""
                )
                self._value = (
                    isinstance(version, str)
                    and version != ""
                    and pkg_resources.parse_version(self.session.version)
                    >= pkg_resources.parse_version(version)
                )
            return self._value


class SettingStore:
    def __init__(
        self, settings: Iterable[Setting], extend_from: SettingStore | None = None
    ) -> None:
        self._settings = dict()
        if extend_from is not None:
            self.add(extend_from._settings.values())
        self.add(settings)

    def _add(self, setting: Setting):
        if isinstance(setting, SettingGroup):
            for s in setting.settings:
                self._settings[s.name] = s
        self._settings[setting.name] = setting

    def add(self, setting: Iterable[Setting] | Setting):
        if isinstance(setting, Iterable):
            for param in setting:
                self._add(param)
        else:
            self._add(setting)

    def set(self, setting_name: str, value: Any):
        if setting_name in self._settings:
            self._settings[setting_name].value = value
        else:
            raise ValueError(f"Unable to set setting. Unknown setting {setting_name}")

    def get(self, setting_name: str, default: Any | None = None):
        if setting_name in self._settings:
            return self._settings[setting_name].value
        else:
            return default

    def update(self, options: dict[str, Any]):
        for k, v in options.items():
            if k in self._settings:
                self.set(k, v)

    def __getitem__(self, instance):
        return self.get(instance)

    def __setitem__(self, instance, value):
        self.set(instance, value)


GLOBAL_SETTINGS = SettingStore([])
