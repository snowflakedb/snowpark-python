#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
This module provides a central location for defining various forms of configuration that them
snowpark-python module uses. It seeks to provide an extensible uniform api for storing and
retrieving configuration.

This module has two main classes. :class:Setting stores a single confugration settings and all
relevant information to it. :class:SettingStore stores a collection of Settings and relevant logic
for manipulating them.

This module defines a global SettingStore called GLOBAL_SETTINGS which should be used for package
wide configuration options. Other SettingStore instances can include global configurations by
passing GLOBAL_SETTINGS in via the extend_from parameter during initialization. This is useful
when all configuration needs to be accessible from a single object.
"""

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
    from snowflake.snowpark.session import Session  # pragma: no cover

SettingType = TypeVar("SettingType")


@dataclass
class Setting:
    """
    A dataclass that describes the attributes of a single configuration setting.

    Attributes:
        name (str): The name of the setting that is used to reference it.
        description (str): A docstring that describes the function of the setting.
        default: (Any): The default value that a setting takes when not otherwise configured.
        read_only (bool): Disallows modification of the setting when set to True.
        experimental_since (str): When set this will warn users that changing the value of this
            setting is experimental and may not be ready for production environments.
    """

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
    """
    A specialized Setting that represents a logical grouping of settings. Child settings will default
    to the overall group setting if no other value is specified.

    Attributes:
        settings: (list[Setting]): Settings that are grouped up in this Setting.
    """

    settings: list[Setting] = field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        for setting in self.settings:
            setting._parent = self


@dataclass
class SessionParameter(Setting):
    """
    A specialized Setting that retrieves its value from a session.

    Attributes:
        session (Session): The Session that will be used to retrieve this settings value.
        parameter_name (str): The name of the session parameter that holds the value for this setting.
        synchronize (bool): When set to True the server side session parameter will be updated when
            this settings value is changed.
        telemetry_hook (Callable): A callback function that allows emitting telemetry when thissettings
            settings value is changed.
    """

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
    """
    A specialized SessionParamter that sets its value based on wether or not the server side reports
    that a feature is supported or not by the current package version.
    """

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
        """
        An object that stores one or more Settings.

        Args:
            settings (Iterable[Setting]): The settings that this instance should store.
            extend_from (SettingStore | None, optional): When set this instance will add references
                to all Settings in provided SettingStore. Values modified on those settings are
                reflected in the parent store.
        """
        self._settings = dict()
        self._parent = extend_from
        self.add(settings)

    def _add(self, setting: Setting):
        if isinstance(setting, SettingGroup):
            for s in setting.settings:
                self._settings[s.name] = s
        self._settings[setting.name] = setting

    def add(self, setting: Iterable[Setting] | Setting):
        """
        Adds a new setting to the store.
        """
        if isinstance(setting, Iterable):
            for param in setting:
                self._add(param)
        else:
            self._add(setting)

    def set(self, setting_name: str, value: Any):
        """
        Sets the value of the provided setting to the provided value.
        """
        if setting_name in self._settings:
            self._settings[setting_name].value = value
        elif self._parent:
            self._parent.set(setting_name, value)
        else:
            raise ValueError(f"Unable to set setting. Unknown setting {setting_name}")

    def _get(self, setting_name: str) -> Any:
        if setting_name in self._settings:
            return self._settings[setting_name].value
        elif (
            self._parent
            and (parent_value := self._parent._get(setting_name)) is not None
        ):
            return parent_value
        return None

    def get(self, setting_name: str, default: Any | None = None) -> Any:
        """
        Retrieves the value for the given setting or returns a default value if the setting does not exist.
        """
        value = self._get(setting_name)
        if value:
            return value
        return default

    def update(self, options: dict[str, Any]):
        """
        Updates the value of multiples settings at once.

        Args:
            options (dict[str, Any]): A dictionary of setting name to updated value.
        """
        for k, v in options.items():
            if k in self._settings:
                self.set(k, v)
            elif self._parent:
                self._parent.set(k, v)

    def __getitem__(self, instance):
        return self.get(instance)

    def __setitem__(self, instance, value):
        self.set(instance, value)


GLOBAL_SETTINGS = SettingStore([])
