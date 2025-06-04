#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import importlib


class MissingOptionalDependency:
    """Local implementation to avoid importing from snowflake.connector.options"""

    _dep_name = "unknown"

    def __init__(self, *args, **kwargs):  # noqa: FIR100
        pass

    def __getattr__(self, name):
        raise RuntimeError(
            f"Local Testing requires {self._dep_name} as dependency, "
            f"please make sure {self._dep_name} is installed in the environment.\n"
        )


class MissingPandas(MissingOptionalDependency):
    """Local implementation to avoid importing from snowflake.connector.options"""

    _dep_name = "pandas"


class _LazyPandasProxy:
    def __init__(self) -> None:  # noqa: FIR100
        self._pandas = None
        self._is_checked = False
        self._is_installed = None

    def _ensure_checked(self):
        if not self._is_checked:
            try:
                self._pandas = importlib.import_module("pandas")
                self._is_installed = True
            except ImportError:
                self._pandas = MissingPandas()
                self._is_installed = False
            self._is_checked = True

    def __getattr__(self, name):
        self._ensure_checked()
        return getattr(self._pandas, name)

    def __bool__(self):
        self._ensure_checked()
        return self._is_installed

    def __repr__(self):
        self._ensure_checked()
        return repr(self._pandas)


class _LazyInstalledPandas:
    def __init__(self, pandas_proxy) -> None:  # noqa: FIR100
        self._pandas_proxy = pandas_proxy

    def __bool__(self):
        self._pandas_proxy._ensure_checked()
        return self._pandas_proxy._is_installed

    def __eq__(self, other):
        return bool(self) == other

    def __repr__(self):
        return str(bool(self))


pandas = _LazyPandasProxy()
installed_pandas = _LazyInstalledPandas(pandas)


class MissingNumpy(MissingOptionalDependency):
    """The class is specifically for numpy optional dependency."""

    _dep_name = "numpy"


try:
    numpy = importlib.import_module("numpy")
    installed_numpy = True
except ImportError:
    numpy = MissingNumpy()
    installed_numpy = False
