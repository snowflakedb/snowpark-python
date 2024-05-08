#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
from types import ModuleType
from typing import Any, Callable, Optional

# import the entire context submodule instead of just get_active_session so
# that we can mock get_active_session
import snowflake.snowpark.context
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark.session import Session, _active_sessions


def _subimport(name: str) -> ModuleType:
    """
    We need this to pickle the session holder class: https://github.com/cloudpipe/cloudpickle/issues/405#issuecomment-756085104
    """
    __import__(name)
    return sys.modules[name]


class SnowpandasSessionHolder(ModuleType):
    """
    This class implements the pattern [1] to make "session" a singleton.

    [1] https://docs.python.org/3.12/reference/datamodel.html#customizing-module-attribute-access
    """

    _session: Optional[Session] = None
    """
    The Snowpark session that Snowpark pandas DataFrame or Series will use.

    It starts as `None`, but if you try to access it when it's `None`:
      - If there is a unique active Snowpark session, snowpark assigns that one to ``session``.
      - If there are no active sessions, or multiple sessions, Snowpark will raise an exception.

    You can assign a value to this session as you would normally assign a
    value to a module property, e.g. `pd.session = session1`.
    """

    def _get_active_session(self) -> Session:
        if self._session is not None and self._session in _active_sessions:
            return self._session

        try:
            session = snowflake.snowpark.context.get_active_session()
            self._session = session
            return session
        except SnowparkSessionException as ex:
            if ex.error_code == "1409":
                raise SnowparkSessionException(
                    "There are multiple active snowpark sessions, but you need to choose one for Snowpark pandas. "
                    + "Please assign one to Snowpark pandas with a statement like `modin.pandas.session = session`."
                ) from ex
            if ex.error_code == "1403":
                raise SnowparkSessionException(
                    "Snowpark pandas requires an active snowpark session, but there is none. Please create one "
                    + "by following the instructions here: https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-session#creating-a-session"
                ) from ex
            raise

    def __setattr__(self, attr: str, value: Any) -> None:
        # If this module is modin.pandas, delegate the attribute to snowflake.snowpark.modin.pandas
        if self.__package__ == "modin.pandas" and attr == "session":
            import snowflake.snowpark.modin.pandas as spd

            setattr(spd, attr, value)
            return
        if attr == "session":
            self._session = value
        else:
            super().__setattr__(attr, value)

    def __getattr__(self, name: str) -> Any:
        # If this module is modin.pandas, delegate the attribute to snowflake.snowpark.modin.pandas
        if self.__package__ == "modin.pandas" and name == "session":
            import snowflake.snowpark.modin.pandas as spd

            return getattr(spd, name)
        return (
            self._get_active_session()
            if name == "session"
            else super().__getattribute__(name)
        )

    def __reduce__(self) -> tuple[Callable[[str], ModuleType], tuple[str]]:
        """
        Implement a custom pickle method so this class is pickleable.

        We need to pickle this class to use the Snowpark pandas module in
        stored procedures.

        Explanation of why we need this to pickle the class: https://github.com/cloudpipe/cloudpickle/issues/405#issuecomment-756085104
        """
        return _subimport, (self.__name__,)
