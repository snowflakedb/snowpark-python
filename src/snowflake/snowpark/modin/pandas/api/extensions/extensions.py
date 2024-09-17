#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""
File containing decorators that allow registering extension APIs on Modin's API layer objects.
In Modin OSS, this file is placed under modin/pandas/api/extensions/extensions.py. However,
since our repository does not use Modin as an external dependency, following this path construction
would cause circular imports.

plugin/{pd,dataframe,series}_extensions.py must import this module (modin/pandas/extensions.py)
in order to use the decorators defined here.

Note that telemetry and other decorators must always be placed below the extension decorator:
the extension decorator calls setattr on the relevant object/module, and therefore must be called
last for other decorators to be applied. Furthermore, all DataFrame/Series methods declared in
extensions must have a telemetry decorator, unlike those defined directly on the class, which
have telemetry automatically added by the TelemetryMeta metaclass.
"""

from types import ModuleType
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    import snowflake.snowpark.modin.pandas as pd


def _set_attribute_on_obj(
    name: str,
    extensions_dict: dict,
    obj: Union["pd.DataFrame", "pd.Series", ModuleType],
):
    """
    Create a new or override existing attribute on obj.

    Parameters
    ----------
    name : str
        The name of the attribute to assign to `obj`.
    extensions_dict : dict
        The dictionary mapping extension name to `new_attr` (assigned below).
    obj : DataFrame, Series, or modin.pandas
        The object we are assigning the new attribute to.

    Returns
    -------
    decorator
        Returns the decorator function.
    """

    def decorator(new_attr: Any):
        """
        The decorator for a function or class to be assigned to name

        Parameters
        ----------
        new_attr : Any
            The new attribute to assign to name.

        Returns
        -------
        new_attr
            Unmodified new_attr is return from the decorator.
        """
        extensions_dict[name] = new_attr
        setattr(obj, name, new_attr)
        return new_attr

    return decorator


def register_pd_accessor(name: str):
    """
    Registers a pd namespace attribute with the name provided.

    This is a decorator that assigns a new attribute to modin.pandas. It can be used
    with the following syntax:

    ```
    @register_pd_accessor("new_function")
    def my_new_pd_function(*args, **kwargs):
        # logic goes here
        return
    ```

    The new attribute can then be accessed with the name provided:

    ```
    import modin.pandas as pd

    pd.new_method(*my_args, **my_kwargs)
    ```


    Parameters
    ----------
    name : str
        The name of the attribute to assign to modin.pandas.

    Returns
    -------
    decorator
        Returns the decorator function.
    """
    import snowflake.snowpark.modin.pandas as pd

    return _set_attribute_on_obj(name, pd._PD_EXTENSIONS_, pd)
