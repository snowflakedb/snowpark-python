#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from snowflake.connector.version import VERSION as connector_version

IS_V5_DRIVER: bool = connector_version[0] >= 5

if IS_V5_DRIVER:
    from snowflake.connector._common.extras import pandas  # noqa: F401
    from snowflake.connector._common.extras import ModuleLikeObject  # noqa: F401
    from snowflake.connector._common.extras import installed_pandas  # noqa: F401
    from snowflake.connector._common.extras import (
        MissingOptionalDependency,
        pyarrow,
        installed_pyarrow,
    )
else:
    from snowflake.connector.options import pandas  # noqa: F401
    from snowflake.connector.options import ModuleLikeObject  # noqa: F401
    from snowflake.connector.options import installed_pandas  # noqa: F401
    from snowflake.connector.options import (
        MissingOptionalDependency,
        MissingPandas,
        pyarrow,
    )

    # connector.options (v4) never exported installed_pyarrow as its own name.
    installed_pyarrow: bool = not isinstance(pyarrow, MissingOptionalDependency)


def _missing_pandas() -> MissingOptionalDependency:
    # v4 has no __init__ override (no-arg-subclass only); v5 deleted
    # MissingPandas in favor of the positional-arg form.
    if IS_V5_DRIVER:
        return MissingOptionalDependency("pandas")
    return MissingPandas()
