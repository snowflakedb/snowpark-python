#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Optional dependency management for snowpark-python.

Mirrors the ``snowflake.connector.options`` pattern for connector-side optional
deps (pandas, pyarrow). This module owns optional deps that snowpark-python
needs to gate independently of what the connector exposes:

* ``installed_polars`` — polars is not part of the connector dep tree at all.
* ``installed_pyarrow`` — the connector exposes ``installed_pandas`` but **not**
  ``installed_pyarrow``. Historically pyarrow has only reached snowpark users
  via the connector's ``[pandas]`` extra, so ``installed_pandas`` doubled as a
  pyarrow guard. With the new ``[polars]`` extra (which pulls pyarrow without
  pandas), that conflation is wrong: a polars-only install needs pyarrow checks
  that don't depend on pandas being present.

Usage::

    from snowflake.snowpark._internal.options import installed_polars, polars

    if installed_polars:
        df = polars.DataFrame(...)
"""

from snowflake.connector.options import MissingOptionalDependency


class MissingPolars(MissingOptionalDependency):
    """Placeholder used when ``polars`` is not installed.

    Attribute access on this object raises a clear ``ImportError`` pointing
    the user at the ``[polars]`` extra.
    """

    _dep_name = "polars"


try:
    import polars  # type: ignore[import-not-found]

    installed_polars = True
except ImportError:
    polars = MissingPolars()  # type: ignore[assignment]
    installed_polars = False


try:
    import pyarrow as _pyarrow  # noqa: F401

    installed_pyarrow = True
except ImportError:
    installed_pyarrow = False
