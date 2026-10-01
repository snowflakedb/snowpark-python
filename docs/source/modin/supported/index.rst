======================================
Snowpark pandas Supported API
======================================

For your convenience, we have compiled a list of currently implemented APIs and methods available in Snowpark pandas.
This documentation is updated as new methods and APIs are merged into the release branch, and not necessarily correct as
of the most recent release.

To view the docs for the most recent release, check that you’re viewing the stable version of the docs.

Read each table's legend and the missing-parameter and notes columns, not just
the method name. An entry can describe a partial implementation or an
unsupported operation. These tables describe Snowpark pandas support; they are
not a promise that every upstream pandas or Modin API is available with identical
behavior. Also consult :doc:`../hybrid_execution` when interpreting where an
operation runs.

.. toctree::
   :maxdepth: 2

   general_supported
   series_supported
   dataframe_supported
   index_supported
   datetime_index_supported
   timedelta_index_supported
   window_supported
   groupby_supported
   resampling_supported
   series_dt_supported
   series_str_supported
