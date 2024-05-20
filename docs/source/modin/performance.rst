Performance Recommendations
===========================

This page contains recommendations to help improve performance when using Snowpark pandas.

Caching Intermediate Results
----------------------------
Snowpark pandas uses a lazy paradigm - when operations are called on a Snowpark pandas object,
the operation is not evaluated until absolutely necessary - e.g. when displaying results to users.
This paradigm enables larger queries to be optimized using Snowflake's SQL Query Optimizer, but in certain
cases, can lead to expensive recomputations. Take, for example, the following toy code snippet:

.. code-block:: python

    import modin.pandas as pd
    import numpy as np
    import snowflake.snowpark.modin.plugin
    from snowflake.snowpark import Session

    # Session.builder.create() will create a default Snowflake connection.
    Session.builder.create()
    df = pd.concat([pd.DataFrame([range(i, i+5)]) for i in range(0, 150, 5)])
    print(df)
    df = df.reset_index(drop=True)
    print(df)

The above code snippet creates a 30x5 DataFrame using concatenation of 30 smaller 1x5 DataFrames,
prints it, resets its index, and prints it again. The concatenation step can be expensive, and is
recomputed every time the dataframe is materialized - once per print. Instead, we recommend using
Snowpark pandas' ``cache_result`` API in order to materialize expensive computations that are reused
in order to decrease the latency of longer pipelines.

.. code-block:: python

    import modin.pandas as pd
    import numpy as np
    import snowflake.snowpark.modin.plugin
    from snowflake.snowpark import Session

    # Session.builder.create() will create a default Snowflake connection.
    Session.builder.create()
    df = pd.concat([pd.DataFrame([range(i, i+5)]) for i in range(0, 150, 5)])
    df = df.cache_result(inplace=False)
    print(df)
    df = df.reset_index(drop=True)
    print(df)

Consider using the ``cache_result`` API whenever a DataFrame or Series that is expensive to compute sees high reuse.
