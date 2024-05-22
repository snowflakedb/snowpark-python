Performance Recommendations
===========================

This page contains recommendations to help improve performance when using the Snowpark pandas API.

Caching Intermediate Results
----------------------------
Snowpark pandas uses a lazy paradigm - when operations are called on a Snowpark pandas object,
a lazy operator graph is built up and executed only when an output operation is called (e.g. printing
the data, or persisting it to a table in Snowflake). This paradigm mirrors the Snowpark DataFrame paradigm,
and enables larger queries to be optimized using Snowflake's SQL Query Optimizer. Certain workloads; however,
can generate large operator graphs that include repeated, computationally expensive, subgraphs, that can introduce
recomputations that impact performance. Take the following code snippet as an example:

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
lazily recomputed every time the dataframe is materialized - once per print. Instead, we recommend using
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

Known Limitations
^^^^^^^^^^^^^^^^^
Using the ``cache_result`` API after an ``apply``, an ``applymap`` or a ``groupby.apply`` is unlikely to yield performance savings.
``apply(func, axis=1)`` when ``func`` has no return type annotation and ``groupby.apply`` are implemented internally via UDTFs, and feature
intermediate result caching as part of their implementation. ``apply(func, axis=1)`` when func has a return type annotation, and ``applymap``
internally use UDFs - any overhead observed when using these APIs is likely due to the set-up and definition of the UDF, and is unlikely to be
alleviated via the ``cache_result`` API.