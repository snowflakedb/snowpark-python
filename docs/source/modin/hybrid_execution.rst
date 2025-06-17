===========================================
Hybrid Execution (Public Preview)
===========================================

Snowpark pandas supports workloads on mixed underlying execution engines and will automatically
move data to the most appropriate engine for a given dataset size and operation. Currently you
can use either Snowflake or local pandas to back a DataFrame object. Decisions on when to move
data are dominated by dataset size.

For Snowflake, specific API calls will trigger hybrid backend evaluation. These are registered 
as either a pre-operation switch point or a post-operation switch point. These switch points
may change over time.

Example Pre-Operation Switchpoints:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
apply, iterrows, itertuples, items, plot, quantile, __init__, plot, quantile, T, read_csv, read_json, concat, merge 

Post-Operation Switchpoints:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
read_snowflake, value_counts, tail, var, std, sum, sem, max, min, mean, agg, aggregate, count, nunique, cummax, cummin, cumprod, cumsum


Examples
========

Enabling Hybrid Execution
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin

    # Import the configuration variable
    from modin.config import AutoSwitchBackend
    from snowflake.snowpark import Session
    
    # Enable hybrid execution
    Session.builder.create()
    df = pd.DataFrame([1, 2, 3])
    print(df.get_backend()) # 'Snowflake'

    AutoSwitchBackend().enable()
    df = pd.DataFrame([4, 5, 6])
    # DataFrame should use local execution backend, 'Pandas'
    print(df.get_backend()) # 'Pandas'

    # Disable hybrid execution ( All DataFrames say on existing engine )
    AutoSwitchBackend().disable()

Manually Changing DataFrame Backends
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Move a DataFrame to the local machine
    df_local = df.move_to('Pandas')
    # Move a DataFrame to be backed by Snowflake
    df_snow = df_local.move_to('Snowflake')
    # Move a DataFrame to the local machine, without changing the 'df' reference
    df.move_to('Pandas', inplace=True)
    # "pin" the current backend, preventing data movement
    df.pin_backend(inplace=True)
    # "unpin" the current backend, preventing data movement
    df.unpin_backend(inplace=True)

Configuring Local Pandas Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently the auto switching behavior is dominated by dataset size, with some exceptions
for specific operations. The default limit for running workloads on the local pandas 
backend is 10M rows. This can be configured through the modin environment variables:

.. code-block:: python

    # Change row threshold to 500k
    from modin.config.envvars import NativePandasMaxRows
    NativePandasMaxRows.put(500_000)