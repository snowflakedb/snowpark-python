===========
Session
===========

``modin.pandas.session`` is the Snowpark session that new
Snowpark pandas DataFrames and Series will use to execute queries.

* ``session`` starts as ``None``.

* When there is no active Snowpark session and ``session`` is ``None``, accessing
  ``session`` or creating a Snowpark pandas Dataframe or Series will raise an
  exception. You will need to create a Snowpark session to acccess ``session`` or
  create a DataFrame or Series.

* When there a single active Snowpark session and ``session`` is ``None``,
  Snowpark pandas automatically assigns that session to ``session``.

* When there are multiple active Snowpark sessions and ``session`` is ``None``,
  accessing ``session`` or creating a Snowpark pandas Dataframe or Series will
  raise an exception. To make Snowpark pandas populate
  ``modin.pandas.session``, you can
  `close <https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/latest/api/snowflake.snowpark.Session.close#snowflake.snowpark.Session.close>`_
  one of the sessions, or assign a particular session to ``session``. For
  example, if you execute ``modin.pandas.session = session1``,
  Snowpark pandas will use ``session1``.

Examples
========

Creating and using the default Snowpark session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you have set a `default Snowflake connection <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection>`_,
you can use use that connection to create a Snowpark session for Snowpark pandas:

.. code-block:: python

    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    from snowflake.snowpark import Session

    # Session.builder.create() will create a default Snowflake connection.
    Session.builder.create()
    df = pd.DataFrame([1, 2, 3])

Note that Snowpark pandas uses the unique active Snowpark session, even though
the code does not explicitly assign that session to Snowpark pandas.

Assigning one of multiple sessions to Snowpark pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can create multiple Snowpark sessions, then assign one of them to Snowpark
pandas.

.. code-block:: python

    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    from snowflake.snowpark import Session

    pandas_session = Session.builder.configs({"user": "<user>", "password": "<password>", "account": "<account1>").create()
    other_session = Session.builder.configs({"user": "<user>", "password": "<password>", "account": "<account2>").create()
    pd.session = pandas_session
    df = pd.DataFrame([1, 2, 3])

Trying to use Snowpark pandas when there is no active Snowpark session
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The code below will cause a :doc:`SnowparkSessionException <../snowpark/api/snowflake.snowpark.exceptions.SnowparkSessionException>`
with a message like ``Snowpark pandas requires an active snowpark session, but there is none.``
Once you create a session, you can use Snowpark pandas.

.. code-block:: python

    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin

    df = pd.DataFrame([1, 2, 3])

Trying to use Snowpark pandas when there are multiple active Snowpark sessions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The code below will cause a :doc:`SnowparkSessionException <../snowpark/api/snowflake.snowpark.exceptions.SnowparkSessionException>`
with a message like ``There are multiple active snowpark sessions, but you need
to choose one for Snowpark pandas.``

.. code-block:: python

    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    from snowflake.snowpark import Session

    pandas_session = Session.builder.configs({"user": "<user>", "password": "<password>", "account": "<account1>"}).create()
    other_session = Session.builder.configs({"user": "<user>", "password": "<password>", "account": "<account2>"}).create()
    df = pd.DataFrame([1, 2, 3])
