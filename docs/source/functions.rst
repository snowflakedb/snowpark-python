=============
Functions
=============

Provides utility and SQL functions that generate :class:`~snowflake.snowpark.Column` expressions that you can pass to :class:`~snowflake.snowpark.DataFrame` transformation methods.

These utility functions generate references to columns, literals, and SQL expressions (e.g. "c + 1").

  - Use :func:`col()` to convert a column name to a :class:`Column` object. Refer to the API docs of :class:`Column` to know more ways of referencing a column.
  - Use :func:`lit()` to convert a Python value to a :class:`Column` object that represents a constant value in Snowflake SQL.
  - Use :func:`sql_expr()` to convert a Snowflake SQL expression to a :class:`Column`.

    >>> df = session.create_dataframe([[1, 'a', True, '2022-03-16'], [3, 'b', False, '2023-04-17']], schema=["a", "b", "c", "d"])
    >>> res1 = df.filter(col("a") == 1).collect()
    >>> res2 = df.filter(lit(1) == col("a")).collect()
    >>> res3 = df.filter(sql_expr("a = 1")).collect()
    >>> assert res1 == res2 == res3
    >>> res1
    [Row(A=1, B='a', C=True, D='2022-03-16')]

Some :class:`DataFrame` methods accept column names or SQL expressions text aside from a Column object for convenience.
For instance:

    >>> df.filter("a = 1").collect()  # use the SQL expression directly in filter
    [Row(A=1, B='a', C=True, D='2022-03-16')]
    >>> df.select("a").collect()
    [Row(A=1), Row(A=3)]

whereas :class:`Column` objects enable you to use chained column operators and transformations with
Python code fluently:

    >>> # Use columns and literals in expressions.
    >>> df.select(((col("a") + 1).cast("string")).alias("add_one")).show()
    -------------
    |"ADD_ONE"  |
    -------------
    |2          |
    |4          |
    -------------
    <BLANKLINE>

The Snowflake database has hundreds of `SQL functions <https://docs.snowflake.com/en/sql-reference-functions.html>`_
This module provides Python functions that correspond to the Snowflake SQL functions. They typically accept :class:`Column`
objects or column names as input parameters and return a new :class:`Column` objects.
The following examples demonstrate the use of some of these functions:

    >>> # This example calls the function that corresponds to the TO_DATE() SQL function.
    >>> df.select(dateadd('day', lit(1), to_date(col("d")))).show()
    ---------------------------------------
    |"DATEADD('DAY', 1, TO_DATE(""D""))"  |
    ---------------------------------------
    |2022-03-17                           |
    |2023-04-18                           |
    ---------------------------------------
    <BLANKLINE>

If you want to use a SQL function in Snowflake but can't find the corresponding Python function here,
you can create your own Python function with :func:`function`:

    >>> my_radians = function("radians")  # "radians" is the SQL function name.
    >>> df.select(my_radians(col("a")).alias("my_radians")).show()
    ------------------------
    |"MY_RADIANS"          |
    ------------------------
    |0.017453292519943295  |
    |0.05235987755982988   |
    ------------------------
    <BLANKLINE>

or call the SQL function directly:

    >>> df.select(call_function("radians", col("a")).as_("call_function_radians")).show()
    ---------------------------
    |"CALL_FUNCTION_RADIANS"  |
    ---------------------------
    |0.017453292519943295     |
    |0.05235987755982988      |
    ---------------------------
    <BLANKLINE>

**How to find help on input parameters of the Python functions for SQL functions**
The Python functions have the same name as the corresponding `SQL functions <https://docs.snowflake.com/en/sql-reference-functions.html>`_.

By reading the API docs or the source code of a Python function defined in this module, you'll see the type hints of the input parameters and return type.
The return type is always ``Column``. The input types tell you the acceptable values:

  - ``ColumnOrName`` accepts a :class:`Column` object, or a column name in str. Most functions accept this type.
    If you still want to pass a literal to it, use `lit(value)`, which returns a ``Column`` object that represents a literal value.

    >>> df.select(avg("a")).show()
    ----------------
    |"AVG(""A"")"  |
    ----------------
    |2.000000      |
    ----------------
    <BLANKLINE>
    >>> df.select(avg(col("a"))).show()
    ----------------
    |"AVG(""A"")"  |
    ----------------
    |2.000000      |
    ----------------
    <BLANKLINE>

  - ``LiteralType`` accepts a value of type ``bool``, ``int``, ``float``, ``str``, ``bytearray``, ``decimal.Decimal``,
    ``datetime.date``, ``datetime.datetime``, ``datetime.time``, or ``bytes``. An example is the third parameter of :func:`lead`.

    >>> import datetime
    >>> from snowflake.snowpark.window import Window
    >>> df.select(col("d"), lead("d", 1, datetime.date(2024, 5, 18), False).over(Window.order_by("d")).alias("lead_day")).show()
    ---------------------------
    |"D"         |"LEAD_DAY"  |
    ---------------------------
    |2022-03-16  |2023-04-17  |
    |2023-04-17  |2024-05-18  |
    ---------------------------
    <BLANKLINE>

  - ``ColumnOrLiteral`` accepts a ``Column`` object, or a value of ``LiteralType`` mentioned above.
    The difference from ``ColumnOrLiteral`` is ``ColumnOrLiteral`` regards a str value as a SQL string value instead of
    a column name. When a function is much more likely to accept a SQL constant value than a column expression, ``ColumnOrLiteral``
    is used. Yet you can still pass in a ``Column`` object if you need to. An example is the second parameter of
    :func:``when``.

    >>> df.select(when(df["a"] > 2, "Greater than 2").else_("Less than 2").alias("compare_with_2")).show()
    --------------------
    |"COMPARE_WITH_2"  |
    --------------------
    |Less than 2       |
    |Greater than 2    |
    --------------------
    <BLANKLINE>

  - ``int``, ``bool``, ``str``, or another specific type accepts a value of that type. An example is :func:`to_decimal`.

    >>> df.with_column("e", lit("1.2")).select(to_decimal("e", 5, 2)).show()
    -----------------------------
    |"TO_DECIMAL(""E"", 5, 2)"  |
    -----------------------------
    |1.20                       |
    |1.20                       |
    -----------------------------
    <BLANKLINE>

  - ``ColumnOrSqlExpr`` accepts a ``Column`` object, or a SQL expression. For instance, the first parameter in :func:``when``.

    >>> df.select(when("a > 2", "Greater than 2").else_("Less than 2").alias("compare_with_2")).show()
    --------------------
    |"COMPARE_WITH_2"  |
    --------------------
    |Less than 2       |
    |Greater than 2    |
    --------------------
    <BLANKLINE>

.. currentmodule:: snowflake.snowpark.functions


.. autosummary::
    :toctree: api/

        abs
        acos
        add_months
        any_value
        approx_count_distinct
        approx_percentile
        approx_percentile_accumulate
        approx_percentile_combine
        approx_percentile_estimate
        array_agg
        array_append
        array_cat
        array_compact
        array_construct
        array_construct_compact
        array_contains
        array_insert
        array_intersection
        array_position
        array_prepend
        array_size
        array_slice
        array_to_string
        arrays_overlap
        as_array
        as_binary
        as_char
        as_date
        as_decimal
        as_double
        as_integer
        as_number
        as_object
        as_real
        as_time
        as_timestamp_ltz
        as_timestamp_ntz
        as_timestamp_tz
        as_varchar
        ascii
        asin
        atan
        atan2
        avg
        bitnot
        bitshiftleft
        bitshiftright
        builtin
        call_builtin
        call_function
        call_table_function
        call_udf
        cast
        ceil
        char
        charindex
        check_json
        check_xml
        coalesce
        col
        collate
        collation
        column
        concat
        concat_ws
        contains
        convert_timezone
        corr
        cos
        cosh
        count
        countDistinct
        count_distinct
        covar_pop
        covar_samp
        cume_dist
        current_available_roles
        current_database
        current_date
        current_region
        current_role
        current_schema
        current_schemas
        current_session
        current_statement
        current_time
        current_timestamp
        current_user
        current_version
        current_warehouse
        date_from_parts
        date_trunc
        dateadd
        datediff
        dayname
        dayofmonth
        dayofweek
        dayofyear
        degrees
        dense_rank
        div0
        endswith
        equal_nan
        exp
        factorial
        first_value
        floor
        function
        get
        get_ignore_case
        get_path
        greatest
        grouping
        grouping_id
        hash
        hour
        iff
        in_
        initcap
        insert
        is_array
        is_binary
        is_boolean
        is_char
        is_date
        is_date_value
        is_decimal
        is_double
        is_integer
        is_null
        is_null_value
        is_object
        is_real
        is_time
        is_timestamp_ltz
        is_timestamp_ntz
        is_timestamp_tz
        is_varchar
        json_extract_path_text
        kurtosis
        lag
        last_day
        last_value
        lead
        least
        left
        length
        listagg
        lit
        log
        lower
        lpad
        ltrim
        max
        md5
        mean
        median
        min
        minute
        mode
        month
        monthname
        months_between
        negate
        next_day
        not_
        ntile
        object_agg
        object_construct
        object_construct_keep_null
        object_delete
        object_insert
        object_keys
        object_pick
        pandas_udf
        parse_json
        parse_xml
        percent_rank
        percentile_cont
        pow
        previous_day
        quarter
        radians
        random
        rank
        regexp_count
        regexp_replace
        repeat
        replace
        right
        round
        row_number
        rpad
        rtrim
        second
        seq1
        seq2
        seq4
        seq8
        sha1
        sha2
        sin
        sinh
        skew
        soundex
        split
        sproc
        sql_expr
        sqrt
        startswith
        stddev
        stddev_pop
        stddev_samp
        strip_null_value
        strtok_to_array
        substr
        substring
        sum
        sum_distinct
        sysdate
        table_function
        tan
        tanh
        time_from_parts
        timestamp_from_parts
        timestamp_ltz_from_parts
        timestamp_ntz_from_parts
        timestamp_tz_from_parts
        to_array
        to_binary
        to_char
        to_date
        to_decimal
        to_geography
        to_json
        to_object
        to_time
        to_timestamp
        to_varchar
        to_variant
        to_xml
        translate
        trim
        trunc
        try_cast
        typeof
        udf
        udtf
        uniform
        upper
        var_pop
        var_samp
        variance
        weekofyear
        when
        when_matched
        when_not_matched
        xmlget
        year


