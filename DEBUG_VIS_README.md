# Instructions


1. add extra dependencies. I think they are


    - networkx
    - pygraphviz
    - colorcet

1. run a script like

    ```python
    import modin.pandas as pd
    import snowflake.snowpark.modin.plugin
    import numpy as np
    import pandas as native_pd
    from snowflake.snowpark.session import Session; session = Session.builder.create()
    import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)

    pd.session.sql_simplifier_enabled = False


    (pd.DataFrame([1, 2]) + pd.DataFrame([3, 4])).rename({0: 'a'}, axis=1).max().debug_vis()
    ```

1. you can keep running python code after viewing the visualization.


1. when you want to do a new visualization, exit with control-c or `exit`, or close your terminal. don't exit with control-z as it sends SIGTSTP and i haven't figured out how to properly close the webserver when that happens.

1. if you accidentally control-z out and leave the process open, search of `lsof -i :3777` and kill the process on that port.

# Notes

1. to get prettier visualizations, set `pd.session.sql_simplifier_enabled = False` before generating your dataframe. The SQL simplifier often flattens recursive SQL queries.
1. some DataFrame methods don't have visualizations yet.

# Examples

![picture](debug_vis_screenshot.png)

![video](debug_vis_recording.mov)