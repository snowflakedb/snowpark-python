#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

html_string = """<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>col1</th>
      <th>col2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3</td>
    </tr>
  </tbody>
</table>"""


@sql_count_checker(query_count=1)
def test_to_html():
    df = pd.DataFrame(data={"col1": [1, 2], "col2": [4, 3]})
    assert html_string == df.to_html()
