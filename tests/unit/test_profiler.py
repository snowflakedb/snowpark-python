#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.profiler import Profiler


def test_sp_call_match():
    pro = Profiler()
    sp_call_sql = """WITH myProcedure AS PROCEDURE ()
  RETURNS TABLE ( )
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ( 'snowflake-snowpark-python==1.2.0', 'pandas==1.3.3' )
  IMPORTS = ( '@my_stage/file1.py', '@my_stage/file2.py' )
  HANDLER = 'my_function'
  RETURNS NULL ON NULL INPUT
AS 'fake'
CALL myProcedure()INTO :result
    """
    assert pro._is_sp_call(sp_call_sql)
