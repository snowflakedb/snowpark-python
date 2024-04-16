#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# We need this import here to prevent circular dependency issues, since snowflake.snowpark.modin.pandas
# currently imports some internal utilities from snowflake.snowpark.modin.plugin. Test cases will
# import snowflake.snowpark.modin.plugin before snowflake.snowpark.modin.pandas, so in order to prevent
# circular dependencies from manifesting, apparently snowflake.snowpark.modin.pandas needs to
# be imported first.
from snowflake.snowpark.modin import pandas  # noqa: F401
from snowflake.snowpark.modin.config import DocModule
from snowflake.snowpark.modin.plugin import docstrings

DocModule.put(docstrings.__name__)
