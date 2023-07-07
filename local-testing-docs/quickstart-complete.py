from snowflake.snowpark.session import Session
from snowflake.snowpark.mock.mock_connection import MockServerConnection

session = Session(MockServerConnection())

df = session.create_dataframe(
        [
            [1, 2, "welcome"],
            [3, 4, "to"],
            [5, 6, "the"],
            [7, 8, "private"],
            [9, 0, "preview"]
        ],
        schema=['a', 'b', 'c']
    )

df.select('c').show()
