from snowflake.snowpark.mock.connection import MockServerConnection
from snowflake.snowpark.session import Session

session = Session(MockServerConnection())

df = session.create_dataframe(
    [
        [1, 2, "welcome"],
        [3, 4, "to"],
        [5, 6, "the"],
        [7, 8, "private"],
        [9, 0, "preview"],
    ],
    schema=["a", "b", "c"],
)

df.select("c").show()
