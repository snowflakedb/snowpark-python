class DataFrameGenerator2:
    def __init__(self, session) -> None:
        self.session = session

    def simple_dataframe(self):
        return self.session.create_dataframe(
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
            schema=["id", "letter"],
        )
