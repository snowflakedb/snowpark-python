class DataFrameGenerator1:
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

    def dataframe_with_loop(self):
        df = self.session.create_dataframe(
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
            schema=["id", "letter"],
        )
        for i in range(5):
            df = df.with_column("new_col", df["id"] + i)
        return df

    def dataframe_with_nested_calls(self):
        df = self.session.create_dataframe(
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
            schema=["id", "letter"],
        )
        df = df.with_column("new_col", df["id"] + 1)
        df = df.with_column("new_col2", df["new_col"] * 2)
        return df

    def dataframe_with_union(self):
        df1 = (
            self.session.create_dataframe(
                [
                    (1, "a"),
                    (2, "b"),
                ],
                schema=["id", "letter"],
            )
            .filter("id > 0")
            .sort("letter")
        )

        df2 = self.session.create_dataframe(
            [
                (3, "c"),
                (4, "d"),
            ],
            schema=["id", "letter"],
        ).distinct()

        return df1.union(df2)

    def dataframe_with_embedded_error(self):
        df = self.session.create_dataframe(
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
            schema=["id", "letter"],
        )
        # This will raise an error
        df = df.select("id", "non_existent_column")
        df = df.with_column("new_col", df["id"] + 1)
        return df

    def dataframe_with_join_on_bad_col(self):
        df1 = self.session.create_dataframe(
            [
                (1, "a"),
                (2, "b"),
            ],
            schema=["id", "letter"],
        )
        df2 = self.session.create_dataframe(
            [
                (3, "c"),
                (4, "d"),
            ],
            schema=["id", "letter"],
        )
        # This will raise an error
        return df1.join(df2.select("non_existent_column"), on="id", how="inner")

    def dataframe_with_long_operation_chain(self):
        df = self.session.create_dataframe(
            [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ],
            schema=["id", "letter"],
        )
        df = df.with_column("new_col", df["id"] + 1)
        df = df.with_column("new_col2", df["new_col"] * 2)
        df = df.with_column("new_col3", df["new_col2"] + 3)
        df = df.with_column("new_col4", df["new_col3"] * 4)
        df = df.with_column("new_col5", df["new_col4"] + 5)
        df = df.with_column("new_col6", df["new_col5"] * 6)
        df = df.with_column("new_col7", df["new_col6"] + 7)
        df = df.with_column("new_col8", df["new_col7"] * 8)
        df = df.sort("new_col6")
        df = df.filter("new_col6 > 10")
        return df
