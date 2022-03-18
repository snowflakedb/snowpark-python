import pandas as pd


def pandas_apply_mod5(df: pd.DataFrame) -> pd.Series:
    return df[0].apply(lambda x: x % 5)


pandas_apply_mod5._sf_vectorized_input = pd.DataFrame
