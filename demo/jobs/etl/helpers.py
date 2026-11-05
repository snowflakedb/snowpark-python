from snowflake.snowpark import functions as F


def clean_customers(df):
    """Trims names, lower-cases emails, and drops incomplete and duplicate customers."""
    return (
        df.dropna(subset=["NAME", "EMAIL"])
        .with_column("NAME", F.initcap(F.trim("NAME")))
        .with_column("EMAIL", F.lower(F.trim("EMAIL")))
        .drop_duplicates("EMAIL")
    )
