from src.snowflake.snowpark.relational_grouped_dataframe import GroupByType, CubeType, RollupType, PivotType


def test_to_string_of_relational_grouped_dataframe():
    assert GroupByType().to_string() == "GroupBy"
    assert CubeType().to_string() == "Cube"
    assert RollupType().to_string() == "Rollup"
    assert PivotType(None, []).to_string() == "Pivot"