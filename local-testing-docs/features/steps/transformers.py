from behave import *
from project.transformers import add_rider_age, calc_bike_facts
from snowflake.snowpark.types import StructType, StructField, IntegerType

# Scenario: Add user age in years

@given("A dataframe with the riders' birth year")
def step_impl(context):
    table = [[element for element in row] for row in context.table.rows]
    schema = StructType([StructField("BIRTH_YEAR", IntegerType())])
    context.rider_df = context.session.create_dataframe(data=table, schema=schema)


@when('the rider age is calculated')
def step_impl(context):
    context.result = add_rider_age(context.rider_df).collect()


@then('the column, RIDER_AGE, is added to the DataFrame')
def step_impl(context):
    expected = [[int(element) for element in row] for row in context.table.rows]
    actual = [[int(element) for element in row] for row in context.result]
    assert expected == actual


# Scenario: Calculate monthly bike facts


@given('a table with bike ID, trip duration, and rider age')
def step_impl(context):
    table = [[element for element in row] for row in context.table.rows]
    schema = StructType([StructField("BIKEID", IntegerType()), StructField("TRIPDURATION", IntegerType()), StructField("RIDER_AGE", IntegerType())])
    context.bike_df = context.session.create_dataframe(data=table, schema=schema)


@when('the fact table is calculated')
def step_impl(context):
    context.bike_df_result = calc_bike_facts(context.bike_df).collect()


@then('the average trip duration, average rider age, and count are returned for each bike ID')
def step_impl(context):
    expected_df = [[element for element in row] for row in context.table.rows]
    actual_df = [[element for element in row] for row in context.bike_df_result]

    for expected_row, actual_row in zip(expected_df, actual_df):
        for expected_elem, actual_elem in zip(expected_row, actual_row):
            assert float(expected_elem) == float(actual_elem)
