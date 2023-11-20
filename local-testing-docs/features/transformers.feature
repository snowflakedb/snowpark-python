Feature: DataFrame transformers

    Scenario: Add user age in years
        Given A dataframe with the riders' birth year
            | BIRTH_YEAR |
            | 1980       |
            | 1995       |
            | 2000       |
        When the rider age is calculated
        Then the column, RIDER_AGE, is added to the DataFrame
            | BIRTH_YEAR | RIDER_AGE |
            | 1980       | 43        |
            | 1995       | 28        |
            | 2000       | 23        |

    Scenario: Calculate bike facts
        Given a table with bike ID, trip duration, and rider age
            | BIKEID | TRIPDURATION | RIDER_AGE |
            | 1       | 10           | 20        |
            | 1       | 5            | 30        |
            | 2       | 20           | 50        |
            | 2       | 10           | 60        |
        When the fact table is calculated
        Then the average trip duration, average rider age, and count are returned for each bike ID
            | BIKEID | COUNT | AVG_TRIPDURATION | AVG_RIDER_AGE | COUNT |
            | 1      | 2     | 7.5              | 25            | 2     |
            | 2      | 2     | 15               | 55            | 2     |
