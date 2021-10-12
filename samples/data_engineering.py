#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import logging
import os

from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    call_builtin,
    col,
    max as max_,
    median,
    min as min_,
    udf,
)
from snowflake.snowpark.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.DEBUG)

# Create a session
CONNECTION_PARAMETERS = {
    "protocol": os.environ["SNOWFLAKE_PROTOCOL"],
    "host": os.environ["SNOWFLAKE_HOST"],
    "port": os.environ["SNOWFLAKE_PORT"],
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database": os.environ["SNOWFLAKE_DATABASE"],
    "schema": os.environ["SNOWFLAKE_SCHEMA"],
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()

try:
    # Upload a CSV file to Snowflake and read it into dataframe
    session.sql(
        f"put file://dataset/titanic.csv {session.getSessionStage()}/dataset auto_compress=false overwrite=true"
    ).collect()
    train_csv_schema = StructType(
        [
            StructField("PassengerId", IntegerType()),
            StructField("Survived", IntegerType()),
            StructField("Pclass", IntegerType()),
            StructField("Name", StringType()),
            StructField("Sex", StringType()),
            StructField("Age", IntegerType()),
            StructField("SibSp", IntegerType()),
            StructField("Parch", IntegerType()),
            StructField("Ticket", StringType()),
            StructField("Fare", FloatType()),
            StructField("Cabin", StringType()),
            StructField("Embarked", StringType()),
        ]
    )
    read_options = {"SKIP_HEADER": 1, "FIELD_OPTIONALLY_ENCLOSED_BY": '"'}
    train_file_on_stage = f"{session.getSessionStage()}/dataset/titanic.csv"
    df_train = (
        session.read.options(read_options)
        .schema(train_csv_schema)
        .csv(train_file_on_stage)
    )

    # Data cleaning
    train_age_median = df_train.select(median("age")).collect()[0][0]
    train_embarked_mode = df_train.select(
        call_builtin("mode", col("embarked"))
    ).collect()[0][0]

    columns_to_drop = ["passengerid", "name", "ticket", "cabin"]

    # we will have a public API for filling null/nan value soon
    df_train_cleaned = (
        df_train.withColumn(
            "age_cleaned",
            call_builtin("iff", col("age").is_null(), train_age_median, col("age")),
        )
        .withColumn(
            "embarked_cleaned",
            call_builtin(
                "iff", col("embarked").is_null(), train_embarked_mode, col("embarked")
            ),
        )
        .drop("age", "embarked", *columns_to_drop)
    )

    # Feature engineering
    train_age_min = df_train_cleaned.select(min_("age_cleaned")).collect()[0][0]
    train_age_max = df_train_cleaned.select(max_("age_cleaned")).collect()[0][0]
    train_fare_min = df_train_cleaned.select(min_("fare")).collect()[0][0]
    train_fare_max = df_train_cleaned.select(max_("fare")).collect()[0][0]
    df_train_augmented = df_train_cleaned.withColumn(
        "family_size", df_train_cleaned["sibsp"] + df_train_cleaned["parch"] + 1
    )
    df_train_augmented = (
        df_train_augmented.withColumn(
            "is_alone", (df_train_augmented["family_size"] > 1).cast(IntegerType())
        )
        .withColumn(
            "age_bin",
            call_builtin(
                "width_bucket", col("age_cleaned"), train_age_min, train_age_max, 5
            ),
        )
        .withColumn(
            "fare_bin",
            call_builtin(
                "width_bucket", col("fare"), train_fare_min, train_fare_max, 4
            ),
        )
        .drop("sibsp", "parch", "fare", "age_cleaned")
    )

    # Encode lable with a UDF
    @udf
    def encode_embarked(e: str) -> int:
        if e == "S":
            return 0
        elif e == "C":
            return 1
        else:
            return 2

    df_train_augmented = df_train_augmented.withColumn(
        "embarked_encoded", encode_embarked("EMBARKED_CLEANED")
    )
    df_train_augmented = (
        df_train_augmented.withColumn(
            "sex_encoded", call_builtin("iff", col("sex") == "male", 0, 1)
        )
        .withColumn("age_encoded", col("age_bin") - 1)
        .withColumn("fare_encoded", col("fare_bin") - 1)
    )
    df_train_augmented = df_train_augmented.select(
        "survived",
        "pclass",
        "family_size",
        "is_alone",
        "embarked_encoded",
        "sex_encoded",
        "age_encoded",
        "fare_encoded",
    )

    # Save into a different table in Snowflake
    df_train_augmented.write.mode("overwrite").saveAsTable("titanic")
    try:
        # Convert to pandas
        pandas_df = session.table("titanic").toPandas()
        print(pandas_df.describe())

        # Do more with the pandas_df

    finally:
        # Drop the table
        session.sql("drop table if exists titanic").collect()
finally:
    session.close()
