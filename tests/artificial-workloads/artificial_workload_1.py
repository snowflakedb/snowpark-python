#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os

from pyspark.sql.connect.session import SparkSession
import pyspark.sql.connect.functions as fn
from pyspark.sql.types import *
from pyspark.sql.connect.functions import *
from pyspark.sql.connect.window import Window

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# To move to an expectation test later.
current_dir = os.path.dirname(os.path.abspath(__file__))

# Read CSV files with proper schema inference and handling
cities_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{current_dir}/resources/artificial_cities_data.csv")
)

distributors_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{current_dir}/resources/artificial_distributors_data.csv")
)

orders_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{current_dir}/resources/artificial_distributor_order_statistics.csv")
)

# Data Cleaning and Preparation
# Clean cities data - split zip codes into array and explode
cities_clean = (
    cities_df.withColumn("zip_codes", split(col("zip codes"), ","))
    .drop("zip codes")
    .withColumn("zip_code", explode(col("zip_codes")))
    .withColumn("zip_code", trim(col("zip_code")))
    .drop("zip_codes")
)

# Clean distributors data - handle email arrays and formatting
distributors_clean = (
    distributors_df.withColumn("emails", split(col("emails"), ";"))
    .withColumn("zip_code", trim(col("zip code")))
    .drop("zip code", "registration date")
)

# TODO(SNOW-1913552): Fails due to date format issue. Works in Spark.
#    .withColumn("registration_date", to_date(col("registration date"), "yyyy-MM-dd"))\

# Clean orders data - handle dates and boolean values
orders_clean = orders_df.withColumn("amount", col("amount").cast("double")).withColumn(
    "completed", col("completed").cast("boolean")
)

#    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))\

# Scenario 1: Distributor with largest order volume
# TODO(SNOW-1913558): Fails due to a join condition, probably around column renaming. Works in Spark.
# largest_distributor = orders_clean\
#    .groupBy("distributor id")\
#    .agg(sum("amount").alias("total_amount"))\
#    .join(distributors_clean, orders_clean["distributor id"] == distributors_clean["distributor id"])\
#    .orderBy(col("total_amount").desc())

# print("Top distributors by order volume:")
# largest_distributor.select("name", "total_amount").show(5)

# Scenario 2: Email mismatch in orders
# First, extract email from contact person in orders
orders_with_email = orders_clean.withColumn(
    "order_email", regexp_extract(col("contact person"), r"[\w\.-]+@[\w\.-]+", 0)
)

# Find mismatches
email_mismatches = (
    orders_with_email.join(distributors_clean, "distributor id")
    .where(~array_contains(col("emails"), col("order_email")))
    .select("order id", "distributor id", "order_email", "emails")
)

print("Orders with email mismatches:")
email_mismatches.show()

# Scenario 3: LA orders with non-primary distribution channel
la_zips = cities_clean.where(lower(col("name")) == "los angeles").select("zip_code")

non_primary_channel_orders = (
    orders_clean.join(distributors_clean, "distributor id")
    .join(la_zips, distributors_clean["zip_code"] == la_zips["zip_code"])
    .where(col("distribution channel") != col("primary distribution channel"))
    .select("order id", "distribution channel", "primary distribution channel")
)

print("LA orders with non-primary distribution channel:")
non_primary_channel_orders.show()

# Additional Scenario 4: Distribution Channel Performance Analysis
channel_performance = (
    orders_clean.where(col("completed") == True)
    .groupBy("distribution channel")
    .agg(
        count("order id").alias("total_orders"),
        avg("amount").alias("avg_order_amount"),
        sum("amount").alias("total_revenue"),
    )
    .orderBy(col("total_revenue").desc())
)

print("Distribution Channel Performance:")
channel_performance.show()

# Additional Scenario 5: Monthly Growth Analysis
monthly_growth = (
    orders_clean.withColumn("year_month", date_format(col("date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(sum("amount").alias("monthly_revenue"))
    .orderBy("year_month")
    .withColumn(
        "previous_month_revenue",
        lag("monthly_revenue", 1).over(Window.orderBy("year_month")),
    )
    .withColumn(
        "growth_rate",
        (
            (col("monthly_revenue") - col("previous_month_revenue"))
            / col("previous_month_revenue")
        )
        * 100,
    )
)

print("Monthly Revenue Growth:")
monthly_growth.show()

# Additional Scenario 6: Distributor Efficiency Analysis
distributor_efficiency = (
    orders_clean.groupBy("distributor id")
    .agg(
        count("order id").alias("total_orders"),
        sum(when(col("completed") == True, 1).otherwise(0)).alias("completed_orders"),
        avg("amount").alias("avg_order_value"),
    )
    .withColumn(
        "completion_rate", (col("completed_orders") / col("total_orders")) * 100
    )
    .join(distributors_clean, "distributor id")
    .orderBy(col("completion_rate").desc())
)

print("Distributor Efficiency Metrics:")
distributor_efficiency.select(
    "name", "total_orders", "completion_rate", "avg_order_value"
).show()
