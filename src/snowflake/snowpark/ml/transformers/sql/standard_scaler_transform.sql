create database if not exists feature_engineering;
create schema if not exists feature_engineering.standard_scaler;
use database feature_engineering;
use schema feature_engineering.standard_scaler;
use schema feature_engineering.standard_scaler_clone;
create stage if not exists stage_test;

create or replace function feature_engineering.standard_scaler.transform(value float)
returns float
language python
runtime_version = 3.8
packages = ('pandas')
imports=('@stage_test/temp_standard_scaler.csv')
handler='transform'
as
$$
import sys
import pandas as pd
import csv
from _snowflake import vectorized

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
FILENAME = "temp_standard_scaler.csv"

@vectorized(input=pd.DataFrame)
def transform(df):
    with open(import_dir + FILENAME, 'r') as file:
        reader = csv.reader(file)
        row = next(reader)
        mean, stddev = float(row[0]), float(row[1])

    return df[0].sub(mean).div(stddev)
$$;

show functions like '%transform%' in standard_scaler;

-- clone transform() to the transformer_clone schema
create or replace schema standard_scaler_clone clone feature_engineering.standard_scaler;

show procedures like '%fit%' in standard_scaler_clone;
show functions like '%transform%' in standard_scaler_clone;
desc function transform(varchar);
list @stage_test;


// ======================
//         TEST
// ======================

select transform(0.5);

create or replace table table_test_transform as select uniform(0::float, 1::float, random(2)) as float from table(generator(rowcount => 1000000));
select float, transform(float) from table_test_transform;
