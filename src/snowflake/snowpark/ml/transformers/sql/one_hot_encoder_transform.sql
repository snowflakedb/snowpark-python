create database if not exists feature_engineering;
create schema if not exists feature_engineering.one_hot_encoder;
use database feature_engineering;
use schema feature_engineering.one_hot_encoder;
use schema feature_engineering.one_hot_encoder_clone;
create stage if not exists stage_test;

create or replace function feature_engineering.one_hot_encoder.transform(value string)
returns array
language python
runtime_version = 3.8
packages = ('pandas')
imports=('@stage_test/temp_one_hot_encoder.csv')
handler='transform'
as
$$
import sys
import pandas as pd
import csv
from _snowflake import vectorized

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
FILENAME = "temp_one_hot_encoder.csv"

@vectorized(input=pd.DataFrame)
def transform(df):
    with open(import_dir + FILENAME, 'r') as file:
        reader = csv.reader(file)
        value_to_index = {row[0]: int(row[1]) for row in reader}

    count = 1
    return df[0].map(lambda x: [value_to_index.get(x, -1), len(value_to_index), count])
$$;

show functions like '%transform%' in one_hot_encoder;

-- clone transform() to the transformer_clone schema
create or replace schema one_hot_encoder_clone clone feature_engineering.one_hot_encoder;

show procedures like '%fit%' in one_hot_encoder_clone;
show functions like '%transform%' in one_hot_encoder_clone;
desc function transform(varchar);
list @stage_test;


// ======================
//         TEST
// ======================

create or replace table table_test_transform as select uniform(1, 1000, random(2)) as integer from table(generator(rowcount => 10000000));
select integer, transform(integer) from table_test_transform;

create or replace table table_test_transform as select randstr(3, random(2)) as str from table(generator(rowcount => 1000000));
select str, transform(str) from table_test_transform;
