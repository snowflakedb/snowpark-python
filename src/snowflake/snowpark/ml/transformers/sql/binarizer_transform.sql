create database if not exists feature_engineering;
create schema if not exists feature_engineering.binarizer;
use database feature_engineering;
use schema feature_engineering.binarizer;
use schema feature_engineering.binarizer_clone;
create stage if not exists stage_test;

create or replace function feature_engineering.binarizer.transform(value float)
returns float
language python
runtime_version = 3.8
packages = ('pandas')
imports=('@stage_test/temp_binarizer.csv')
handler='transform'
as
$$
import sys
import pandas as pd
import csv
from _snowflake import vectorized

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
FILENAME = "temp_binarizer.csv"

@vectorized(input=pd.DataFrame)
def transform(df):
    with open(import_dir + FILENAME, 'r') as file:
        reader = csv.reader(file)
        row = next(reader)
        threshold = float(row[0])

    return (df[0] > threshold).astype(float)
$$;

show functions like '%transform%' in binarizer;

-- clone transform() to the transformer_clone schema
create or replace schema binarizer_clone clone feature_engineering.binarizer;

show procedures like '%fit%' in binarizer_clone;
show functions like '%transform%' in binarizer_clone;
desc function transform(varchar);
list @stage_test;


// ======================
//         TEST
// ======================

create or replace table table_test_transform as select random(1) as integer from table(generator(rowcount => 1000000));
select integer, transform(integer) from table_test_transform;
