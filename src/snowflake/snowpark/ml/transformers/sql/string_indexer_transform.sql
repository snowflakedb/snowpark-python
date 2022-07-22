create database if not exists feature_engineering;
create schema if not exists feature_engineering.string_indexer;
use database feature_engineering;
use schema feature_engineering.string_indexer;
use schema feature_engineering.string_indexer_clone;
create stage if not exists stage_test;

create or replace function feature_engineering.string_indexer.transform(value string)
returns number(10, 0)
language python
runtime_version = 3.8
packages = ('pandas')
imports=('@stage_test/temp_string_indexer.csv')
handler='transform'
as
$$
import sys
import pandas as pd
import csv
from _snowflake import vectorized

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
FILENAME = "temp_string_indexer.csv"

@vectorized(input=pd.DataFrame)
def transform(df):
    with open(import_dir + FILENAME, 'r') as file:
        reader = csv.reader(file)
        value_to_index = {row[0]: row[1] for row in reader}

    return df[0].map(value_to_index).fillna(-1)
$$;

show functions like '%transform%' in string_indexer;

-- clone transform() to the transformer_clone schema
create or replace schema string_indexer_clone clone feature_engineering.string_indexer;

show procedures like '%fit%' in string_indexer_clone;
show functions like '%transform%' in string_indexer_clone;
desc function transform(varchar);
list @stage_test;


// ======================
//         TEST
// ======================

create or replace table table_test_transform as select randstr(3, random(2)) as str from table(generator(rowcount => 1000000));
select str, transform(str) from table_test_transform;
