create database if not exists feature_engineering;
create schema if not exists feature_engineering.imputer;
use database feature_engineering;
use schema feature_engineering.imputer;
use schema feature_engineering.imputer_clone;
create stage if not exists stage_test;

-- numerical
create or replace function feature_engineering.imputer.transform(value float)
returns float
language python
runtime_version = 3.8
packages = ('pandas')
imports=('@stage_test/temp_imputer.csv')
handler='transform'
as
$$
import sys
import pandas as pd
import csv
from _snowflake import vectorized

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
FILENAME = "temp_imputer.csv"

@vectorized(input=pd.DataFrame)
def transform(df):
    with open(import_dir + FILENAME, 'r') as file:
        reader = csv.reader(file)
        row = next(reader)
        mean = float(row[0])

    return df[0].fillna(mean)
$$;

-- categorical
create or replace function feature_engineering.imputer.transform(value string)
returns string
language python
runtime_version = 3.8
packages = ('pandas')
imports=('@stage_test/temp_imputer.csv')
handler='transform'
as
$$
import sys
import pandas as pd
import csv
from _snowflake import vectorized

IMPORT_DIRECTORY_NAME = "snowflake_import_directory"
import_dir = sys._xoptions[IMPORT_DIRECTORY_NAME]
FILENAME = "temp_imputer.csv"

@vectorized(input=pd.DataFrame)
def transform(df):
    with open(import_dir + FILENAME, 'r') as file:
        reader = csv.reader(file)
        row = next(reader)
        mode = row[1]

    return df[0].fillna(mode)
$$;

show functions like '%transform%' in imputer;

-- clone transform() to the transformer_clone schema
create or replace schema imputer_clone clone feature_engineering.imputer;

show procedures like '%fit%' in imputer_clone;
show functions like '%transform%' in imputer_clone;
desc function transform(float);
desc function transform(varchar);
list @stage_test;


// ======================
//         TEST
// ======================

select transform(0.5);
select transform('String');
select transform(null);

-- numerical
create or replace table table_test_transform as select uniform(0::float, 1::float, random(1)) as float from table(generator(rowcount => 5));
insert into table_test_transform values (null);
select float, transform(float) from table_test_transform;

create or replace table table_test_transform as select null as float from table(generator(rowcount => 10000000));
insert into table_test_transform values (0::float);
select float, transform(float) from table_test_transform;

-- categorical
create or replace table table_test_transform as select randstr(3, random(1)) as str from table(generator(rowcount => 5));
insert into table_test_transform values (null);
select str, transform(str) from table_test_transform;

create or replace table table_test_transform as select null as str from table(generator(rowcount => 10000000));
insert into table_test_transform values ('test');
select str, transform(str) from table_test_transform;
