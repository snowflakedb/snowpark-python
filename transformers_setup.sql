create or replace database feature_engineering;
use database feature_engineering;
create or replace schema transformers;
use schema transformers;

create or replace table dataset_context (
  version varchar not null,
  asof datetime not null,
  num_records bigint, -- the number of records in the dataset
  constraint pkey_1 primary key (version) enforced
);

create or replace table columns_metadata (
  dataset_version varchar not null,
  column_name varchar not null, -- name of the column
  data_type varchar not null, -- datatype of the column
  tags varchar, -- column tags
  is_array boolean, -- whether the column is an array
  basic_statistics object, -- an object containing basic statistics applicable to all types
  numeric_statistics object, -- contains basic numeric statistics (e.g. min, max)
  constraint pkey_1 primary key (dataset_version, column_name) enforced,
  constraint fkey_1 foreign key (dataset_version) REFERENCES dataset_context (version) enforced
);

create or replace table fit_run (
  fit_run_id varchar not null, -- unique id of this fitrun
  dataset_version varchar not null,
  transformer varchar not null, -- the transformer used for this fit run
  settings object, -- the settings to the transformer, e.g. max bins
  input_columns array,
  output_columns array,
  time_of_run datetime, -- the time at which fit was called
  custom_state object, -- opaque custom state interpreted by the transformer
  constraint pkey_1 primary key (fit_run_id) enforced,
  constraint fkey_1 foreign key (dataset_version) REFERENCES dataset_context (version) enforced
);

create or replace table dictionary_data (
  dataset_version varchar not null,
  column_name varchar not null,
  column_value varchar,
  counts object, -- the co-occurrence of the column value with each value of the label
  constraint pkey_1 primary key (dataset_version, column_name) enforced,
  constraint fkey_1 foreign key (dataset_version) REFERENCES dataset_context (version) enforced
);

create or replace table percentiles_data (
  dataset_version varchar not null,
  column_name varchar not null,
  percentiles object, -- returned by APPROX_PERCENTILE_ACCUMULATE
  constraint pkey_1 primary key (dataset_version, column_name) enforced,
  constraint fkey_1 foreign key (dataset_version) REFERENCES dataset_context (version) enforced
);
-------------------------------------------------------------------------------------------------


