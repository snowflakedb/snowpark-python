create database if not exists feature_engineering;
create schema if not exists feature_engineering.max_abs_scaler;
use database feature_engineering;
use schema feature_engineering.max_abs_scaler;
use schema feature_engineering.max_abs_scaler_clone;

create or replace procedure fit(INPUT_QUERY string)
  returns varchar
  language javascript
  execute as caller
  as

  $$
  const TABLENAME = `table_temp`;
  const STAGENAME = `stage_test`;
  const FILENAME = `temp_max_abs_scaler.csv`;

  let inputQuery = INPUT_QUERY;
  inputQuery = inputQuery.replace(/;\s*$/, "");  // remove ending ; and trailing spaces

  // construct fitting queries
  let queries = [];

  const createTableQuery = `
  create or replace table ${TABLENAME} as
  with t as (
    ${inputQuery}
  )
  select max(abs($1)) as max_abs from t;
  `;
  queries.push(createTableQuery);

  const createStageQuery = `
  create stage if not exists ${STAGENAME};
  `;
  queries.push(createStageQuery);

  const copyQuery = `
  copy into @${STAGENAME}/${FILENAME}
       from ${TABLENAME}
       file_format=(type=csv, compression=NONE, field_delimiter=',')
       overwrite=TRUE
       single=TRUE;
  `;
  queries.push(copyQuery);

  // fit
  for (const query of queries) {
    const stmt = snowflake.createStatement({sqlText: query});
    stmt.execute();
  }

  // return instructions
  return `Fitting completed successfully.`;
  $$;

show procedures like '%fit%' in feature_engineering.max_abs_scaler;
desc procedure feature_engineering.max_abs_scaler.fit(varchar);

create or replace schema max_abs_scaler_clone clone feature_engineering.max_abs_scaler;
show procedures like '%fit%' in max_abs_scaler_clone;
show functions like '%transform%' in max_abs_scaler_clone;
list @stage_test;


// ======================
//         TEST
// ======================

create or replace table table_test_fit as select uniform(-1000, 1000, random(1)) as integer from table(generator(rowcount => 20));
call max_abs_scaler_clone.fit($$
  select
    integer
  from table_test_fit
  ;$$
);

select * from max_abs_scaler_clone.table_temp;
list @stage_test;

drop schema max_abs_scaler_clone;
