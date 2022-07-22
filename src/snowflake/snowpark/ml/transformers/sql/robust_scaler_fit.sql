create database if not exists feature_engineering;
create schema if not exists feature_engineering.robust_scaler;
use database feature_engineering;
use schema feature_engineering.robust_scaler;
use schema feature_engineering.robust_scaler_clone;

create or replace procedure fit(INPUT_QUERY string)
  returns varchar
  language javascript
  execute as caller
  as

  $$
  const TABLENAME = `table_temp`;
  const STAGENAME = `stage_test`;
  const FILENAME = `temp_robust_scaler.csv`;

  let inputQuery = INPUT_QUERY;
  inputQuery = inputQuery.replace(/;\s*$/, "");  // remove ending ; and trailing spaces

  // construct fitting queries
  let queries = [];

  const createTableQuery = `
  create or replace table ${TABLENAME} as
  with t as (
    ${inputQuery}
  )
  select median($1) as center, percentile_cont(0.25) within group (order by $1) as quantile_min, percentile_cont(0.75) within group (order by $1) as quantile_max from t;
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

show procedures like '%fit%' in feature_engineering.robust_scaler;
desc procedure feature_engineering.robust_scaler.fit(varchar);

create or replace schema robust_scaler_clone clone feature_engineering.robust_scaler;
show procedures like '%fit%' in robust_scaler_clone;
show functions like '%transform%' in robust_scaler_clone;
list @stage_test;


// ======================
//         TEST
// ======================

create or replace table table_test_fit as select uniform(1, 1000, random(1)) as integer from table(generator(rowcount => 1000));
call robust_scaler_clone.fit($$
  select
    integer
  from table_test_fit
  ;$$
);

select * from robust_scaler_clone.table_temp;
list @stage_test;

drop schema robust_scaler_clone;
