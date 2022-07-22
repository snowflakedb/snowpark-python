create database if not exists feature_engineering;
create schema if not exists feature_engineering.imputer;
use database feature_engineering;
use schema feature_engineering.imputer;
use schema feature_engineering.imputer_clone;

create or replace procedure fit(INPUT_QUERY string, IS_NUMERICAL boolean)
  returns varchar
  language javascript
  execute as caller
  as

  $$
  const TABLENAME = `table_temp`;
  const STAGENAME = `stage_test`;
  const FILENAME = `temp_imputer.csv`;

  let inputQuery = INPUT_QUERY;
  inputQuery = inputQuery.replace(/;\s*$/, "");  // remove ending ; and trailing spaces
  const isNumerical = IS_NUMERICAL;

  // construct fitting queries
  let queries = [];

  const selectQuery = `
    ${isNumerical?
      `
        select
          avg($1) as mean, 'null' as mode
        from t
      ` :
      `
        select
          -1.0 as mean, mode($1) as mode
        from t
      `
    }
  `;

  const createTableQuery = `
  create or replace table ${TABLENAME} as
  with t as (
    ${inputQuery}
  )
  ${selectQuery};
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

show procedures like '%fit%' in feature_engineering.imputer;
desc procedure feature_engineering.imputer.fit(varchar, boolean);

create or replace schema imputer_clone clone feature_engineering.imputer;
show procedures like '%fit%' in imputer_clone;
show functions like '%transform%' in imputer_clone;
list @stage_test;


// ======================
//         TEST
// ======================

--numerical
create or replace table table_test_fit as select uniform(0::float, 1::float, random(1)) as float from table(generator(rowcount => 1000000));
call imputer_clone.fit($$
  select
    float
  from table_test_fit
  ;$$,
  true
);

-- categorical
create or replace table table_test_fit as select randstr(5, random(1)) as str from table(generator(rowcount => 1000000));
call imputer_clone.fit($$
  select
    str
  from table_test_fit
  ;$$,
  false
);

select * from imputer_clone.table_temp;
list @stage_test;

drop schema imputer_clone;
