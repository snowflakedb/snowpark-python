create database if not exists feature_engineering;
create schema if not exists feature_engineering.binarizer;
use database feature_engineering;
use schema feature_engineering.binarizer;
use schema feature_engineering.binarizer_clone;

create or replace procedure fit(INPUT_QUERY string)
  returns varchar
  language javascript
  execute as caller
  as

  $$
  const TABLENAME = `table_temp`;
  const STAGENAME = `stage_test`;
  const FILENAME = `temp_binarizer.csv`;

  let inputQuery = INPUT_QUERY;
  inputQuery = inputQuery.replace(/;\s*$/, "");  // remove ending ; and trailing spaces

  // construct fitting queries
  let queries = [];

  const createTableQuery = `
  create or replace table ${TABLENAME} (threshold float);
  `;
  queries.push(createTableQuery);

  const insertQuery = `
  insert into ${TABLENAME} (threshold) values (0.0);
  `;
  queries.push(insertQuery);

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

show procedures like '%fit%' in feature_engineering.binarizer;
desc procedure feature_engineering.binarizer.fit(varchar);

create or replace schema binarizer_clone clone feature_engineering.binarizer;
show procedures like '%fit%' in binarizer_clone;
show functions like '%transform%' in binarizer_clone;
list @stage_test;


// ======================
//         TEST
// ======================

create or replace table table_test_fit as select random(1) as integer from table(generator(rowcount => 1000000));
call binarizer_clone.fit($$
  select
    integer
  from table_test_fit
  ;$$
);

select * from binarizer_clone.table_temp;
list @stage_test;

drop schema binarizer_clone;
