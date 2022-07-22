create database if not exists feature_engineering;
create schema if not exists feature_engineering.string_indexer;
use database feature_engineering;
use schema feature_engineering.string_indexer;
use schema feature_engineering.string_indexer_clone;

ALTER SESSION SET ENABLE_DOP_DOWNGRADE = false;

create or replace procedure fit(INPUT_QUERY string)
  returns varchar
  language javascript
  execute as caller
  as

  $$
  const TABLENAME = `table_temp`;
  const STAGENAME = `stage_test`;
  const FILENAME = `temp_string_indexer.csv`;

  let inputQuery = INPUT_QUERY;
  inputQuery = inputQuery.replace(/;\s*$/, "");  // remove ending ; and trailing spaces

  // construct fitting queries
  let queries = [];

  const createTableQuery = `
  create or replace table ${TABLENAME} as
  with t as (
    ${inputQuery}
  )
  select distinct $1 as distinct_value from t as output;
  `;
  queries.push(createTableQuery);

  const createTempTableQuery = `
  create or replace table temp like ${TABLENAME};
  `;
  queries.push(createTempTableQuery);

  const createSeqQuery = `
  create or replace sequence seq;
  `;
  queries.push(createSeqQuery);

  const alterQuery = `
  alter table temp
  add column index int default seq.nextval;
  `;
  queries.push(alterQuery);

  const insertQuery = `
  insert into temp
  select *, seq.nextval
  from ${TABLENAME};
  `;
  queries.push(insertQuery);

  const dropQuery = `
  drop table ${TABLENAME};
  `;
  queries.push(dropQuery);

  const renameQuery = `
  alter table temp rename to ${TABLENAME};
  `;
  queries.push(renameQuery);

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

show procedures like '%fit%' in feature_engineering.string_indexer;
desc procedure feature_engineering.string_indexer.fit(varchar);

create or replace schema string_indexer_clone clone feature_engineering.string_indexer;
show procedures like '%fit%' in string_indexer_clone;
show functions like '%transform%' in string_indexer_clone;
list @stage_test;


// ======================
//        TEST
// ======================

create or replace table table_test_fit as select randstr(3, random(1)) as str from table(generator(rowcount => 1000000));
call string_indexer_clone.fit($$
  select
    str
  from table_test_fit
  ;$$
);

select * from string_indexer_clone.table_temp;
list @stage_test;

drop schema string_indexer_clone;
