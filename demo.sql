select value from generate_series(1, 10);



.load dist/debug/parquet0

-- Basic: filename= syntax
create virtual table numbers using parquet0(filename = 'tests/data/numbers.parquet');

select * from numbers;

-- Table-name-as-filepath
create virtual table "tests/data/duck.parquet" using parquet0;

select * from "tests/data/duck.parquet";

-- Glob: query multiple files at once
create virtual table "tests/data/*.parquet" using parquet0;

select
  parquet_path as file,
  *
from "tests/data/*.parquet"
limit 10;

-- JSON columns: lists and structs come back as JSON text
create virtual table json_demo using parquet0(filename = 'tests/data/json.parquet');

select
  json_array,
  ummm ->> '$.name' as name,
  ummm ->> '$.age' as age
from json_demo;

-- Metadata introspection
select name from parquet_metadata('tests/data/numbers.parquet');

select
  column_name,
  column_type,
  num_values,
  stats_min,
  stats_max
from parquet_column_chunks('tests/data/numbers.parquet');

-- Version
select parquet_version();

