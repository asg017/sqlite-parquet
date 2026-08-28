-- Generate test parquet files using DuckDB
-- Usage: duckdb < tests/generate_test_data.sql

-- numbers.parquet: all numeric types + bool + timestamp
COPY (
  SELECT
    1::TINYINT as int8, 1::SMALLINT as int16, 1::INTEGER as int32, 1::BIGINT as int64,
    1::UTINYINT as uint8, 1::USMALLINT as uint16, 1::UINTEGER as uint32, 1::UBIGINT as uint64,
    1::INTEGER as ints, 3.14::DOUBLE as umm, true::BOOLEAN as bools,
    TIMESTAMP '2018-01-01 00:00:00'::TIMESTAMP as dates
  UNION ALL SELECT
    2, 2, 2, 2, 2, 2, 2, 2, 2, 6.28, false, TIMESTAMP '2019-12-31 00:00:00'
) TO 'tests/data/numbers.parquet' (FORMAT PARQUET, FIELD_IDS 'auto');

-- json.parquet: lists, structs
COPY (
  SELECT
    [2]::INTEGER[] as json_array,
    {'name': 'alex', 'age': 10}::STRUCT(name VARCHAR, age INTEGER) as ummm
  UNION ALL SELECT
    [3,4,5],
    {'name': 'brian', 'age': 20}
) TO 'tests/data/json.parquet' (FORMAT PARQUET);

-- misc.parquet: bytes and strings
COPY (
  SELECT 'a'::VARCHAR as category, '\x61'::BLOB as bytes
  UNION ALL SELECT 'b', '\x62'::BLOB
) TO 'tests/data/misc.parquet' (FORMAT PARQUET);

-- dates.parquet: timestamps
COPY (
  SELECT TIMESTAMP '2018-01-01 00:00:00' as dates
  UNION ALL SELECT TIMESTAMP '2019-12-31 00:00:00'
  UNION ALL SELECT TIMESTAMP '2020-10-31 19:00:00'
) TO 'tests/data/dates.parquet' (FORMAT PARQUET);

-- simple.parquet: minimal test file
COPY (
  SELECT 1::INTEGER as x, 'hello'::VARCHAR as y
) TO 'tests/data/simple.parquet' (FORMAT PARQUET);

-- duck.parquet: mixed types
COPY (
  SELECT
    1::INTEGER as col0,
    'alex'::VARCHAR as col1,
    '\x00\x00\x00\x01'::BLOB as col2,
    TIMESTAMP '2022-10-26 23:01:24.303' as col3,
    DATE '1999-12-31' as col4
  UNION ALL SELECT
    2, 'brian', '\x00\x00\x00\x02'::BLOB, TIMESTAMP '2022-10-26 23:01:24.303', DATE '2000-01-01'
) TO 'tests/data/duck.parquet' (FORMAT PARQUET);
