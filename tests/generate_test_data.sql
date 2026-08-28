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

-- types.parquet: edge cases — negative timestamps, decimals (incl. negative,
-- scale 0), huge uint64, nested struct/list/map, enum (dictionary), quoted names
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
COPY (
  SELECT
    TIMESTAMP '1969-12-31 23:59:59.5' as ts_before_epoch,
    (-0.5)::DECIMAL(10,2) as dec_neg,
    123.45::DECIMAL(10,2) as dec_pos,
    42::DECIMAL(10,0) as dec_int,
    18446744073709551615::UBIGINT as u64_max,
    {'n': 1::TINYINT, 'xs': [1, 2]::INTEGER[], 'when': DATE '2020-02-29'} as nested,
    MAP {'a': 1, 'b': NULL} as m,
    'happy'::mood as mood,
    'x'::VARCHAR as "weird ""name""",
    1.5::FLOAT as f32,
    [1.5, NULL]::DOUBLE[] as list_with_null,
    NULL::INTEGER as always_null,
    '\x00\xff'::BLOB as bytes
) TO 'tests/data/types.parquet' (FORMAT PARQUET);
DROP TYPE mood;

-- gzip.parquet: exercises the gzip codec
COPY (
  SELECT range as n FROM range(10)
) TO 'tests/data/gzip.parquet' (FORMAT PARQUET, COMPRESSION GZIP);

-- empty.parquet: zero rows
COPY (
  SELECT 1::INTEGER as x WHERE false
) TO 'tests/data/empty.parquet' (FORMAT PARQUET);

-- glob/*.parquet: same columns, different order and widths
COPY (SELECT 1::INTEGER as a, 'one'::VARCHAR as b) TO 'tests/data/glob/part1.parquet' (FORMAT PARQUET);
COPY (SELECT 'two'::VARCHAR as b, 2::BIGINT as a) TO 'tests/data/glob/part2.parquet' (FORMAT PARQUET);

-- mismatch/*.parquet: the second file is missing a column the first has
COPY (SELECT 1::INTEGER as a, 'one'::VARCHAR as b) TO 'tests/data/mismatch/first.parquet' (FORMAT PARQUET);
COPY (SELECT 2::INTEGER as a) TO 'tests/data/mismatch/second.parquet' (FORMAT PARQUET);
