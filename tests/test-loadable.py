import sqlite3
import unittest
import os

EXT_PATH="./dist/debug/parquet0"

def connect(ext):
  db = sqlite3.connect(":memory:")

  db.execute("create temp table base_functions as select name from pragma_function_list")
  db.execute("create temp table base_modules as select name from pragma_module_list")

  db.enable_load_extension(True)
  db.load_extension(ext)

  db.execute("create temp table loaded_functions as select name from pragma_function_list where name not in (select name from base_functions) order by name")
  db.execute("create temp table loaded_modules as select name from pragma_module_list where name not in (select name from base_modules) order by name")

  db.row_factory = sqlite3.Row
  return db

db = connect(EXT_PATH)

def execute_all(sql, args=None):
  if args is None: args = []
  results = db.execute(sql, args).fetchall()
  return list(map(lambda x: dict(x), results))

def execute_error(sql):
  with unittest.TestCase().assertRaises(sqlite3.OperationalError) as cm:
    db.execute(sql).fetchall()
  return str(cm.exception)

FUNCTIONS = [
  "parquet_debug",
  "parquet_version",
]

MODULES = [
  "parquet",
  "parquet0",
  "parquet_column_chunks",
  "parquet_metadata",
]

class TestParquet(unittest.TestCase):
  def test_funcs(self):
    funcs = list(map(lambda a: a[0], db.execute("select name from loaded_functions").fetchall()))
    self.assertEqual(funcs, FUNCTIONS)

  def test_modules(self):
    modules = list(map(lambda a: a[0], db.execute("select name from loaded_modules").fetchall()))
    self.assertEqual(modules, MODULES)

  def test_parquet_version(self):
    version = 'v0.1.0'
    self.assertEqual(db.execute("select parquet_version()").fetchone()[0], version)

  def test_parquet_debug(self):
    debug = db.execute("select parquet_debug()").fetchone()[0]
    self.assertEqual(len(debug.splitlines()), 2)

  def test_parquet(self):
    # `parquet` is an alias of `parquet0`
    db.execute("create virtual table alias using parquet(filename='tests/data/simple.parquet');")
    self.assertEqual(execute_all("select * from alias"), [{'x': 1, 'y': 'hello'}])

  def test_parquet0(self):
    db.execute("create virtual table numbers using parquet0(filename='tests/data/numbers.parquet');")
    self.assertEqual(
      execute_all("select * from numbers"),
       [
        {'bools': 1,
         'dates': '2018-01-01 00:00:00.000',
         'int16': 1,
         'int32': 1,
         'int64': 1,
         'int8': 1,
         'ints': 1,
         'uint16': 1,
         'uint32': 1,
         'uint64': 1.0,  # stored as DOUBLE in the file (DuckDB FIELD_IDS quirk)
         'uint8': 1,
         'umm': 3.14},
        {'bools': 0,
         'dates': '2019-12-31 00:00:00.000',
         'int16': 2,
         'int32': 2,
         'int64': 2,
         'int8': 2,
         'ints': 2,
         'uint16': 2,
         'uint32': 2,
         'uint64': 2.0,
         'uint8': 2,
         'umm': 6.28}]
    )
    self.assertEqual(
      execute_all("select typeof(bools) as b, typeof(int8) as i, typeof(umm) as f, typeof(dates) as d from numbers limit 1"),
      [{'b': 'integer', 'i': 'integer', 'f': 'real', 'd': 'text'}]
    )

    db.execute("create virtual table json using parquet0(filename='tests/data/json.parquet');")
    self.assertEqual(
      execute_all("select * from json;"),
         [
          { 'json_array': '[2]', 'ummm': '{"age":10,"name":"alex"}' },
          { 'json_array': '[3,4,5]', 'ummm': '{"age":20,"name":"brian"}' }
        ]
    )
    # nested values are real JSON, usable with SQLite's JSON functions
    self.assertEqual(
      execute_all("select ummm ->> '$.name' as name, json_array_length(json_array) as n from json;"),
      [{'name': 'alex', 'n': 1}, {'name': 'brian', 'n': 3}]
    )

    db.execute("create virtual table misc using parquet0(filename='tests/data/misc.parquet');")
    self.assertEqual(
      execute_all("select * from misc;"),
      [
        {'bytes': b'a', 'category': 'a'},
        {'bytes': b'b', 'category': 'b'}
      ]
    )

    db.execute("create virtual table dates using parquet0(filename='tests/data/dates.parquet');")
    self.assertEqual(
      execute_all("select * from dates;"),
       [
        {'dates': '2018-01-01 00:00:00.000'},
        {'dates': '2019-12-31 00:00:00.000'},
        {'dates': '2020-10-31 19:00:00.000'},
      ]
    )

    db.execute("create virtual table duck using parquet0(filename='tests/data/duck.parquet');")
    self.assertEqual(
      execute_all("select * from duck;"),
        [
          { 'col0': 1, 'col1': 'alex', 'col2': b'\x00\x00\x00\x01', 'col3': '2022-10-26 23:01:24.303', 'col4': '1999-12-31'},
          { 'col0': 2, 'col1': 'brian', 'col2': b'\x00\x00\x00\x02', 'col3': '2022-10-26 23:01:24.303', 'col4': '2000-01-01'}
        ]
    )

  def test_parquet0_arguments(self):
    # positional, quoted
    db.execute("create virtual table pos1 using parquet0('tests/data/simple.parquet');")
    self.assertEqual(execute_all("select * from pos1"), [{'x': 1, 'y': 'hello'}])
    # positional, bare
    db.execute("create virtual table pos2 using parquet0(tests/data/simple.parquet);")
    self.assertEqual(execute_all("select * from pos2"), [{'x': 1, 'y': 'hello'}])
    # filename=, double quoted
    db.execute('create virtual table named using parquet0(filename="tests/data/simple.parquet");')
    self.assertEqual(execute_all("select * from named"), [{'x': 1, 'y': 'hello'}])
    # table-name-as-filepath
    db.execute('create virtual table "tests/data/simple.parquet" using parquet0;')
    self.assertEqual(execute_all('select * from "tests/data/simple.parquet";'), [{'x': 1, 'y': 'hello'}])

    self.assertIn("unknown option 'foo'", execute_error("create virtual table e1 using parquet0(foo='bar')"))
    self.assertIn("no files matched 'e2'", execute_error("create virtual table e2 using parquet0"))
    self.assertIn("no files matched 'nope.parquet'", execute_error("create virtual table e3 using parquet0('nope.parquet')"))
    self.assertIn("more than once", execute_error("create virtual table e4 using parquet0(filename='a', 'b')"))
    self.assertIn("column declarations are not supported", execute_error("create virtual table e5 using parquet0(a integer)"))
    self.assertIn("failed to read parquet file", execute_error("create virtual table e6 using parquet0('tests/test-loadable.py')"))

  def test_parquet0_schema(self):
    db.execute("create virtual table schema using parquet0('tests/data/types.parquet');")
    self.assertEqual(
      execute_all("select name, type, hidden from pragma_table_xinfo('schema')"),
      [
        {'name': 'ts_before_epoch', 'type': 'TEXT', 'hidden': 0},
        {'name': 'dec_neg', 'type': 'REAL', 'hidden': 0},
        {'name': 'dec_pos', 'type': 'REAL', 'hidden': 0},
        {'name': 'dec_int', 'type': 'INTEGER', 'hidden': 0},
        {'name': 'u64_max', 'type': 'INTEGER', 'hidden': 0},
        {'name': 'nested', 'type': 'TEXT', 'hidden': 0},
        {'name': 'm', 'type': 'TEXT', 'hidden': 0},
        {'name': 'mood', 'type': 'TEXT', 'hidden': 0},
        {'name': 'weird "name"', 'type': 'TEXT', 'hidden': 0},
        {'name': 'f32', 'type': 'REAL', 'hidden': 0},
        {'name': 'list_with_null', 'type': 'TEXT', 'hidden': 0},
        {'name': 'always_null', 'type': 'INTEGER', 'hidden': 0},
        {'name': 'bytes', 'type': 'BLOB', 'hidden': 0},
        {'name': 'parquet_path', 'type': 'TEXT', 'hidden': 1},
      ]
    )

  def test_parquet0_types(self):
    db.execute("create virtual table types using parquet0('tests/data/types.parquet');")
    self.assertEqual(
      execute_all("select * from types"),
      [{
        'ts_before_epoch': '1969-12-31 23:59:59.500',
        'dec_neg': -0.5,
        'dec_pos': 123.45,
        'dec_int': 42,
        'u64_max': 18446744073709551615.0,
        'nested': '{"n":1,"when":"2020-02-29","xs":[1,2]}',
        'm': '{"a":1,"b":null}',
        'mood': 'happy',
        'weird "name"': 'x',
        'f32': 1.5,
        'list_with_null': '[1.5,null]',
        'always_null': None,
        'bytes': b'\x00\xff',
      }]
    )
    self.assertEqual(
      execute_all("select typeof(dec_neg) a, typeof(dec_int) b, typeof(u64_max) c, typeof(always_null) d, typeof(bytes) e from types"),
      [{'a': 'real', 'b': 'integer', 'c': 'real', 'd': 'null', 'e': 'blob'}]
    )
    self.assertEqual(execute_all('select "weird ""name""" as w from types'), [{'w': 'x'}])

  def test_parquet0_glob(self):
    db.execute("create virtual table g using parquet0('tests/data/glob/*.parquet');")
    # columns are matched by name per file, so differently-ordered schemas line up
    self.assertEqual(
      execute_all("select rowid, parquet_path, a, b from g"),
      [
        {'rowid': 1, 'parquet_path': 'tests/data/glob/part1.parquet', 'a': 1, 'b': 'one'},
        {'rowid': 2, 'parquet_path': 'tests/data/glob/part2.parquet', 'a': 2, 'b': 'two'},
      ]
    )
    # hidden column stays out of `select *`
    self.assertEqual(execute_all("select * from g where a = 2"), [{'a': 2, 'b': 'two'}])
    # no columns projected at all
    self.assertEqual(execute_all("select count(*) as n from g"), [{'n': 2}])
    # self join: each cursor tracks its own file
    self.assertEqual(
      execute_all("select x.parquet_path as xp, y.parquet_path as yp from g x, g y where x.a = 1 and y.a = 2"),
      [{'xp': 'tests/data/glob/part1.parquet', 'yp': 'tests/data/glob/part2.parquet'}]
    )

    db.execute("create virtual table mismatch using parquet0('tests/data/mismatch/*.parquet');")
    # only `a` is needed, and every file has it
    self.assertEqual(execute_all("select a from mismatch"), [{'a': 1}, {'a': 2}])
    # `b` is missing from the second file
    self.assertIn("does not have column 'b'", execute_error("select b from mismatch"))

  def test_parquet0_misc_files(self):
    db.execute("create virtual table gz using parquet0('tests/data/gzip.parquet');")
    self.assertEqual(execute_all("select sum(n) as s, count(*) as c from gz"), [{'s': 45, 'c': 10}])
    db.execute("create virtual table empty using parquet0('tests/data/empty.parquet');")
    self.assertEqual(execute_all("select count(*) as c from empty"), [{'c': 0}])
    self.assertEqual(execute_all("select * from empty"), [])

  def test_parquet_metadata(self):
    rows = execute_all("select * from parquet_metadata('tests/data/numbers.parquet');")
    self.assertEqual(len(rows), 1)
    row = rows[0]
    self.assertEqual(row['path'], 'tests/data/numbers.parquet')
    self.assertEqual(row['num_rows'], 2)
    self.assertEqual(row['num_columns'], 12)
    self.assertEqual(row['num_row_groups'], 1)
    self.assertTrue(row['created_by'].startswith('DuckDB'))
    self.assertTrue(row['schema'].startswith('message duckdb_schema {'))
    self.assertIsNone(row['key_value_metadata'])

    self.assertEqual(
      execute_all("select rowid, path, num_rows from parquet_metadata('tests/data/glob/*.parquet')"),
      [
        {'rowid': 1, 'path': 'tests/data/glob/part1.parquet', 'num_rows': 1},
        {'rowid': 2, 'path': 'tests/data/glob/part2.parquet', 'num_rows': 1},
      ]
    )
    self.assertIn("no files matched", execute_error("select * from parquet_metadata('nope/*.parquet')"))
    self.assertIn("must not be NULL", execute_error("select * from parquet_metadata(NULL)"))

  def test_parquet_column_chunks(self):
    rows = execute_all("select * from parquet_column_chunks('tests/data/numbers.parquet');")
    self.assertEqual(len(rows), 12)
    self.assertEqual([r['column_name'] for r in rows][:3], ['int8', 'int16', 'int32'])
    self.assertEqual(rows[0]['path'], 'tests/data/numbers.parquet')
    self.assertEqual(rows[0]['row_group'], 0)
    self.assertEqual(rows[0]['column_type'], 'INT32')
    self.assertEqual(rows[0]['compression'], 'SNAPPY')
    self.assertEqual(rows[0]['num_values'], 2)
    self.assertEqual((rows[0]['stats_min'], rows[0]['stats_max']), (1, 2))
    self.assertEqual(rows[0]['stats_null_count'], 0)
    # rowids are unique
    self.assertEqual([r['rowid'] for r in execute_all("select rowid from parquet_column_chunks('tests/data/numbers.parquet')")], list(range(1, 13)))

    self.assertEqual(
      execute_all("select path, column_name, column_type, compression, stats_min, stats_max from parquet_column_chunks('tests/data/g*.parquet')"),
      [{'path': 'tests/data/gzip.parquet', 'column_name': 'n', 'column_type': 'INT64', 'compression': 'GZIP', 'stats_min': 0, 'stats_max': 9}]
    )
    self.assertEqual(
      execute_all("select path, column_name from parquet_column_chunks('tests/data/glob/*.parquet')"),
      [
        {'path': 'tests/data/glob/part1.parquet', 'column_name': 'a'},
        {'path': 'tests/data/glob/part1.parquet', 'column_name': 'b'},
        {'path': 'tests/data/glob/part2.parquet', 'column_name': 'b'},
        {'path': 'tests/data/glob/part2.parquet', 'column_name': 'a'},
      ]
    )

class TestCoverage(unittest.TestCase):
  def test_coverage(self):
    test_methods = [method for method in dir(TestParquet) if method.startswith('test_')]
    funcs_with_tests = set([x.replace("test_", "") for x in test_methods])

    for func in FUNCTIONS:
      self.assertTrue(func in funcs_with_tests, f"{func} does not have corresponding test in {funcs_with_tests}")

    for module in MODULES:
      self.assertTrue(module in funcs_with_tests, f"{module} does not have corresponding test in {funcs_with_tests}")

if __name__ == '__main__':
    unittest.main()
