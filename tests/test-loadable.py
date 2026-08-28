import sqlite3
import unittest
import os

EXT_PATH="./dist/debug/parquet0"

def connect(ext):
  db = sqlite3.connect(":memory:")

  db.execute("create table base_functions as select name from pragma_function_list")
  db.execute("create table base_modules as select name from pragma_module_list")

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

FUNCTIONS = [
  "parquet_debug",
  "parquet_path",
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
         'uint64': 1.0,
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

    db.execute("create virtual table json using parquet0(filename='tests/data/json.parquet');")
    self.assertEqual(
      execute_all("select * from json;"),
         [
          { 'json_array': '[2]', 'ummm': '{"age":10,"name":"alex"}' },
          { 'json_array': '[3,4,5]', 'ummm': '{"age":20,"name":"brian"}' }
        ]
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
        {'dates': '2020-10-31 19:00:00.000'}
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

  def test_parquet0(self):
    # table-name-as-filepath syntax
    db.execute('create virtual table "tests/data/simple.parquet" using parquet0;')
    rows = execute_all('select * from "tests/data/simple.parquet";')
    self.assertEqual(rows, [{'x': 1, 'y': 'hello'}])

    # glob pattern
    db.execute('create virtual table "tests/data/d*.parquet" using parquet0;')
    rows = execute_all('select parquet_path(rowid) as path from "tests/data/d*.parquet";')
    paths = [r['path'] for r in rows]
    self.assertIn('tests/data/dates.parquet', paths)
    self.assertIn('tests/data/duck.parquet', paths)

  def test_parquet_path(self):
    db.execute("create virtual table ppath using parquet0(filename='tests/data/dates.parquet');")
    rows = execute_all("select parquet_path(rowid) as path from ppath;")
    for r in rows:
      self.assertEqual(r['path'], 'tests/data/dates.parquet')

  def test_parquet_metadata(self):
    rows = execute_all("select * from parquet_metadata('tests/data/numbers.parquet');")
    self.assertEqual(len(rows), 1)
    self.assertEqual(rows[0]['num_rows'], 2)
    self.assertEqual(rows[0]['num_row_groups'], 1)

  def test_parquet_column_chunks(self):
    rows = execute_all("select * from parquet_column_chunks('tests/data/numbers.parquet');")
    self.assertTrue(len(rows) > 0)
    col_names = [r['column_name'] for r in rows]
    self.assertIn('"int8"', col_names)

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
