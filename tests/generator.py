import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
pd.DataFrame(data={
  'ints': [1, 2], 
  'umm': [3.14, 6.28], 
  'bools': [True, False],
  'int8': pd.Series([1, 2], dtype='Int8'),
  'int16': pd.Series([1, 2], dtype='Int16'),
  'int32': pd.Series([1, 2], dtype='Int32'),
  'int64': pd.Series([1, 2], dtype='Int64'),
  'uint8': pd.Series([1, 2], dtype='UInt8'),
  'uint16': pd.Series([1, 2], dtype='UInt16'),
  'uint32': pd.Series([1, 2], dtype='UInt32'),
  'uint64': pd.Series([1, 2], dtype='UInt64'),
  'dates': pd.Series([datetime(2018, 1, 1), datetime(2019, 12, 31)]),
  #'map': {"a": "x", "b": "y"},

}).to_parquet('tests/data/numbers.parquet')

pd.DataFrame(data={
  'json_array': pd.Series([[2], [3, 4, 5]]),
  'list_bytes': pd.Series([[b"a"], [b"x",b"y",b"z"]]),
  'ummm': pd.Series([{"name": "alex", "age": 10}, {"name": "brian", "age": 20}]),

}).to_parquet('tests/data/json.parquet')

pd.DataFrame(data={
  'category': pd.Series(["a", "b"], dtype="category"),
  'bytes': pd.Series([b"a", b"b"], dtype="bytes"),
}).to_parquet('tests/data/misc.parquet')

pd.DataFrame(data={
  'dates': pd.Series([
    datetime(2018, 1, 1), 
    datetime(2019, 12, 31),
    datetime(2020, 10, 31, 12, tzinfo=ZoneInfo("America/Los_Angeles"))
    ]),
}).to_parquet('tests/data/dates.parquet')



# breaks
# pd.Series([1, 2 ** 63], dtype='UInt64'),