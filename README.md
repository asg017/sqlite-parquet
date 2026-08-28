# sqlite-parquet

A work-in-progress SQLite extension for querying parquet files. Not meant to be widely shared yet.

```sql
.load ./parquet0

create virtual table trips using parquet('fhvhv_tripdata_2026-01.parquet');

select hvfhs_license_num, count(*), round(avg(trip_miles), 2)
from trips
group by 1;
```

## Building

```
make loadable          # dist/debug/parquet0.{dylib,so,dll}
make loadable-release  # dist/release/...
make test              # builds, then runs tests/test-loadable.py
make test-data         # regenerates tests/data/*.parquet with DuckDB
```

## API

### `parquet(...)` / `parquet0(...)` virtual table

Reads one or more parquet files. The path may be given three ways:

```sql
create virtual table t using parquet('data/trips.parquet');           -- positional
create virtual table t using parquet(filename = 'data/trips.parquet'); -- named
create virtual table "data/trips.parquet" using parquet;              -- table name
```

The path can be a glob, in which case every matching file is read in
sorted order as one table:

```sql
create virtual table trips using parquet('data/trips-*.parquet');

select parquet_path, count(*) from trips group by 1;
```

- The table's columns come from the first matching file. Later files are
  matched **by column name**, so column order may differ between files;
  a file missing a column that the query reads is an error.
- Only the columns a statement references are decoded from disk, so
  `select a, b from t` and `count(*)` don't pay for the other columns.
- `parquet_path` is a hidden column with the file the row came from. It is
  omitted from `select *`, and from the table entirely if the file already
  has a column of that name.
- `parquet0` is the versioned module name; `parquet` is an alias.

#### Type mapping

| Parquet / Arrow type                        | SQLite value                                            |
| ------------------------------------------- | ------------------------------------------------------- |
| boolean, int8–64, uint8–32                  | INTEGER                                                 |
| uint64                                      | INTEGER, or REAL when above `i64::MAX`                  |
| float16/32/64                               | REAL                                                    |
| decimal (any width)                         | INTEGER when scale is 0, otherwise REAL                 |
| string (utf8, large, view)                  | TEXT                                                    |
| binary (plain, large, view, fixed size)     | BLOB                                                    |
| date32 / date64                             | TEXT `YYYY-MM-DD`                                       |
| timestamp (any unit, with or without tz)    | TEXT `YYYY-MM-DD HH:MM:SS.SSS`, in UTC, ms precision    |
| list, struct, map                           | TEXT containing JSON (binary values inside are hex)     |
| dictionary / enum                           | the underlying value type                               |
| time, duration, interval, other             | TEXT via Arrow's display formatting                     |

NULLs are NULLs. Declared column types (`pragma table_info`) follow the same
mapping.

### `parquet_metadata(path_or_glob)`

One row per matching file with file-level metadata.

```sql
select path, num_rows, num_row_groups, created_by
from parquet_metadata('data/*.parquet');
```

Columns: `path`, `version`, `created_by`, `num_rows`, `num_columns`,
`num_row_groups`, `schema` (the parquet schema printed as text),
`key_value_metadata` (JSON object, or NULL).

### `parquet_column_chunks(path_or_glob)`

One row per column chunk (file × row group × column).

```sql
select column_name, compression, compressed_size, stats_min, stats_max
from parquet_column_chunks('data/trips.parquet')
where row_group = 0;
```

Columns: `path`, `row_group`, `column_name`, `column_type` (physical type),
`compression`, `encodings`, `num_values`, `compressed_size`,
`uncompressed_size`, `stats_min`, `stats_max`, `stats_distinct`,
`stats_null_count`. Statistics are NULL when the file doesn't record them.

### Scalar functions

- `parquet_version()` — extension version.
- `parquet_debug()` — version plus the git commit it was built from.
