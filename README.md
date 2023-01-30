# sqlite-parquet

A work-in-progress SQLite extension for querying parquet files! Not meant to be widely shared.

Once it's ready, you'll be able to do things like:

```sql
.load ./parquet0

create virtual table temp.taxi using parquet(filename="tests/data/taxi_2019_04.parquet");

select
  vendor_id,
  pickup_at,
  dropoff_at,
  total_amount
from temp.taxi
limit 5;

/*
┌───────────┬─────────────────────────┬─────────────────────────┬──────────────────┐
│ vendor_id │        pickup_at        │       dropoff_at        │   total_amount   │
├───────────┼─────────────────────────┼─────────────────────────┼──────────────────┤
│ 1         │ 2019-04-01 00:04:09.000 │ 2019-04-01 00:06:35.000 │ 8.80000019073486 │
│ 1         │ 2019-04-01 00:22:45.000 │ 2019-04-01 00:25:43.000 │ 8.30000019073486 │
│ 1         │ 2019-04-01 00:39:48.000 │ 2019-04-01 01:19:39.000 │ 47.75            │
│ 1         │ 2019-04-01 00:35:32.000 │ 2019-04-01 00:37:11.000 │ 7.30000019073486 │
│ 1         │ 2019-04-01 00:44:05.000 │ 2019-04-01 00:57:58.000 │ 23.1499996185303 │
└───────────┴─────────────────────────┴─────────────────────────┴──────────────────┘
*/

select * from parquet_metadata('tests/data/taxi_2019_04.parquet');
select * from parquet_column_chunks('tests/data/taxi_2019_04.parquet') limit 10;
```
