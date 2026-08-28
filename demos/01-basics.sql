-- 01-basics.sql: Load a parquet file and explore the schema
-- Run: solite-dev run demos/01-basics.sql

.load dist/release/parquet0

-- Create a virtual table pointing at the tripdata parquet file
create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- See the schema
pragma table_info(trips);

-- Peek at a few rows
select * from trips limit 5;

-- How many trips total?
select count(*) as total_trips from trips;
