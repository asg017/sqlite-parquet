-- 03-busiest-hours.sql: Find the busiest pickup hours
-- Run: solite-dev run demos/03-busiest-hours.sql

.load dist/release/parquet0

create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- Trips by hour of day
select
  cast(strftime('%H', pickup_datetime) as integer) as hour,
  count(*) as num_trips,
  round(avg(trip_miles), 2) as avg_miles,
  round(avg(base_passenger_fare), 2) as avg_fare
from trips
group by hour
order by hour;
