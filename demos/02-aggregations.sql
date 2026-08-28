-- 02-aggregations.sql: Aggregate stats on NYC rideshare trips
-- Run: solite-dev run demos/02-aggregations.sql

.load dist/release/parquet0

create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- Average trip distance and time by license (Uber vs Lyft)
select
  hvfhs_license_num as license,
  count(*) as num_trips,
  round(avg(trip_miles), 2) as avg_miles,
  round(avg(trip_time) / 60.0, 1) as avg_minutes,
  round(avg(base_passenger_fare), 2) as avg_fare,
  round(avg(tips), 2) as avg_tip
from trips
group by hvfhs_license_num;
