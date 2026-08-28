-- 04-top-routes.sql: Most popular pickup/dropoff location pairs
-- Run: solite-dev run demos/04-top-routes.sql

.load dist/release/parquet0

create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- Top 20 routes by trip count
select
  PULocationID as pickup,
  DOLocationID as dropoff,
  count(*) as num_trips,
  round(avg(trip_miles), 2) as avg_miles,
  round(avg(base_passenger_fare), 2) as avg_fare,
  round(avg(tips), 2) as avg_tip
from trips
group by PULocationID, DOLocationID
order by num_trips desc
limit 20;
