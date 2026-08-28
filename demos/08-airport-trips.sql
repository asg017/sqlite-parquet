-- 08-airport-trips.sql: Airport trip analysis (trips with airport fees)
-- Run: solite-dev run demos/08-airport-trips.sql

.load dist/release/parquet0

create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- Airport vs non-airport trips
select
  case when airport_fee > 0 then 'airport' else 'non-airport' end as trip_type,
  count(*) as num_trips,
  round(avg(trip_miles), 2) as avg_miles,
  round(avg(trip_time) / 60.0, 1) as avg_minutes,
  round(avg(base_passenger_fare), 2) as avg_fare,
  round(avg(tips), 2) as avg_tip,
  round(avg(tolls), 2) as avg_tolls
from trips
group by trip_type;

-- Top pickup locations for airport trips
select
  PULocationID as pickup_zone,
  count(*) as num_airport_trips,
  round(avg(base_passenger_fare), 2) as avg_fare
from trips
where airport_fee > 0
group by PULocationID
order by num_airport_trips desc
limit 15;
