-- 07-daily-patterns.sql: Day-of-week and daily trip patterns
-- Run: solite-dev run demos/07-daily-patterns.sql

.load dist/release/parquet0

create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- Trips by day of week (0=Sunday, 6=Saturday)
select
  case cast(strftime('%w', pickup_datetime) as integer)
    when 0 then 'Sun'
    when 1 then 'Mon'
    when 2 then 'Tue'
    when 3 then 'Wed'
    when 4 then 'Thu'
    when 5 then 'Fri'
    when 6 then 'Sat'
  end as day,
  count(*) as num_trips,
  round(avg(trip_miles), 2) as avg_miles,
  round(avg(base_passenger_fare), 2) as avg_fare,
  round(sum(base_passenger_fare + tips + tolls + congestion_surcharge), 2) as total_revenue
from trips
group by strftime('%w', pickup_datetime)
order by strftime('%w', pickup_datetime);
