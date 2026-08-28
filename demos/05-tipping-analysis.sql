-- 05-tipping-analysis.sql: Who tips and how much?
-- Run: solite-dev run demos/05-tipping-analysis.sql

.load dist/release/parquet0

create virtual table trips using parquet0(filename = 'fhvhv_tripdata_2026-01.parquet');

-- Tip percentage distribution (for trips with a fare > $5)
select
  case
    when tips = 0 then 'no tip'
    when tips / base_passenger_fare < 0.10 then 'under 10%'
    when tips / base_passenger_fare < 0.15 then '10-15%'
    when tips / base_passenger_fare < 0.20 then '15-20%'
    when tips / base_passenger_fare < 0.25 then '20-25%'
    else '25%+'
  end as tip_bucket,
  count(*) as num_trips,
  round(avg(base_passenger_fare), 2) as avg_fare,
  round(avg(tips), 2) as avg_tip,
  round(avg(trip_miles), 2) as avg_miles
from trips
where base_passenger_fare > 5
group by tip_bucket
order by
  case tip_bucket
    when 'no tip' then 0
    when 'under 10%' then 1
    when '10-15%' then 2
    when '15-20%' then 3
    when '20-25%' then 4
    else 5
  end;
