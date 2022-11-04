.timer on

select count(*)
from 'yellow_tripdata_2022-01.parquet';

select VendorID, trip_distance 
from 'yellow_tripdata_2022-01.parquet' 
limit 5;
