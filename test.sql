.load target/release/libparquet0
.mode box
.timer on

select parquet_version();

with files as (
  select name
  from fsdir('./tests/data')
  where name like '%.parquet'
)
select *
from files
join parquet_metadata(files.name);


with files as (
  select name
  from fsdir('./tests/data')
  where name like '%.parquet'
)
select count(*)
from files
join parquet_column_chunks(files.name);

.exit

select * from parquet_metadata('yellow_tripdata_2022-01.parquet');
select * from parquet_metadata('train-0of7.parquet');
select * from parquet_metadata('tests/data/taxi_2019_04.parquet');
select * from parquet_metadata('tests/data/dates.parquet');
select * from parquet_metadata('tests/data/duck.parquet');
select * from parquet_metadata('tests/data/json.parquet');
select * from parquet_metadata('tests/data/misc.parquet');

.exit

select * from parquet_column_chunks('yellow_tripdata_2022-01.parquet');


select * from parquet_column_chunks('train-0of7.parquet')
;--limit 200;
.exit

create virtual table numbers using parquet(filename='tests/data/numbers.parquet');
select * from numbers;

create virtual table json using parquet(filename='tests/data/json.parquet');
select * from json;

create virtual table misc using parquet(filename='tests/data/misc.parquet');
select * from misc;

create virtual table dates using parquet(filename='tests/data/dates.parquet');
select * from dates;


create virtual table duck using parquet(filename='tests/data/duck.parquet');
select * from duck;

.exit

create virtual table train using parquet(filename='train-00000-of-00007-29aec9150af50f9f.parquet');

select URL, hash from train limit 20;

select count(1) from train;


.exit


create virtual table t using parquet(filename='yellow_tripdata_2022-01.parquet');

select 
  VendorID, --trip_distance, store_and_fwd_flag,
  tpep_pickup_datetime,
  total_amount
from t limit 10;
--select distinct VendorID from t;

--select count(*) from t;