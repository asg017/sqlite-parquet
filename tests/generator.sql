COPY (
  select * 
  from (
    values
    ((1), ('alex'), .1, get_current_timestamp()	, DATE '1999-12-31', /*INTERVAL 1 HOUR*/ ),
    ((2), ('brian'), .2, get_current_timestamp(), DATE '2000-01-01', /*INTERVAL 1 HOUR*/ ),
  )
) TO 'tests/data/duck.parquet' (FORMAT 'parquet');
