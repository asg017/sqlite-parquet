#!/bin/bash

hyperfine --warmup 5 \
  'duckdb :memory: "select * from \"yellow_tripdata_2022-01.parquet\" limit 1e3"' \
  'sqlite3x :memory: ".load target/release/libparquet0" "create virtual table t using parquet(filename=\"yellow_tripdata_2022-01.parquet\")" "select * from t limit 1e3" '
  
#hyperfine --warmup 5 'sqlite3x :memory: ".load target/release/libparquet0" "create virtual table t using parquet(filename=\"yellow_tripdata_2022-01.parquet\")" "select count(*) from t" '