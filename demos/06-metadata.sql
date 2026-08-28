-- 06-metadata.sql: Inspect parquet file metadata without reading all data
-- Run: solite-dev run demos/06-metadata.sql

.load dist/release/parquet0

-- File-level metadata
select * from parquet_metadata('fhvhv_tripdata_2026-01.parquet');

-- Column-level statistics for row group 0 (min/max values, compression, etc.)
select
  column_name,
  column_type,
  num_values,
  stats_min,
  stats_max,
  compressed_size,
  uncompressed_size
from parquet_column_chunks('fhvhv_tripdata_2026-01.parquet')
where row_group = 0;
