//! `parquet_column_chunks(path_or_glob)`: one row per (file, row group, column chunk).

use parquet::file::{metadata::ParquetMetaData, statistics::Statistics};
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};

use std::{mem, os::raw::c_int, path::PathBuf};

use crate::metadata::{matching_files, read_metadata, source_argument, source_best_index};

static CREATE_SQL: &str = "CREATE TABLE x(
      source hidden,
      path text,
      row_group integer,
      column_name text,
      column_type text,
      compression text,
      encodings text,
      num_values integer,
      compressed_size integer,
      uncompressed_size integer,
      stats_min,
      stats_max,
      stats_distinct integer,
      stats_null_count integer
    )";
enum Columns {
    Source,
    Path,
    RowGroup,
    ColumnName,
    ColumnType,
    Compression,
    Encodings,
    NumValues,
    CompressedSize,
    UncompressedSize,
    StatsMin,
    StatsMax,
    StatsDistinct,
    StatsNullCount,
}
fn column(index: i32) -> Option<Columns> {
    match index {
        0 => Some(Columns::Source),
        1 => Some(Columns::Path),
        2 => Some(Columns::RowGroup),
        3 => Some(Columns::ColumnName),
        4 => Some(Columns::ColumnType),
        5 => Some(Columns::Compression),
        6 => Some(Columns::Encodings),
        7 => Some(Columns::NumValues),
        8 => Some(Columns::CompressedSize),
        9 => Some(Columns::UncompressedSize),
        10 => Some(Columns::StatsMin),
        11 => Some(Columns::StatsMax),
        12 => Some(Columns::StatsDistinct),
        13 => Some(Columns::StatsNullCount),
        _ => None,
    }
}

#[repr(C)]
pub struct ColumnChunksTable {
    /// must be first
    base: sqlite3_vtab,
}

impl<'vtab> VTab<'vtab> for ColumnChunksTable {
    type Aux = ();
    type Cursor = ColumnChunksCursor;

    fn connect(
        _db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, ColumnChunksTable)> {
        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        Ok((CREATE_SQL.to_owned(), ColumnChunksTable { base }))
    }

    fn best_index(&self, info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        source_best_index(info, |i| matches!(column(i), Some(Columns::Source)))
    }

    fn open(&mut self) -> Result<ColumnChunksCursor> {
        Ok(ColumnChunksCursor {
            base: unsafe { mem::zeroed() },
            files: Vec::new(),
            file_idx: 0,
            metadata: None,
            row_group_idx: 0,
            column_idx: 0,
            rowid: 0,
        })
    }
}

#[repr(C)]
pub struct ColumnChunksCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    files: Vec<PathBuf>,
    file_idx: usize,
    /// Metadata of `files[file_idx]`; `None` once past the last file.
    metadata: Option<ParquetMetaData>,
    row_group_idx: usize,
    column_idx: usize,
    rowid: i64,
}

impl ColumnChunksCursor {
    /// Loads the next file that has at least one column chunk.
    fn load_next_file(&mut self) -> Result<()> {
        while let Some(path) = self.files.get(self.file_idx) {
            let metadata = read_metadata(path)?;
            self.row_group_idx = 0;
            self.column_idx = 0;
            if metadata.row_groups().iter().any(|rg| rg.num_columns() > 0) {
                // Skip leading row groups with no columns (shouldn't happen, but be safe).
                while metadata.row_group(self.row_group_idx).num_columns() == 0 {
                    self.row_group_idx += 1;
                }
                self.metadata = Some(metadata);
                return Ok(());
            }
            self.file_idx += 1;
        }
        self.metadata = None;
        Ok(())
    }
}

impl VTabCursor for ColumnChunksCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        self.files = matching_files(source_argument(values)?)?;
        self.file_idx = 0;
        self.rowid = 1;
        self.load_next_file()
    }

    fn next(&mut self) -> Result<()> {
        let Some(metadata) = self.metadata.as_ref() else {
            return Ok(());
        };
        self.rowid += 1;
        self.column_idx += 1;
        if self.column_idx < metadata.row_group(self.row_group_idx).num_columns() {
            return Ok(());
        }
        self.column_idx = 0;
        self.row_group_idx += 1;
        while self.row_group_idx < metadata.num_row_groups() {
            if metadata.row_group(self.row_group_idx).num_columns() > 0 {
                return Ok(());
            }
            self.row_group_idx += 1;
        }
        self.file_idx += 1;
        self.load_next_file()
    }

    fn eof(&self) -> bool {
        self.metadata.is_none()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let metadata = self
            .metadata
            .as_ref()
            .ok_or_else(|| Error::new_message("no metadata available"))?;
        let chunk = metadata
            .row_group(self.row_group_idx)
            .column(self.column_idx);

        match column(i) {
            Some(Columns::Source) => (),
            Some(Columns::Path) => {
                api::result_text(context, self.files[self.file_idx].to_string_lossy())?;
            }
            Some(Columns::RowGroup) => api::result_int64(context, self.row_group_idx as i64),
            Some(Columns::ColumnName) => {
                api::result_text(context, chunk.column_path().string())?;
            }
            Some(Columns::ColumnType) => {
                api::result_text(context, chunk.column_type().to_string())?;
            }
            Some(Columns::Compression) => {
                // Display includes the level, e.g. `GZIP(GzipLevel(6))`; keep the codec name.
                let codec = chunk.compression().to_string();
                api::result_text(context, codec.split('(').next().unwrap_or(&codec))?;
            }
            Some(Columns::Encodings) => {
                let encodings: Vec<String> = chunk.encodings().map(|e| e.to_string()).collect();
                api::result_text(context, encodings.join(","))?;
            }
            Some(Columns::NumValues) => api::result_int64(context, chunk.num_values()),
            Some(Columns::CompressedSize) => api::result_int64(context, chunk.compressed_size()),
            Some(Columns::UncompressedSize) => {
                api::result_int64(context, chunk.uncompressed_size())
            }
            Some(Columns::StatsMin) => {
                if let Some(stats) = chunk.statistics() {
                    result_stat(context, stats, true)?;
                }
            }
            Some(Columns::StatsMax) => {
                if let Some(stats) = chunk.statistics() {
                    result_stat(context, stats, false)?;
                }
            }
            Some(Columns::StatsDistinct) => {
                if let Some(distinct) = chunk.statistics().and_then(|s| s.distinct_count_opt()) {
                    api::result_int64(context, distinct as i64);
                }
            }
            Some(Columns::StatsNullCount) => {
                if let Some(nulls) = chunk.statistics().and_then(|s| s.null_count_opt()) {
                    api::result_int64(context, nulls as i64);
                }
            }
            None => (),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

/// Writes the min (or max) statistic, leaving the result NULL when the
/// statistic is absent or of a type with no natural SQLite representation.
fn result_stat(context: *mut sqlite3_context, stats: &Statistics, min: bool) -> Result<()> {
    macro_rules! pick {
        ($v:expr) => {
            if min {
                $v.min_opt()
            } else {
                $v.max_opt()
            }
        };
    }
    match stats {
        Statistics::Boolean(v) => {
            if let Some(b) = pick!(v) {
                api::result_bool(context, *b);
            }
        }
        Statistics::Int32(v) => {
            if let Some(n) = pick!(v) {
                api::result_int(context, *n);
            }
        }
        Statistics::Int64(v) => {
            if let Some(n) = pick!(v) {
                api::result_int64(context, *n);
            }
        }
        Statistics::Float(v) => {
            if let Some(n) = pick!(v) {
                api::result_double(context, f64::from(*n));
            }
        }
        Statistics::Double(v) => {
            if let Some(n) = pick!(v) {
                api::result_double(context, *n);
            }
        }
        Statistics::ByteArray(v) => {
            if let Some(bytes) = pick!(v) {
                match bytes.as_utf8() {
                    Ok(s) => api::result_text(context, s)?,
                    Err(_) => api::result_blob(context, bytes.data()),
                }
            }
        }
        Statistics::FixedLenByteArray(v) => {
            if let Some(bytes) = pick!(v) {
                api::result_blob(context, bytes.data());
            }
        }
        // Int96 (legacy timestamps) has no useful ordering; leave NULL.
        Statistics::Int96(_) => (),
    }
    Ok(())
}
