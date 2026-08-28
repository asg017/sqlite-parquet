use parquet::file::{
    metadata::ParquetMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
    statistics::Statistics,
};
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};

use std::{fs::File, mem, os::raw::c_int};

static CREATE_SQL: &str = "CREATE TABLE x(
      source hidden,
      row_group integer,
      column_name text,
      column_type text,
      num_values int,
      compressed_size int,
      uncompressed_size int,
      stats_min,
      stats_max,
      stats_distinct,
      stats_null_count
    )";
enum Columns {
    Source,
    RowGroup,
    ColumnName,
    ColumnType,
    Values,
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
        1 => Some(Columns::RowGroup),
        2 => Some(Columns::ColumnName),
        3 => Some(Columns::ColumnType),
        4 => Some(Columns::Values),
        5 => Some(Columns::CompressedSize),
        6 => Some(Columns::UncompressedSize),
        7 => Some(Columns::StatsMin),
        8 => Some(Columns::StatsMax),
        9 => Some(Columns::StatsDistinct),
        10 => Some(Columns::StatsNullCount),
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
        let vtab = ColumnChunksTable { base };
        Ok((CREATE_SQL.to_owned(), vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_source = false;
        for mut constraint in info.constraints() {
            match column(constraint.column_idx()) {
                Some(Columns::Source) => {
                    if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                        constraint.set_omit(true);
                        constraint.set_argv_index(1);
                        has_source = true;
                    } else {
                        return Err(BestIndexError::Constraint);
                    }
                }
                _ => (),
            }
        }
        if !has_source {
            return Err(BestIndexError::Error);
        }
        info.set_estimated_cost(100000.0);
        info.set_estimated_rows(100000);
        info.set_idxnum(1);

        Ok(())
    }

    fn open(&mut self) -> Result<ColumnChunksCursor> {
        Ok(ColumnChunksCursor::new())
    }
}

#[repr(C)]
pub struct ColumnChunksCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    metadata: Option<ParquetMetaData>,
    row_group_idx: usize,
    column_idx: usize,
    eof: bool,
}
impl ColumnChunksCursor {
    fn new() -> ColumnChunksCursor {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        ColumnChunksCursor {
            base,
            metadata: None,
            row_group_idx: 0,
            column_idx: 0,
            eof: false,
        }
    }
}

impl VTabCursor for ColumnChunksCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let path = api::value_text(values.get(0).ok_or_else(|| {
            Error::new_message("missing source argument")
        })?)?;
        let file = File::open(&path).map_err(|e| {
            Error::new_message(&format!("failed to open file '{}': {}", path, e))
        })?;
        let reader = SerializedFileReader::new(file).map_err(|e| {
            Error::new_message(&format!("failed to read parquet file '{}': {}", path, e))
        })?;
        let metadata = reader.metadata();
        self.metadata = Some(metadata.to_owned());
        self.eof = false;
        self.column_idx = 0;
        self.row_group_idx = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            Error::new_message("no metadata available")
        })?;
        let curr_rowgroup = metadata.row_group(self.row_group_idx);
        self.column_idx += 1;
        if self.column_idx >= curr_rowgroup.columns().len() {
            self.column_idx = 0;
            self.row_group_idx += 1;
            if self.row_group_idx >= metadata.num_row_groups() {
                self.eof = true
            }
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            Error::new_message("no metadata available")
        })?;
        let row_group = metadata.row_group(self.row_group_idx);
        let column_chunk = row_group.column(self.column_idx);

        match column(i) {
            Some(Columns::RowGroup) => {
                api::result_int(context, self.row_group_idx as i32);
            }
            Some(Columns::Source) => (),
            Some(Columns::ColumnName) => {
                api::result_text(context, &column_chunk.column_path().to_string())?;
            }
            Some(Columns::ColumnType) => {
                api::result_text(context, &column_chunk.column_type().to_string())?;
            }
            Some(Columns::Values) => {
                api::result_int64(context, column_chunk.num_values());
            }
            Some(Columns::CompressedSize) => {
                api::result_int64(context, column_chunk.compressed_size());
            }
            Some(Columns::UncompressedSize) => {
                api::result_int64(context, column_chunk.uncompressed_size());
            }
            Some(Columns::StatsMin) => {
                if let Some(stats) = column_chunk.statistics() {
                    match stats {
                        Statistics::Int32(ref value) => {
                            if let Some(min) = value.min_opt() {
                                api::result_int(context, *min);
                            }
                        }
                        Statistics::Int64(ref value) => {
                            if let Some(min) = value.min_opt() {
                                api::result_int64(context, *min);
                            }
                        }
                        Statistics::Double(ref value) => {
                            if let Some(min) = value.min_opt() {
                                api::result_double(context, *min);
                            }
                        }
                        Statistics::Float(ref value) => {
                            if let Some(min) = value.min_opt() {
                                api::result_double(context, (*min).into());
                            }
                        }
                        Statistics::ByteArray(ref value) => {
                            if let Some(min) = value.min_opt() {
                                if let Ok(s) = min.as_utf8() {
                                    api::result_text(context, s)?;
                                }
                            }
                        }
                        _ => (),
                    };
                }
            }
            Some(Columns::StatsMax) => {
                if let Some(stats) = column_chunk.statistics() {
                    match stats {
                        Statistics::Int32(ref value) => {
                            if let Some(max) = value.max_opt() {
                                api::result_int(context, *max);
                            }
                        }
                        Statistics::Int64(ref value) => {
                            if let Some(max) = value.max_opt() {
                                api::result_int64(context, *max);
                            }
                        }
                        Statistics::Double(ref value) => {
                            if let Some(max) = value.max_opt() {
                                api::result_double(context, *max);
                            }
                        }
                        Statistics::Float(ref value) => {
                            if let Some(max) = value.max_opt() {
                                api::result_double(context, (*max).into());
                            }
                        }
                        Statistics::ByteArray(ref value) => {
                            if let Some(max) = value.max_opt() {
                                if let Ok(s) = max.as_utf8() {
                                    api::result_text(context, s)?;
                                }
                            }
                        }
                        _ => (),
                    };
                }
            }
            Some(Columns::StatsDistinct) => {
                if let Some(stats) = column_chunk.statistics() {
                    if let Some(distinct) = stats.distinct_count_opt() {
                        api::result_int64(context, distinct as i64);
                    }
                }
            }
            Some(Columns::StatsNullCount) => {
                if let Some(stats) = column_chunk.statistics() {
                    if let Some(null_count) = stats.null_count_opt() {
                        api::result_int64(context, null_count as i64);
                    }
                }
            }
            None => (),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}
