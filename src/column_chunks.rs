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
        // TODO db.config(VTabConfig::Innocuous)?;
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
                _ => todo!(),
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
        let path = api::value_text(values.get(0).unwrap())?;
        println!("{path}");
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let metadata = reader.metadata();
        self.metadata = Some(metadata.to_owned());
        self.eof = false;
        self.column_idx = 0;
        self.row_group_idx = 0;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        let curr_rowgroup = self
            .metadata
            .as_ref()
            .unwrap()
            .row_group(self.row_group_idx);
        self.column_idx += 1;
        if self.column_idx >= curr_rowgroup.columns().len() {
            self.column_idx = 0;
            self.row_group_idx += 1;
            if self.row_group_idx >= self.metadata.as_ref().unwrap().num_row_groups() {
                self.eof = true
            }
        }
        //self.eof = self.current >= self.records.as_ref().unwrap().len();
        Ok(())
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let row_group = self
            .metadata
            .as_ref()
            .unwrap()
            .row_group(self.row_group_idx);
        let column_chunk = row_group.column(self.column_idx);

        // row_group.compressed_size()
        // row_group.num_columns()
        // row_group.num_rows()

        //self.metadata.as_ref().unwrap().num_row_groups()
        //self.metadata.as_ref().unwrap().file_metadata().version()
        //self.metadata.as_ref().unwrap().file_metadata().created_by()
        //self.metadata.as_ref().unwrap().file_metadata().num_rows()
        //self.metadata.as_ref().unwrap().file_metadata().schema()
        //self.metadata.as_ref().unwrap().file_metadata().schema().
        match column(i) {
            Some(Columns::RowGroup) => {
                api::result_int(context, self.row_group_idx.try_into().unwrap());
            }
            Some(Columns::Source) => (),
            Some(Columns::ColumnName) => {
                api::result_text(context, column_chunk.column_path().to_string().as_str())?;
            }
            Some(Columns::ColumnType) => {
                api::result_text(context, column_chunk.column_type().to_string().as_str())?;
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
                        Statistics::Int32(ref value) => api::result_int(context, *value.min()),
                        Statistics::Int64(ref value) => api::result_int64(context, *value.min()),
                        Statistics::Double(ref value) => api::result_double(context, *value.min()),
                        Statistics::Float(ref value) => {
                            api::result_double(context, (*value.min()).into())
                        }
                        Statistics::ByteArray(ref value) => {
                            api::result_text(context, (*value.min()).as_utf8().unwrap())?
                        }
                        _ => (),
                    };
                }
            }
            Some(Columns::StatsMax) => {
                if let Some(stats) = column_chunk.statistics() {
                    match stats {
                        Statistics::Int32(ref value) => api::result_int(context, *value.max()),
                        Statistics::Int64(ref value) => api::result_int64(context, *value.max()),
                        Statistics::Double(ref value) => api::result_double(context, *value.max()),
                        Statistics::Float(ref value) => {
                            api::result_double(context, (*value.max()).into())
                        }
                        Statistics::ByteArray(ref value) => {
                            api::result_text(context, (*value.max()).as_utf8().unwrap())?
                        }
                        _ => (),
                    };
                }
            }
            Some(Columns::StatsDistinct) => {
                if let Some(stats) = column_chunk.statistics() {
                    if let Some(distinct) = stats.distinct_count() {
                        api::result_int64(context, distinct.try_into().unwrap());
                    }
                }
            }
            Some(Columns::StatsNullCount) => {
                if let Some(stats) = column_chunk.statistics() {
                    api::result_int64(context, stats.null_count().try_into().unwrap());
                }
            }
            None => todo!(),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}
