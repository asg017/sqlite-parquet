use parquet::file::{
    metadata::ParquetMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
    statistics::Statistics,
};
use sqlite3_loadable::{
    errors::{BestIndexError, Result},
    table::{ConstraintOperator, SqliteXIndexInfo, VTab, VTabCursor, VTableArguments},
    SqliteContext, SqliteValue,
};
use sqlite3ext_sys::{sqlite3, sqlite3_vtab, sqlite3_vtab_cursor};

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

unsafe impl<'vtab> VTab<'vtab> for ColumnChunksTable {
    type Aux = ();
    type Cursor = ColumnChunksCursor;

    fn connect(
        _db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTableArguments,
    ) -> Result<(String, ColumnChunksTable)> {
        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        let vtab = ColumnChunksTable { base };
        // TODO db.config(VTabConfig::Innocuous)?;
        Ok((CREATE_SQL.to_owned(), vtab))
    }
    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: SqliteXIndexInfo) -> core::result::Result<(), BestIndexError> {
        let mut has_source = false;
        for mut constraint in info.constraints() {
            match column(constraint.icolumn()) {
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
    fn new<'vtab>() -> ColumnChunksCursor {
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

unsafe impl VTabCursor for ColumnChunksCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: Vec<SqliteValue>,
    ) -> Result<()> {
        let path = values.get(0).unwrap().text()?;
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

    fn column(&self, ctx: SqliteContext, i: c_int) -> Result<()> {
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
                ctx.result_int(self.row_group_idx.try_into().unwrap());
            }
            Some(Columns::Source) => (),
            Some(Columns::ColumnName) => {
                ctx.result_text(column_chunk.column_path().to_string().as_str())?;
            }
            Some(Columns::ColumnType) => {
                ctx.result_text(column_chunk.column_type().to_string().as_str())?;
            }
            Some(Columns::Values) => {
                ctx.result_int64(column_chunk.num_values());
            }
            Some(Columns::CompressedSize) => {
                ctx.result_int64(column_chunk.compressed_size());
            }
            Some(Columns::UncompressedSize) => {
                ctx.result_int64(column_chunk.uncompressed_size());
            }
            Some(Columns::StatsMin) => {
                if let Some(stats) = column_chunk.statistics() {
                    match stats {
                        Statistics::Int32(ref value) => ctx.result_int(*value.min()),
                        Statistics::Int64(ref value) => ctx.result_int64(*value.min()),
                        Statistics::Double(ref value) => ctx.result_double(*value.min()),
                        Statistics::Float(ref value) => ctx.result_double((*value.min()).into()),
                        Statistics::ByteArray(ref value) => {
                            ctx.result_text((*value.min()).as_utf8().unwrap())?
                        }
                        _ => (),
                    };
                }
            }
            Some(Columns::StatsMax) => {
                if let Some(stats) = column_chunk.statistics() {
                    match stats {
                        Statistics::Int32(ref value) => ctx.result_int(*value.max()),
                        Statistics::Int64(ref value) => ctx.result_int64(*value.max()),
                        Statistics::Double(ref value) => ctx.result_double(*value.max()),
                        Statistics::Float(ref value) => ctx.result_double((*value.max()).into()),
                        Statistics::ByteArray(ref value) => {
                            ctx.result_text((*value.max()).as_utf8().unwrap())?
                        }
                        _ => (),
                    };
                }
            }
            Some(Columns::StatsDistinct) => {
                if let Some(stats) = column_chunk.statistics() {
                    if let Some(distinct) = stats.distinct_count() {
                        ctx.result_int64(distinct.try_into().unwrap());
                    }
                }
            }
            Some(Columns::StatsNullCount) => {
                if let Some(stats) = column_chunk.statistics() {
                    ctx.result_int64(stats.null_count().try_into().unwrap());
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
