use parquet::file::{
    metadata::ParquetMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
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
    version text,
    created_by text,
    schema text,
    num_rows integer,
    num_columns integer,
    num_row_groups integer
  )";
enum Columns {
    Source,
    Version,
    CreatedBy,
    Schema,
    NumRows,
    NumColumns,
    NumRowGroups,
}
fn column(index: i32) -> Option<Columns> {
    match index {
        0 => Some(Columns::Source),
        1 => Some(Columns::Version),
        2 => Some(Columns::CreatedBy),
        3 => Some(Columns::Schema),
        4 => Some(Columns::NumRows),
        5 => Some(Columns::NumColumns),
        6 => Some(Columns::NumRowGroups),
        _ => None,
    }
}

#[repr(C)]
pub struct MetadataTable {
    /// must be first
    base: sqlite3_vtab,
}

impl<'vtab> VTab<'vtab> for MetadataTable {
    type Aux = ();
    type Cursor = MetadataCursor;

    fn connect(
        _db: *mut sqlite3,
        _aux: Option<&Self::Aux>,
        _args: VTabArguments,
    ) -> Result<(String, MetadataTable)> {
        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        let vtab = MetadataTable { base };
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

    fn open(&mut self) -> Result<MetadataCursor> {
        Ok(MetadataCursor::new())
    }
}

#[repr(C)]
pub struct MetadataCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    metadata: Option<ParquetMetaData>,
    done: bool,
}
impl MetadataCursor {
    fn new() -> MetadataCursor {
        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        MetadataCursor {
            base,
            metadata: None,
            done: false,
        }
    }
}

impl VTabCursor for MetadataCursor {
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
        self.done = false;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.done = true;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.done
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let metadata = self.metadata.as_ref().ok_or_else(|| {
            Error::new_message("no metadata available")
        })?;

        match column(i) {
            Some(Columns::Source) => (),
            Some(Columns::Version) => {
                api::result_int(context, metadata.file_metadata().version());
            }
            Some(Columns::CreatedBy) => {
                if let Some(created_by) = metadata.file_metadata().created_by() {
                    api::result_text(context, created_by)?;
                }
            }
            Some(Columns::Schema) => {
                let mut buf = Vec::new();
                parquet::schema::printer::print_schema(&mut buf, metadata.file_metadata().schema());
                let s = String::from_utf8(buf).map_err(|e| {
                    Error::new_message(&format!("invalid utf8 in schema: {}", e))
                })?;
                api::result_text(context, &s)?;
            }
            Some(Columns::NumRows) => {
                api::result_int64(context, metadata.file_metadata().num_rows());
            }
            Some(Columns::NumColumns) => {
                api::result_int64(
                    context,
                    metadata
                        .file_metadata()
                        .schema_descr()
                        .num_columns() as i64,
                );
            }
            Some(Columns::NumRowGroups) => {
                api::result_int64(context, metadata.num_row_groups() as i64);
            }
            None => (),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}
