use parquet::{file::{
  metadata::ParquetMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
}, schema::printer};
use sqlite3_loadable::{
  errors::{BestIndexError, Result},
  table::{ConstraintOperator, SqliteXIndexInfo, VTab, VTabCursor, VTableArguments},
  SqliteContext, SqliteValue,
};
use sqlite3ext_sys::{sqlite3, sqlite3_vtab, sqlite3_vtab_cursor};

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
  NumRowGroups
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

unsafe impl<'vtab> VTab<'vtab> for MetadataTable {
  type Aux = ();
  type Cursor = MetadataCursor;

  fn connect(
      _db: *mut sqlite3,
      _aux: Option<&Self::Aux>,
      _args: VTableArguments,
  ) -> Result<(String, MetadataTable)> {
      let base: sqlite3_vtab = unsafe { mem::zeroed() };
      let vtab = MetadataTable { base };
      // TODO db.config(VTabConfig::Innocuous)?;
      Ok((CREATE_SQL.to_owned(), vtab))
  }
  fn destroy(&self) -> Result<()> {
      Ok(())
  }

  fn best_index(&self, mut info: SqliteXIndexInfo) -> core::result::Result<(), BestIndexError> {
      let mut has_source = false;
      for mut constraint in info.constraints() {
        //println!("{} {}", constraint.icolumn(), constraint.usable());
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
  fn new<'vtab>() -> MetadataCursor {
      let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
      MetadataCursor {
          base,
          metadata: None,
          done: false,
      }
  }
}

unsafe impl VTabCursor for MetadataCursor {
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

  fn column(&self, ctx: SqliteContext, i: c_int) -> Result<()> {
      let metadata = self
          .metadata
          .as_ref()
          .unwrap();


      match column(i) {
          Some(Columns::Source) => (),
          Some(Columns::Version) => {
            ctx.result_int(metadata.file_metadata().version());
          }
          Some(Columns::CreatedBy) => {
            if let Some(created_by) = metadata.file_metadata().created_by() {
              ctx.result_text(created_by)?;
            }
          }
          Some(Columns::Schema) => {
              let schema = metadata.file_metadata().schema();
              let mut buf = Vec::new();
              printer::print_schema(&mut buf, &schema);
              ctx.result_text(String::from_utf8(buf).unwrap().as_str())?;
          }
          Some(Columns::NumRows) => {
            ctx.result_int64(metadata.file_metadata().num_rows());
          }
          Some(Columns::NumColumns) => {
            ctx.result_int64(metadata.file_metadata().schema_descr().num_columns().try_into().unwrap());
          }
          Some(Columns::NumRowGroups) => {
            ctx.result_int64(metadata.num_row_groups().try_into().unwrap());
          }
        
          None => todo!(),
      }
      Ok(())
  }

  fn rowid(&self) -> Result<i64> {
      Ok(1)
  }
}
