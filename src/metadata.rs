//! `parquet_metadata(path_or_glob)`: one row of file-level metadata per matching file.

use parquet::file::{
    metadata::ParquetMetaData, reader::FileReader, serialized_reader::SerializedFileReader,
};
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};

use std::{fs::File, mem, os::raw::c_int, path::PathBuf};

static CREATE_SQL: &str = "CREATE TABLE x(
    source hidden,
    path text,
    version integer,
    created_by text,
    num_rows integer,
    num_columns integer,
    num_row_groups integer,
    schema text,
    key_value_metadata text
  )";
#[allow(clippy::enum_variant_names)]
enum Columns {
    Source,
    Path,
    Version,
    CreatedBy,
    NumRows,
    NumColumns,
    NumRowGroups,
    Schema,
    KeyValueMetadata,
}
fn column(index: i32) -> Option<Columns> {
    match index {
        0 => Some(Columns::Source),
        1 => Some(Columns::Path),
        2 => Some(Columns::Version),
        3 => Some(Columns::CreatedBy),
        4 => Some(Columns::NumRows),
        5 => Some(Columns::NumColumns),
        6 => Some(Columns::NumRowGroups),
        7 => Some(Columns::Schema),
        8 => Some(Columns::KeyValueMetadata),
        _ => None,
    }
}

/// Expands a path or glob into the list of matching files, erroring when
/// nothing matches. Shared by the metadata table functions.
pub(crate) fn matching_files(pattern: &str) -> Result<Vec<PathBuf>> {
    let paths = glob::glob(pattern)
        .map_err(|e| Error::new_message(format!("invalid glob pattern '{}': {}", pattern, e)))?;
    let mut files = Vec::new();
    for entry in paths {
        let path = entry.map_err(|e| Error::new_message(format!("glob error: {}", e)))?;
        if path.is_file() {
            files.push(path);
        }
    }
    if files.is_empty() {
        return Err(Error::new_message(format!(
            "no files matched '{}'",
            pattern
        )));
    }
    Ok(files)
}

/// Reads only the footer of a parquet file.
pub(crate) fn read_metadata(path: &std::path::Path) -> Result<ParquetMetaData> {
    let display = path.display();
    let file = File::open(path)
        .map_err(|e| Error::new_message(format!("failed to open file '{}': {}", display, e)))?;
    let reader = SerializedFileReader::new(file).map_err(|e| {
        Error::new_message(format!("failed to read parquet file '{}': {}", display, e))
    })?;
    Ok(reader.metadata().clone())
}

/// `best_index` for table functions whose only argument is the hidden `source` column.
pub(crate) fn source_best_index(
    mut info: IndexInfo,
    is_source: impl Fn(i32) -> bool,
) -> core::result::Result<(), BestIndexError> {
    let mut has_source = false;
    for mut constraint in info.constraints() {
        if is_source(constraint.column_idx()) {
            if constraint.usable() && constraint.op() == Some(ConstraintOperator::EQ) {
                constraint.set_omit(true);
                constraint.set_argv_index(1);
                has_source = true;
            } else {
                return Err(BestIndexError::Constraint);
            }
        }
    }
    if !has_source {
        return Err(BestIndexError::Error);
    }
    info.set_estimated_cost(100.0);
    info.set_estimated_rows(1);
    info.set_idxnum(1);
    Ok(())
}

pub(crate) fn source_argument(values: &[*mut sqlite3_value]) -> Result<&str> {
    let value = values
        .first()
        .ok_or_else(|| Error::new_message("missing path argument"))?;
    if api::value_is_null(value) {
        return Err(Error::new_message("path argument must not be NULL"));
    }
    Ok(api::value_text(value)?)
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
        Ok((CREATE_SQL.to_owned(), MetadataTable { base }))
    }

    fn best_index(&self, info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        source_best_index(info, |i| matches!(column(i), Some(Columns::Source)))
    }

    fn open(&mut self) -> Result<MetadataCursor> {
        Ok(MetadataCursor {
            base: unsafe { mem::zeroed() },
            files: Vec::new(),
            idx: 0,
            current: None,
        })
    }
}

#[repr(C)]
pub struct MetadataCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    files: Vec<PathBuf>,
    idx: usize,
    current: Option<ParquetMetaData>,
}

impl MetadataCursor {
    fn load_current(&mut self) -> Result<()> {
        self.current = match self.files.get(self.idx) {
            Some(path) => Some(read_metadata(path)?),
            None => None,
        };
        Ok(())
    }
}

impl VTabCursor for MetadataCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        values: &[*mut sqlite3_value],
    ) -> Result<()> {
        self.files = matching_files(source_argument(values)?)?;
        self.idx = 0;
        self.load_current()
    }

    fn next(&mut self) -> Result<()> {
        self.idx += 1;
        self.load_current()
    }

    fn eof(&self) -> bool {
        self.idx >= self.files.len()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let metadata = self
            .current
            .as_ref()
            .ok_or_else(|| Error::new_message("no metadata available"))?;
        let file_metadata = metadata.file_metadata();

        match column(i) {
            Some(Columns::Source) => (),
            Some(Columns::Path) => {
                api::result_text(context, self.files[self.idx].to_string_lossy())?;
            }
            Some(Columns::Version) => api::result_int(context, file_metadata.version()),
            Some(Columns::CreatedBy) => {
                if let Some(created_by) = file_metadata.created_by() {
                    api::result_text(context, created_by)?;
                }
            }
            Some(Columns::NumRows) => api::result_int64(context, file_metadata.num_rows()),
            Some(Columns::NumColumns) => {
                api::result_int64(context, file_metadata.schema_descr().num_columns() as i64)
            }
            Some(Columns::NumRowGroups) => {
                api::result_int64(context, metadata.num_row_groups() as i64)
            }
            Some(Columns::Schema) => {
                let mut buf = Vec::new();
                parquet::schema::printer::print_schema(&mut buf, file_metadata.schema());
                let s = String::from_utf8(buf)
                    .map_err(|e| Error::new_message(format!("invalid utf8 in schema: {}", e)))?;
                api::result_text(context, &s)?;
            }
            Some(Columns::KeyValueMetadata) => {
                if let Some(kvs) = file_metadata.key_value_metadata() {
                    let mut map = serde_json::Map::with_capacity(kvs.len());
                    for kv in kvs {
                        map.insert(
                            kv.key.clone(),
                            kv.value
                                .clone()
                                .map_or(serde_json::Value::Null, serde_json::Value::String),
                        );
                    }
                    api::result_text(context, serde_json::Value::Object(map).to_string())?;
                }
            }
            None => (),
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.idx as i64 + 1)
    }
}
