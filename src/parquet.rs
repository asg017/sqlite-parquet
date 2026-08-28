//! The `parquet` / `parquet0` virtual table: `CREATE VIRTUAL TABLE t USING parquet(...)`.
//!
//! Reads one or more parquet files (a path or a glob) through the Arrow
//! `RecordBatch` reader and exposes each top-level column as a SQLite column.

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Date32Array, Date64Array, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, MapArray, StructArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, DecimalType, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate};
use glob::Paths;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use sqlite_loadable::prelude::*;
use sqlite_loadable::vtab_argparse::{parse_argument, Argument, ConfigOptionValue};
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};

use std::{fs::File, mem, os::raw::c_int, sync::Arc};

/// Name of the hidden column that reports which file the current row came from.
/// Skipped when the parquet file itself has a column with this name.
const PATH_COLUMN: &str = "parquet_path";

#[repr(C)]
pub struct ParquetTable {
    /// must be first
    base: sqlite3_vtab,
    /// Path or glob pattern, as given by the user.
    pattern: String,
    /// Visible column names, in declaration order (from the first matching file).
    columns: Arc<Vec<String>>,
    /// SQLite column index of the hidden `parquet_path` column, if present.
    path_col: Option<usize>,
    /// Row count of the first matching file, used as the planner estimate.
    estimated_rows: i64,
}

/// Best-effort SQLite declared type for an Arrow type, so `pragma table_info`
/// and `CREATE TABLE ... AS SELECT` carry something useful.
fn declared_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "integer",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "real",
        DataType::Decimal32(_, s) | DataType::Decimal64(_, s) => {
            if *s == 0 {
                "integer"
            } else {
                "real"
            }
        }
        DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => {
            if *s == 0 {
                "integer"
            } else {
                "real"
            }
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "blob",
        DataType::Null => "",
        DataType::Dictionary(_, value) => declared_type(value),
        _ => "text",
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn open_builder(path: &str) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let file = File::open(path)
        .map_err(|e| Error::new_message(format!("failed to open file '{}': {}", path, e)))?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| Error::new_message(format!("failed to read parquet file '{}': {}", path, e)))
}

fn glob_paths(pattern: &str) -> Result<Paths> {
    glob::glob(pattern)
        .map_err(|e| Error::new_message(format!("invalid glob pattern '{}': {}", pattern, e)))
}

fn first_glob_match(pattern: &str) -> Result<String> {
    match glob_paths(pattern)?.next() {
        Some(Ok(path)) => Ok(path.to_string_lossy().into_owned()),
        Some(Err(e)) => Err(Error::new_message(format!("glob error: {}", e))),
        None => Err(Error::new_message(format!(
            "no files matched '{}'. Use filename='path-or-glob', parquet('path-or-glob'), \
             or a file path as the table name.",
            pattern
        ))),
    }
}

/// Parses the `CREATE VIRTUAL TABLE ... USING parquet(...)` arguments.
/// Accepts `filename=...`, a single positional (quoted or bare) path, or
/// neither, in which case the table name is used as the path.
fn parse_pattern(args: &VTabArguments) -> Result<String> {
    let mut pattern: Option<String> = None;
    let mut set = |value: String| -> Result<()> {
        if pattern.is_some() {
            return Err(Error::new_message("filename was given more than once"));
        }
        pattern = Some(value);
        Ok(())
    };

    for raw in &args.arguments {
        let arg = raw.trim();
        let is_quoted = |q: char| arg.len() >= 2 && arg.starts_with(q) && arg.ends_with(q);
        if is_quoted('\'') || is_quoted('"') {
            set(arg[1..arg.len() - 1].to_owned())?;
            continue;
        }
        match parse_argument(arg) {
            Ok(Argument::Config(config)) => {
                let key = config.key.trim();
                if key != "filename" {
                    return Err(Error::new_message(format!(
                        "unknown option '{}'. Only 'filename' is supported.",
                        key
                    )));
                }
                match config.value {
                    ConfigOptionValue::Quoted(v) | ConfigOptionValue::Bareword(v) => set(v)?,
                    ConfigOptionValue::SqliteParameter(_) => {
                        return Err(Error::new_message(
                            "SQL parameters are not supported for filename",
                        ))
                    }
                }
            }
            // A bare word with no type, e.g. `parquet(data/x.parquet)`.
            Ok(Argument::Column(col)) if col.declared_type.is_none() => set(col.name)?,
            Ok(Argument::Column(_)) => {
                return Err(Error::new_message(format!(
                    "column declarations are not supported: '{}'",
                    arg
                )))
            }
            Err(e) => {
                return Err(Error::new_message(format!(
                    "invalid argument '{}': {}",
                    arg, e
                )))
            }
        }
    }

    Ok(pattern.unwrap_or_else(|| args.table_name.clone()))
}

impl<'vtab> VTab<'vtab> for ParquetTable {
    type Aux = ();
    type Cursor = ParquetCursor;

    fn connect(
        _db: *mut sqlite3,
        _aux: Option<&()>,
        args: VTabArguments,
    ) -> Result<(String, ParquetTable)> {
        let pattern = parse_pattern(&args)?;
        let first_file = first_glob_match(&pattern)?;
        let builder = open_builder(&first_file)?;
        let estimated_rows = builder.metadata().file_metadata().num_rows();

        let mut columns = Vec::new();
        let mut decls = Vec::new();
        for field in builder.schema().fields() {
            columns.push(field.name().clone());
            decls.push(format!(
                "{} {}",
                quote_ident(field.name()),
                declared_type(field.data_type())
            ));
        }
        let path_col = if columns.iter().any(|c| c == PATH_COLUMN) {
            None
        } else {
            decls.push(format!("{} TEXT HIDDEN", quote_ident(PATH_COLUMN)));
            Some(columns.len())
        };
        let sql = format!("create table x({})", decls.join(", "));

        let vtab = ParquetTable {
            base: unsafe { mem::zeroed() },
            pattern,
            columns: Arc::new(columns),
            path_col,
            estimated_rows,
        };
        Ok((sql, vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    /// No constraints are pushed down (every WHERE is evaluated by SQLite),
    /// but the set of referenced columns is passed through `idxStr` so the
    /// cursor only decodes the parquet columns the statement actually reads.
    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        let used = info.columns_used();
        let needed: Vec<String> = (0..self.columns.len())
            .filter(|&i| used & (1u64 << i.min(63)) != 0)
            .map(|i| i.to_string())
            .collect();
        info.set_idxstr(&needed.join(","))
            .map_err(|_| BestIndexError::Error)?;
        info.set_idxnum(1);
        info.set_estimated_rows(self.estimated_rows.max(1));
        info.set_estimated_cost(self.estimated_rows.max(1) as f64);
        Ok(())
    }

    fn open(&mut self) -> Result<ParquetCursor> {
        Ok(ParquetCursor {
            base: unsafe { mem::zeroed() },
            pattern: self.pattern.clone(),
            columns: Arc::clone(&self.columns),
            path_col: self.path_col,
            needed: Vec::new(),
            paths: None,
            current_path: String::new(),
            reader: None,
            batch: None,
            batch_row: 0,
            col_map: Vec::new(),
            rowid: 0,
            eof: true,
        })
    }
}

#[repr(C)]
pub struct ParquetCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    pattern: String,
    columns: Arc<Vec<String>>,
    path_col: Option<usize>,
    /// SQLite column indexes the statement reads (set by `filter`).
    needed: Vec<usize>,
    paths: Option<Paths>,
    current_path: String,
    reader: Option<ParquetRecordBatchReader>,
    batch: Option<RecordBatch>,
    batch_row: usize,
    /// For each SQLite column, the index into the current batch's columns
    /// (`None` if the column was not projected).
    col_map: Vec<Option<usize>>,
    rowid: i64,
    eof: bool,
}

impl ParquetCursor {
    /// Opens the next file from the glob, projecting the needed columns by
    /// *name* so files with differently-ordered schemas still line up.
    fn next_file(&mut self) -> Result<bool> {
        let path = loop {
            match self.paths.as_mut().and_then(|p| p.next()) {
                Some(Ok(path)) => {
                    if path.is_file() {
                        break path;
                    }
                }
                Some(Err(e)) => return Err(Error::new_message(format!("glob error: {}", e))),
                None => return Ok(false),
            }
        };
        let path_str = path.to_string_lossy().into_owned();
        let builder = open_builder(&path_str)?;
        let schema = builder.schema();

        let mut roots: Vec<(usize, usize)> = Vec::new(); // (arrow field index, sqlite column)
        for &i in &self.needed {
            let name = &self.columns[i];
            let field_idx = schema.index_of(name).map_err(|_| {
                Error::new_message(format!(
                    "file '{}' does not have column '{}' (schema taken from first file matching '{}')",
                    path_str, name, self.pattern
                ))
            })?;
            roots.push((field_idx, i));
        }
        // A projection mask yields columns in schema order regardless of the
        // order the indexes were given in.
        roots.sort_unstable();
        self.col_map = vec![None; self.columns.len()];
        for (batch_idx, (_, sqlite_col)) in roots.iter().enumerate() {
            self.col_map[*sqlite_col] = Some(batch_idx);
        }
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots.iter().map(|r| r.0));
        let reader = builder.with_projection(mask).build().map_err(|e| {
            Error::new_message(format!("failed to build reader for '{}': {}", path_str, e))
        })?;

        self.current_path = path_str;
        self.reader = Some(reader);
        Ok(true)
    }

    fn next_batch(&mut self) -> Result<bool> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(false);
        };
        match reader.next() {
            Some(Ok(batch)) => {
                let batch = normalize_batch(batch)?;
                if batch.num_rows() == 0 {
                    // Skip empty batches rather than presenting a row with no data.
                    return self.next_batch();
                }
                self.batch = Some(batch);
                self.batch_row = 0;
                Ok(true)
            }
            Some(Err(e)) => Err(Error::new_message(format!(
                "error reading '{}': {}",
                self.current_path, e
            ))),
            None => Ok(false),
        }
    }

    fn advance(&mut self) -> Result<()> {
        if let Some(batch) = &self.batch {
            if self.batch_row + 1 < batch.num_rows() {
                self.batch_row += 1;
                self.rowid += 1;
                return Ok(());
            }
        }
        loop {
            if self.next_batch()? {
                self.rowid += 1;
                return Ok(());
            }
            if !self.next_file()? {
                self.eof = true;
                self.batch = None;
                self.reader = None;
                return Ok(());
            }
        }
    }
}

/// Dictionary- and run-end-encoded columns are decoded once per batch so the
/// per-cell code only deals with plain arrays.
fn normalize_batch(batch: RecordBatch) -> Result<RecordBatch> {
    let needs_cast = batch.columns().iter().any(|c| {
        matches!(
            c.data_type(),
            DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _)
        )
    });
    if !needs_cast {
        return Ok(batch);
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (field, col) in batch.schema().fields().iter().zip(batch.columns()) {
        let target = match col.data_type() {
            DataType::Dictionary(_, value) => Some(value.as_ref().clone()),
            DataType::RunEndEncoded(_, value) => Some(value.data_type().clone()),
            _ => None,
        };
        match target {
            Some(dt) => {
                let cast = arrow::compute::cast(col, &dt)
                    .map_err(|e| Error::new_message(format!("failed to decode column: {}", e)))?;
                fields.push(Arc::new(field.as_ref().clone().with_data_type(dt)));
                columns.push(cast);
            }
            None => {
                fields.push(Arc::clone(field));
                columns.push(Arc::clone(col));
            }
        }
    }
    let schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    RecordBatch::try_new(schema, columns)
        .map_err(|e| Error::new_message(format!("failed to decode batch: {}", e)))
}

impl VTabCursor for ParquetCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        idx_str: Option<&str>,
        _values: &[*mut sqlite3_value],
    ) -> Result<()> {
        self.needed = match idx_str {
            Some(s) if !s.is_empty() => s
                .split(',')
                .filter_map(|n| n.parse::<usize>().ok())
                .filter(|&i| i < self.columns.len())
                .collect(),
            Some(_) => Vec::new(),
            // No idxStr means best_index wasn't consulted; read everything.
            None => (0..self.columns.len()).collect(),
        };
        self.paths = Some(glob_paths(&self.pattern)?);
        self.reader = None;
        self.batch = None;
        self.batch_row = 0;
        self.rowid = 0;
        self.eof = false;
        self.advance()
    }

    fn next(&mut self) -> Result<()> {
        self.advance()
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let i = i as usize;
        if Some(i) == self.path_col {
            return api::result_text(context, &self.current_path);
        }
        let (Some(batch), Some(Some(batch_idx))) = (&self.batch, self.col_map.get(i)) else {
            api::result_null(context);
            return Ok(());
        };
        result_arrow_value(context, batch.column(*batch_idx).as_ref(), self.batch_row)
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

// ---------------------------------------------------------------------------
// Arrow value -> SQLite value
// ---------------------------------------------------------------------------

macro_rules! downcast {
    ($array:expr, $t:ty) => {
        $array.as_any().downcast_ref::<$t>().ok_or_else(|| {
            Error::new_message(concat!("unexpected array type, expected ", stringify!($t)))
        })?
    };
}

fn result_arrow_value(context: *mut sqlite3_context, col: &dyn Array, row: usize) -> Result<()> {
    if col.is_null(row) {
        api::result_null(context);
        return Ok(());
    }
    match col.data_type() {
        DataType::Null => api::result_null(context),
        DataType::Boolean => api::result_bool(context, downcast!(col, BooleanArray).value(row)),
        DataType::Int8 => api::result_int(context, downcast!(col, Int8Array).value(row) as i32),
        DataType::Int16 => api::result_int(context, downcast!(col, Int16Array).value(row) as i32),
        DataType::Int32 => api::result_int(context, downcast!(col, Int32Array).value(row)),
        DataType::Int64 => api::result_int64(context, downcast!(col, Int64Array).value(row)),
        DataType::UInt8 => api::result_int(context, downcast!(col, UInt8Array).value(row) as i32),
        DataType::UInt16 => api::result_int(context, downcast!(col, UInt16Array).value(row) as i32),
        DataType::UInt32 => {
            api::result_int64(context, downcast!(col, UInt32Array).value(row) as i64)
        }
        DataType::UInt64 => {
            let v = downcast!(col, UInt64Array).value(row);
            // SQLite has no unsigned 64-bit integer; degrade to a float past i64::MAX.
            match i64::try_from(v) {
                Ok(v) => api::result_int64(context, v),
                Err(_) => api::result_double(context, v as f64),
            }
        }
        DataType::Float16 => {
            api::result_double(context, f64::from(downcast!(col, Float16Array).value(row)))
        }
        DataType::Float32 => {
            api::result_double(context, downcast!(col, Float32Array).value(row) as f64)
        }
        DataType::Float64 => api::result_double(context, downcast!(col, Float64Array).value(row)),
        DataType::Utf8 => api::result_text(context, col.as_string::<i32>().value(row))?,
        DataType::LargeUtf8 => api::result_text(context, col.as_string::<i64>().value(row))?,
        DataType::Utf8View => api::result_text(context, col.as_string_view().value(row))?,
        DataType::Binary => api::result_blob(context, col.as_binary::<i32>().value(row)),
        DataType::LargeBinary => api::result_blob(context, col.as_binary::<i64>().value(row)),
        DataType::BinaryView => api::result_blob(context, col.as_binary_view().value(row)),
        DataType::FixedSizeBinary(_) => {
            api::result_blob(context, col.as_fixed_size_binary().value(row))
        }
        DataType::Date32 => match date32_to_string(downcast!(col, Date32Array).value(row)) {
            Some(s) => api::result_text(context, &s)?,
            None => api::result_null(context),
        },
        DataType::Date64 => match date64_to_string(downcast!(col, Date64Array).value(row)) {
            Some(s) => api::result_text(context, &s)?,
            None => api::result_null(context),
        },
        DataType::Timestamp(unit, _tz) => match timestamp_to_string(col, row, *unit)? {
            Some(s) => api::result_text(context, &s)?,
            None => api::result_null(context),
        },
        DataType::Decimal32(_, scale) => result_decimal(
            context,
            &col.as_primitive::<Decimal32Type>().value_as_string(row),
            *scale,
        )?,
        DataType::Decimal64(_, scale) => result_decimal(
            context,
            &col.as_primitive::<Decimal64Type>().value_as_string(row),
            *scale,
        )?,
        DataType::Decimal128(_, scale) => result_decimal(
            context,
            &col.as_primitive::<Decimal128Type>().value_as_string(row),
            *scale,
        )?,
        DataType::Decimal256(_, scale) => result_decimal(
            context,
            &col.as_primitive::<Decimal256Type>().value_as_string(row),
            *scale,
        )?,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => {
            let value = arrow_value_to_json(col, row)?;
            let text = serde_json::to_string(&value)
                .map_err(|e| Error::new_message(format!("json error: {}", e)))?;
            api::result_text(context, &text)?;
        }
        // Time32/64, Duration, Interval, dictionaries inside nested types, etc.
        _ => api::result_text(context, &display_value(col, row)?)?,
    }
    Ok(())
}

/// Decimals become INTEGER when the scale is zero and REAL otherwise. SQLite
/// has no exact decimal type; REAL keeps comparisons and arithmetic numeric.
fn result_decimal(context: *mut sqlite3_context, text: &str, scale: i8) -> Result<()> {
    if scale <= 0 {
        if let Ok(v) = text.parse::<i64>() {
            api::result_int64(context, v);
            return Ok(());
        }
    }
    let v = text
        .parse::<f64>()
        .map_err(|e| Error::new_message(format!("invalid decimal '{}': {}", text, e)))?;
    api::result_double(context, v);
    Ok(())
}

fn display_value(col: &dyn Array, row: usize) -> Result<String> {
    arrow::util::display::array_value_to_string(col, row)
        .map_err(|e| Error::new_message(format!("failed to format value: {}", e)))
}

fn date32_to_string(days: i32) -> Option<String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    epoch
        .checked_add_signed(chrono::Duration::days(days as i64))
        .map(|d| d.format("%Y-%m-%d").to_string())
}

fn date64_to_string(millis: i64) -> Option<String> {
    DateTime::from_timestamp_millis(millis).map(|dt| dt.format("%Y-%m-%d").to_string())
}

/// Timestamps are rendered in UTC as `YYYY-MM-DD HH:MM:SS.SSS`, the format
/// SQLite's date/time functions understand. Sub-millisecond precision is truncated.
fn timestamp_to_string(col: &dyn Array, row: usize, unit: TimeUnit) -> Result<Option<String>> {
    use arrow::datatypes::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    let (secs, nanos) = match unit {
        TimeUnit::Second => (col.as_primitive::<TimestampSecondType>().value(row), 0),
        TimeUnit::Millisecond => {
            let v = col.as_primitive::<TimestampMillisecondType>().value(row);
            (v.div_euclid(1000), (v.rem_euclid(1000) * 1_000_000) as u32)
        }
        TimeUnit::Microsecond => {
            let v = col.as_primitive::<TimestampMicrosecondType>().value(row);
            (
                v.div_euclid(1_000_000),
                (v.rem_euclid(1_000_000) * 1000) as u32,
            )
        }
        TimeUnit::Nanosecond => {
            let v = col.as_primitive::<TimestampNanosecondType>().value(row);
            (
                v.div_euclid(1_000_000_000),
                v.rem_euclid(1_000_000_000) as u32,
            )
        }
    };
    Ok(DateTime::from_timestamp(secs, nanos)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()))
}

// ---------------------------------------------------------------------------
// Arrow value -> JSON (for nested types)
// ---------------------------------------------------------------------------

fn decimal_json<T: DecimalType>(col: &dyn Array, row: usize, scale: i8) -> serde_json::Value {
    let text = col.as_primitive::<T>().value_as_string(row);
    if scale <= 0 {
        if let Ok(v) = text.parse::<i64>() {
            return serde_json::json!(v);
        }
    }
    match text.parse::<f64>() {
        Ok(v) => serde_json::json!(v),
        Err(_) => serde_json::Value::String(text),
    }
}

fn arrow_value_to_json(col: &dyn Array, row: usize) -> Result<serde_json::Value> {
    use serde_json::{json, Value};
    if col.is_null(row) {
        return Ok(Value::Null);
    }
    Ok(match col.data_type() {
        DataType::Null => Value::Null,
        DataType::Boolean => Value::Bool(downcast!(col, BooleanArray).value(row)),
        DataType::Int8 => json!(downcast!(col, Int8Array).value(row)),
        DataType::Int16 => json!(downcast!(col, Int16Array).value(row)),
        DataType::Int32 => json!(downcast!(col, Int32Array).value(row)),
        DataType::Int64 => json!(downcast!(col, Int64Array).value(row)),
        DataType::UInt8 => json!(downcast!(col, UInt8Array).value(row)),
        DataType::UInt16 => json!(downcast!(col, UInt16Array).value(row)),
        DataType::UInt32 => json!(downcast!(col, UInt32Array).value(row)),
        DataType::UInt64 => json!(downcast!(col, UInt64Array).value(row)),
        DataType::Float16 => json!(f64::from(downcast!(col, Float16Array).value(row))),
        DataType::Float32 => json!(downcast!(col, Float32Array).value(row)),
        DataType::Float64 => json!(downcast!(col, Float64Array).value(row)),
        DataType::Utf8 => Value::String(col.as_string::<i32>().value(row).to_owned()),
        DataType::LargeUtf8 => Value::String(col.as_string::<i64>().value(row).to_owned()),
        DataType::Utf8View => Value::String(col.as_string_view().value(row).to_owned()),
        // Binary has no JSON representation; hex matches SQLite's own hex().
        DataType::Binary => Value::String(hex(col.as_binary::<i32>().value(row))),
        DataType::LargeBinary => Value::String(hex(col.as_binary::<i64>().value(row))),
        DataType::BinaryView => Value::String(hex(col.as_binary_view().value(row))),
        DataType::FixedSizeBinary(_) => Value::String(hex(col.as_fixed_size_binary().value(row))),
        DataType::Date32 => date32_to_string(downcast!(col, Date32Array).value(row))
            .map_or(Value::Null, Value::String),
        DataType::Date64 => date64_to_string(downcast!(col, Date64Array).value(row))
            .map_or(Value::Null, Value::String),
        DataType::Timestamp(unit, _) => {
            timestamp_to_string(col, row, *unit)?.map_or(Value::Null, Value::String)
        }
        DataType::Decimal32(_, s) => decimal_json::<Decimal32Type>(col, row, *s),
        DataType::Decimal64(_, s) => decimal_json::<Decimal64Type>(col, row, *s),
        DataType::Decimal128(_, s) => decimal_json::<Decimal128Type>(col, row, *s),
        DataType::Decimal256(_, s) => decimal_json::<Decimal256Type>(col, row, *s),
        DataType::List(_) => array_to_json(col.as_list::<i32>().value(row).as_ref())?,
        DataType::LargeList(_) => array_to_json(col.as_list::<i64>().value(row).as_ref())?,
        DataType::FixedSizeList(_, _) => {
            array_to_json(col.as_fixed_size_list().value(row).as_ref())?
        }
        DataType::Struct(_) => struct_to_json(downcast!(col, StructArray), row)?,
        DataType::Map(_, _) => map_to_json(downcast!(col, MapArray), row)?,
        DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) => {
            // Nested dictionaries are rare; decode the single value via cast.
            let target = match col.data_type() {
                DataType::Dictionary(_, v) => v.as_ref().clone(),
                DataType::RunEndEncoded(_, v) => v.data_type().clone(),
                _ => unreachable!(),
            };
            let one = col.slice(row, 1);
            let cast = arrow::compute::cast(one.as_ref(), &target)
                .map_err(|e| Error::new_message(format!("failed to decode value: {}", e)))?;
            arrow_value_to_json(cast.as_ref(), 0)?
        }
        _ => Value::String(display_value(col, row)?),
    })
}

fn array_to_json(array: &dyn Array) -> Result<serde_json::Value> {
    (0..array.len())
        .map(|i| arrow_value_to_json(array, i))
        .collect::<Result<Vec<_>>>()
        .map(serde_json::Value::Array)
}

fn struct_to_json(arr: &StructArray, row: usize) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::with_capacity(arr.num_columns());
    for (field, col) in arr.fields().iter().zip(arr.columns()) {
        map.insert(
            field.name().clone(),
            arrow_value_to_json(col.as_ref(), row)?,
        );
    }
    Ok(serde_json::Value::Object(map))
}

fn map_to_json(arr: &MapArray, row: usize) -> Result<serde_json::Value> {
    let entries = arr.value(row);
    let keys = entries.column(0);
    let values = entries.column(1);
    let mut map = serde_json::Map::with_capacity(entries.len());
    for i in 0..entries.len() {
        let key = match arrow_value_to_json(keys.as_ref(), i)? {
            serde_json::Value::String(s) => s,
            other => other.to_string(),
        };
        map.insert(key, arrow_value_to_json(values.as_ref(), i)?);
    }
    Ok(serde_json::Value::Object(map))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}
