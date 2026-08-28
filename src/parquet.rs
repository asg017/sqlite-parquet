use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, ListArray, MapArray,
    StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use glob::Paths;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlite_loadable::prelude::*;
use sqlite_loadable::scalar::scalar_function_raw_with_aux;
use sqlite_loadable::vtab_argparse::{parse_argument, Argument};
use sqlite_loadable::{
    api,
    table::{IndexInfo, VTab, VTabArguments, VTabCursor, VTabFind},
    BestIndexError, Error, Result,
};

use std::{fs::File, mem, os::raw::c_int, os::raw::c_void};

use chrono::NaiveDate;

#[repr(C)]
pub struct ParquetTable {
    /// must be first
    base: sqlite3_vtab,
    db: *mut sqlite3,
    input: String,
    // dynamically updated from cursor's next() — NOT threadsafe
    current_path: String,
}

fn schema_from_file(path: &str) -> Result<String> {
    let file = File::open(path).map_err(|e| {
        Error::new_message(&format!("failed to open file '{}': {}", path, e))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
        Error::new_message(&format!("failed to read parquet file '{}': {}", path, e))
    })?;
    let arrow_schema = builder.schema();

    let mut sql = String::from("create table x(");
    let mut first = true;
    for field in arrow_schema.fields() {
        if !first {
            sql.push(',');
        }
        first = false;
        sql.push('"');
        sql.push_str(field.name());
        sql.push('"');
    }
    sql.push(')');
    Ok(sql)
}

fn first_glob_match(pattern: &str) -> Result<String> {
    let mut paths = glob::glob(pattern).map_err(|e| {
        Error::new_message(&format!("invalid glob pattern '{}': {}", pattern, e))
    })?;
    match paths.next() {
        Some(Ok(path)) => Ok(path.to_string_lossy().to_string()),
        Some(Err(e)) => Err(Error::new_message(&format!("glob error: {}", e))),
        None => Err(Error::new_message(&format!(
            "no files matched pattern '{}'",
            pattern
        ))),
    }
}

impl<'vtab> VTab<'vtab> for ParquetTable {
    type Aux = ();
    type Cursor = ParquetCursor;

    fn connect(
        db: *mut sqlite3,
        _aux: Option<&()>,
        args: VTabArguments,
    ) -> Result<(String, ParquetTable)> {
        let mut filename: Option<String> = None;

        for arg in &args.arguments {
            match parse_argument(arg.as_str()) {
                Ok(Argument::Config(config)) => {
                    if config.key.trim() == "filename" {
                        filename = Some(match config.value {
                            sqlite_loadable::vtab_argparse::ConfigOptionValue::Quoted(v) => v,
                            sqlite_loadable::vtab_argparse::ConfigOptionValue::Bareword(v) => v,
                            sqlite_loadable::vtab_argparse::ConfigOptionValue::SqliteParameter(_) => {
                                return Err(Error::new_message(
                                    "sqlite parameters not supported for filename",
                                ));
                            }
                        });
                    }
                }
                Ok(Argument::Column(_)) => {}
                Err(_) => {}
            }
        }

        let input = match filename {
            Some(f) => f,
            None => {
                // table name as filepath / glob pattern
                if glob::glob(&args.table_name)
                    .map_or(false, |mut paths| paths.next().is_some())
                {
                    args.table_name.clone()
                } else {
                    return Err(Error::new_message(
                        "no filename given. Use filename=\"path\" or use a file path as the table name.",
                    ));
                }
            }
        };

        let first_file = first_glob_match(&input)?;
        let sql = schema_from_file(&first_file)?;

        let base: sqlite3_vtab = unsafe { mem::zeroed() };
        let vtab = ParquetTable {
            base,
            db,
            input,
            current_path: String::new(),
        };

        Ok((sql, vtab))
    }

    fn destroy(&self) -> Result<()> {
        Ok(())
    }

    fn best_index(&self, mut info: IndexInfo) -> core::result::Result<(), BestIndexError> {
        info.set_idxnum(1);
        info.set_estimated_rows(100000);
        info.set_estimated_cost(100000.0);
        Ok(())
    }

    fn open(&mut self) -> Result<ParquetCursor> {
        ParquetCursor::new(self)
    }
}

impl<'vtab> VTabFind<'vtab> for ParquetTable {
    fn find_function(
        &mut self,
        argc: i32,
        name: &str,
    ) -> Option<(
        unsafe extern "C" fn(*mut sqlite3_context, i32, *mut *mut sqlite3_value),
        Option<i32>,
        Option<*mut c_void>,
    )> {
        if argc == 1 && name == "parquet_path" {
            let x = scalar_function_raw_with_aux(parquet_path_impl, self as *mut ParquetTable);
            return Some((x.0, None, Some(x.1)));
        }
        None
    }
}

pub fn parquet_path_impl(
    context: *mut sqlite3_context,
    _values: &[*mut sqlite3_value],
    aux: &*mut ParquetTable,
) -> Result<()> {
    unsafe {
        api::result_text(context, (**aux).current_path.as_str())?;
    }
    Ok(())
}

#[repr(C)]
pub struct ParquetCursor {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    rowid: i64,
    input: String,
    paths: Option<Paths>,
    current_reader: Option<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
    current_batch: Option<RecordBatch>,
    batch_row_idx: usize,
    eof: bool,
    table: *mut ParquetTable,
}

impl ParquetCursor {
    fn new(table: &mut ParquetTable) -> Result<ParquetCursor> {
        let cursor = ParquetCursor {
            base: unsafe { mem::zeroed() },
            rowid: 0,
            input: table.input.clone(),
            paths: None,
            current_reader: None,
            current_batch: None,
            batch_row_idx: 0,
            eof: true,
            table: table as *mut ParquetTable,
        };
        Ok(cursor)
    }

    fn next_path_reader(
        &mut self,
    ) -> Result<Option<parquet::arrow::arrow_reader::ParquetRecordBatchReader>> {
        let paths = match self.paths.as_mut() {
            Some(p) => p,
            None => return Ok(None),
        };
        match paths.next() {
            Some(Ok(path)) => {
                let path_str = path.to_string_lossy().to_string();
                unsafe {
                    (*self.table).current_path = path_str.clone();
                }
                let file = File::open(&path).map_err(|e| {
                    Error::new_message(&format!("failed to open file '{}': {}", path_str, e))
                })?;
                let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
                    Error::new_message(&format!(
                        "failed to read parquet file '{}': {}",
                        path_str, e
                    ))
                })?;
                let reader = builder.build().map_err(|e| {
                    Error::new_message(&format!(
                        "failed to build reader for '{}': {}",
                        path_str, e
                    ))
                })?;
                Ok(Some(reader))
            }
            Some(Err(e)) => Err(Error::new_message(&format!("glob error: {}", e))),
            None => Ok(None),
        }
    }

    fn next_batch(&mut self) -> Result<bool> {
        if let Some(reader) = self.current_reader.as_mut() {
            match reader.next() {
                Some(Ok(batch)) => {
                    self.current_batch = Some(batch);
                    self.batch_row_idx = 0;
                    return Ok(true);
                }
                Some(Err(e)) => {
                    return Err(Error::new_message(&format!("error reading batch: {}", e)));
                }
                None => {
                    return Ok(false);
                }
            }
        }
        Ok(false)
    }

    fn advance(&mut self) -> Result<()> {
        // Try advancing within current batch
        if let Some(batch) = &self.current_batch {
            if self.batch_row_idx + 1 < batch.num_rows() {
                self.batch_row_idx += 1;
                self.rowid += 1;
                return Ok(());
            }
        }

        // Try next batch from current reader
        if self.next_batch()? {
            self.rowid += 1;
            return Ok(());
        }

        // Try next file from glob
        loop {
            match self.next_path_reader()? {
                Some(reader) => {
                    self.current_reader = Some(reader);
                    if self.next_batch()? {
                        self.rowid += 1;
                        return Ok(());
                    }
                    // empty file, try next
                }
                None => {
                    self.eof = true;
                    self.current_batch = None;
                    self.current_reader = None;
                    return Ok(());
                }
            }
        }
    }
}

impl VTabCursor for ParquetCursor {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _values: &[*mut sqlite3_value],
    ) -> Result<()> {
        let paths = glob::glob(&self.input).map_err(|e| {
            Error::new_message(&format!("invalid glob pattern: {}", e))
        })?;
        self.paths = Some(paths);
        self.current_reader = None;
        self.current_batch = None;
        self.batch_row_idx = 0;
        self.rowid = 0;
        self.eof = false;
        self.advance()?;
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        self.advance()
    }

    fn eof(&self) -> bool {
        self.eof
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let batch = match &self.current_batch {
            Some(b) => b,
            None => return Ok(()),
        };
        let col_idx = i as usize;
        let col = batch.column(col_idx);
        let row = self.batch_row_idx;

        if col.is_null(row) {
            api::result_null(context);
            return Ok(());
        }

        match col.data_type() {
            DataType::Boolean => {
                let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                api::result_bool(context, arr.value(row));
            }
            DataType::Int8 => {
                let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                api::result_int(context, arr.value(row) as i32);
            }
            DataType::Int16 => {
                let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                api::result_int(context, arr.value(row) as i32);
            }
            DataType::Int32 => {
                let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                api::result_int(context, arr.value(row));
            }
            DataType::Int64 => {
                let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                api::result_int64(context, arr.value(row));
            }
            DataType::UInt8 => {
                let arr = col.as_any().downcast_ref::<UInt8Array>().unwrap();
                api::result_int(context, arr.value(row) as i32);
            }
            DataType::UInt16 => {
                let arr = col.as_any().downcast_ref::<UInt16Array>().unwrap();
                api::result_int(context, arr.value(row) as i32);
            }
            DataType::UInt32 => {
                let arr = col.as_any().downcast_ref::<UInt32Array>().unwrap();
                api::result_int64(context, arr.value(row) as i64);
            }
            DataType::UInt64 => {
                let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                let val = arr.value(row);
                match i64::try_from(val) {
                    Ok(v) => api::result_int64(context, v),
                    Err(e) => {
                        return Err(Error::new_message(
                            &format!("uint64 value too large: {}", e),
                        ));
                    }
                }
            }
            DataType::Float32 => {
                let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                api::result_double(context, arr.value(row) as f64);
            }
            DataType::Float64 => {
                let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                api::result_double(context, arr.value(row));
            }
            DataType::Utf8 => {
                let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                api::result_text(context, arr.value(row))?;
            }
            DataType::LargeUtf8 => {
                let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                api::result_text(context, arr.value(row))?;
            }
            DataType::Binary => {
                let arr = col.as_any().downcast_ref::<arrow::array::BinaryArray>().unwrap();
                api::result_blob(context, arr.value(row));
            }
            DataType::LargeBinary => {
                let arr = col.as_any().downcast_ref::<arrow::array::LargeBinaryArray>().unwrap();
                api::result_blob(context, arr.value(row));
            }
            DataType::FixedSizeBinary(_) => {
                let arr = col.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>().unwrap();
                api::result_blob(context, arr.value(row));
            }
            DataType::Date32 => {
                let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                let days = arr.value(row);
                if let Some(date) = NaiveDate::from_num_days_from_ce_opt(719163 + days) {
                    let formatted = date.format("%Y-%m-%d").to_string();
                    api::result_text(context, &formatted)?;
                } else {
                    api::result_null(context);
                }
            }
            DataType::Timestamp(unit, _tz) => {
                match unit {
                    arrow::datatypes::TimeUnit::Second => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .unwrap();
                        let secs = arr.value(row);
                        format_timestamp_to_result(context, secs, 0)?;
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        let ms = arr.value(row);
                        let secs = ms / 1000;
                        let nanos = ((ms % 1000) * 1_000_000) as u32;
                        format_timestamp_to_result(context, secs, nanos)?;
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        let us = arr.value(row);
                        let secs = us / 1_000_000;
                        let nanos = ((us % 1_000_000) * 1000) as u32;
                        format_timestamp_to_result(context, secs, nanos)?;
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        let ns = arr.value(row);
                        let secs = ns / 1_000_000_000;
                        let nanos = (ns % 1_000_000_000) as u32;
                        format_timestamp_to_result(context, secs, nanos)?;
                    }
                }
            }
            DataType::Decimal128(precision, scale) => {
                let arr = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let val = arr.value(row);
                let s = format_decimal128(val, *precision, *scale);
                api::result_text(context, &s)?;
            }
            DataType::List(_) => {
                let arr = col.as_any().downcast_ref::<ListArray>().unwrap();
                let value = arr.value(row);
                let json = arrow_array_to_json(&value)?;
                api::result_text(context, &json)?;
            }
            DataType::Struct(_) => {
                let arr = col.as_any().downcast_ref::<StructArray>().unwrap();
                let json = arrow_struct_row_to_json(arr, row)?;
                api::result_text(context, &json)?;
            }
            DataType::Map(_, _) => {
                let arr = col.as_any().downcast_ref::<MapArray>().unwrap();
                let value = arr.value(row);
                let json = arrow_map_to_json(&value)?;
                api::result_text(context, &json)?;
            }
            _ => {
                // Fallback: try to format as string
                let formatted = arrow::util::display::array_value_to_string(col, row)
                    .unwrap_or_default();
                api::result_text(context, &formatted)?;
            }
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(self.rowid)
    }
}

fn format_timestamp_to_result(
    context: *mut sqlite3_context,
    secs: i64,
    nanos: u32,
) -> Result<()> {
    use chrono::DateTime;
    if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
        let formatted = dt.naive_utc().format("%Y-%m-%d %H:%M:%S.%3f").to_string();
        api::result_text(context, &formatted)?;
    } else {
        api::result_null(context);
    }
    Ok(())
}

fn format_decimal128(value: i128, _precision: u8, scale: i8) -> String {
    if scale <= 0 {
        // No decimal places
        let multiplier = 10i128.pow((-scale) as u32);
        return (value * multiplier).to_string();
    }
    let scale = scale as u32;
    let divisor = 10i128.pow(scale);
    let integer_part = value / divisor;
    let fractional_part = (value % divisor).abs();
    format!("{}.{:0>width$}", integer_part, fractional_part, width = scale as usize)
}

fn arrow_array_to_json(array: &dyn Array) -> Result<String> {
    let mut values = Vec::new();
    for i in 0..array.len() {
        if array.is_null(i) {
            values.push(serde_json::Value::Null);
        } else {
            match array.data_type() {
                DataType::Boolean => {
                    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    values.push(serde_json::Value::Bool(arr.value(i)));
                }
                DataType::Int8 => {
                    let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::Int16 => {
                    let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::Int32 => {
                    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::Int64 => {
                    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::UInt8 => {
                    let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::UInt16 => {
                    let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::UInt32 => {
                    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::UInt64 => {
                    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::Float32 => {
                    let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::Float64 => {
                    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    values.push(serde_json::json!(arr.value(i)));
                }
                DataType::Utf8 => {
                    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                    values.push(serde_json::Value::String(arr.value(i).to_string()));
                }
                DataType::Binary => {
                    use arrow::array::BinaryArray;
                    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let bytes = arr.value(i);
                    let encoded = bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                    values.push(serde_json::Value::String(encoded));
                }
                _ => {
                    let s = arrow::util::display::array_value_to_string(array, i)
                        .unwrap_or_default();
                    values.push(serde_json::Value::String(s));
                }
            }
        }
    }
    serde_json::to_string(&values).map_err(|e| Error::new_message(&format!("json error: {}", e)))
}

fn arrow_struct_row_to_json(arr: &StructArray, row: usize) -> Result<String> {
    let mut map = serde_json::Map::new();
    for (i, field) in arr.fields().iter().enumerate() {
        let col = arr.column(i);
        if col.is_null(row) {
            map.insert(field.name().clone(), serde_json::Value::Null);
        } else {
            match col.data_type() {
                DataType::Boolean => {
                    let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                    map.insert(field.name().clone(), serde_json::Value::Bool(a.value(row)));
                }
                DataType::Int32 => {
                    let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    map.insert(field.name().clone(), serde_json::json!(a.value(row)));
                }
                DataType::Int64 => {
                    let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    map.insert(field.name().clone(), serde_json::json!(a.value(row)));
                }
                DataType::Float64 => {
                    let a = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    map.insert(field.name().clone(), serde_json::json!(a.value(row)));
                }
                DataType::Utf8 => {
                    let a = col.as_any().downcast_ref::<StringArray>().unwrap();
                    map.insert(
                        field.name().clone(),
                        serde_json::Value::String(a.value(row).to_string()),
                    );
                }
                _ => {
                    let s = arrow::util::display::array_value_to_string(col, row)
                        .unwrap_or_default();
                    map.insert(field.name().clone(), serde_json::Value::String(s));
                }
            }
        }
    }
    serde_json::to_string(&map).map_err(|e| Error::new_message(&format!("json error: {}", e)))
}

fn arrow_map_to_json(struct_arr: &dyn Array) -> Result<String> {
    // Map entries are stored as a struct array with "key" and "value" fields
    let struct_arr = struct_arr
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::new_message("expected struct array for map entries"))?;
    let mut map = serde_json::Map::new();
    let keys = struct_arr.column(0);
    let vals = struct_arr.column(1);
    for i in 0..struct_arr.len() {
        let key = arrow::util::display::array_value_to_string(keys, i).unwrap_or_default();
        if vals.is_null(i) {
            map.insert(key, serde_json::Value::Null);
        } else {
            let val = arrow::util::display::array_value_to_string(vals, i).unwrap_or_default();
            // Try to parse as number first
            if let Ok(n) = val.parse::<i64>() {
                map.insert(key, serde_json::json!(n));
            } else if let Ok(n) = val.parse::<f64>() {
                map.insert(key, serde_json::json!(n));
            } else {
                map.insert(key, serde_json::Value::String(val));
            }
        }
    }
    serde_json::to_string(&map).map_err(|e| Error::new_message(&format!("json error: {}", e)))
}
