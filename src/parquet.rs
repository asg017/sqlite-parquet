use parquet::{
    file::{
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
    },
    record::{reader::RowIter, Field, Row},
};
use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    api,
    table::{ConstraintOperator, IndexInfo, VTab, VTabArguments, VTabCursor},
    BestIndexError, Error, Result,
};

use std::{fs::File, marker::PhantomData, mem, os::raw::c_int};

use chrono::{NaiveDate, NaiveDateTime};

#[repr(C)]
pub struct ParquetCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    //reader: Box<SerializedFileReader<File>>,
    iter: Option<RowIter<'vtab>>,
    current: Option<Row>,
    eof: bool,
    phantom: PhantomData<&'vtab ParquetTable>,
}
use std::time::Instant;

impl ParquetCursor<'_> {
    fn new<'vtab>(path: &str) -> ParquetCursor<'vtab> {
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let iter = RowIter::from_file_into(Box::new(reader));

        let base: sqlite3_vtab_cursor = unsafe { mem::zeroed() };
        let mut cursor = ParquetCursor {
            base,
            //reader: Box::new(reader),
            iter: Some(iter),
            current: None,
            eof: false,
            phantom: PhantomData,
        };
        let start = Instant::now();
        cursor.next().unwrap();
        println!("x {:?}", start.elapsed());
        cursor
    }
}

impl VTabCursor for ParquetCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _values: &[*mut sqlite3_value],
    ) -> Result<()> {
        Ok(())
    }

    fn next(&mut self) -> Result<()> {
        //let iter = self.iter;

        //let has_more = self.reader.read_record(&mut self.record).unwrap();
        //self.eof = !has_more;
        //let start = Instant::now();
        self.current = self.iter.as_mut().unwrap().next();
        //println!("1 {:?}", start.elapsed());
        Ok(())
    }

    fn eof(&self) -> bool {
        self.current.is_none()
    }

    fn column(&self, context: *mut sqlite3_context, i: c_int) -> Result<()> {
        let row = self.current.as_ref().unwrap();
        let field = row.get_column_iter().nth(i.try_into().unwrap()).unwrap().1;
        match field {
            Field::Null => {
                api::result_null(context);
            }
            Field::Bool(b) => {
                api::result_bool(context, *b);
            }

            Field::Byte(value) => {
                api::result_int(context, (*value).into());
            }
            Field::UByte(value) => {
                api::result_int(context, (*value).into());
            }
            Field::Short(value) => {
                api::result_int(context, (*value).into());
            }
            Field::UShort(value) => api::result_int(context, (*value).into()),
            Field::Int(i) => {
                api::result_int(context, *i);
            }
            Field::UInt(value) => match i32::try_from(*value) {
                Ok(value) => api::result_int(context, value),
                Err(_) => api::result_int64(context, (*value).into()),
            },

            Field::Long(value) => {
                api::result_int64(context, *value);
            }
            Field::ULong(value) => {
                match i64::try_from(*value) {
                    Ok(value) => api::result_int64(context, value),
                    Err(err) => {
                        return Err(Error::new_message(
                            format!("Value too large: {}", err).as_str(),
                        ))
                    }
                };
            }

            Field::Double(value) => {
                api::result_double(context, *value);
            }
            Field::Float(value) => {
                api::result_double(context, (*value).into());
            }
            Field::Decimal(value) => {
                //println!("{} {}", value.precision(), value.scale());
                // TODO match on value, get i32/i64/bytes, then do something??
                api::result_blob(context, value.data());
            }

            Field::Str(s) => {
                api::result_text(context, s)?;
            }
            Field::Bytes(b) => {
                api::result_blob(context, b.data());
            }
            Field::ListInternal(_) | Field::Group(_) | Field::MapInternal(_) => {
                api::result_json(context, field.to_json_value())?;
            }
            Field::Date(value) => {
                let ts = NaiveDate::from_num_days_from_ce(719163 + i32::try_from(*value).unwrap());
                let f = ts.format("%Y-%m-%d");
                api::result_text(context, &f.to_string())?;
            }
            Field::TimestampMillis(t) => {
                api::result_int64(context, (*t).try_into().unwrap());
            }
            Field::TimestampMicros(t) => {
                //api::result_int64((*t).try_into().unwrap());
                let ts = NaiveDateTime::from_timestamp(
                    (*t / 1000000).try_into().unwrap(),
                    u32::try_from(*t % 1000000).unwrap() * 1000,
                );
                let f = ts.format("%Y-%m-%d %H:%M:%S.%3f");
                api::result_text(context, &f.to_string())?;
            }
        }
        Ok(())
    }

    fn rowid(&self) -> Result<i64> {
        Ok(1)
    }
}

#[repr(C)]
pub struct ParquetTable {
    /// must be first
    base: sqlite3_vtab,
    path: String,
}

impl<'vtab> VTab<'vtab> for ParquetTable {
    type Aux = ();
    type Cursor = ParquetCursor<'vtab>;

    fn connect(
        _db: *mut sqlite3,
        _aux: Option<&()>,
        args: VTabArguments,
    ) -> Result<(String, ParquetTable)> {
        let mut path = None;
        for arg in args.arguments {
            let mut split = arg.trim().split('=');
            let _key = split.next().unwrap();
            let value = split.next().unwrap();
            let mut chars = value.chars();
            chars.next();
            chars.next_back();
            let value = chars.as_str();
            path = Some(value.to_owned());
        }
        let path = path.unwrap();
        let file = File::open(&path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let base: sqlite3_vtab = unsafe { mem::zeroed() };

        let vtab = ParquetTable { base, path };

        let mut sql = String::from("create table x(");
        let metadata = reader.metadata();
        let schema = metadata.file_metadata().schema();
        let mut it = schema.get_fields().iter().peekable();

        while let Some(field) = it.next() {
            sql.push('"');
            sql.push_str(field.name());
            sql.push('"');
            if it.peek().is_some() {
                sql.push(',');
            }
        }
        sql.push(')');

        let fm = metadata.file_metadata();
        //fm.
        let rg = metadata.row_group(0);
        println!(
            "rgo: bytesize={}, comp={}",
            rg.total_byte_size(),
            rg.compressed_size(),
        );
        for column_chunk in rg.columns() {
            let stats = column_chunk.statistics().unwrap();
            print!(
                "{} {} ",
                column_chunk.column_path(),
                stats.has_min_max_set()
            );
            match stats {
                Statistics::Int32(ref typed) => {
                    print!(
                        "i32: [{}, {}] {:?} {}",
                        typed.min(),
                        typed.max(),
                        stats.distinct_count(),
                        stats.has_nulls()
                    )
                }
                Statistics::Int64(ref typed) => {
                    print!("i64: [{}, {}]", typed.min(), typed.max())
                }
                Statistics::Double(ref typed) => {
                    print!("double: [{}, {}]", typed.min(), typed.max())
                }
                _ => (),
            };
            println!();
        }

        for col in fm.schema_descr().columns() {
            println!(
                "{} {}: {} {:?} {}",
                col.name(),
                col.path(),
                col.physical_type(),
                col.logical_type(),
                col.converted_type(),
            );
        }
        if let Some(metadata) = fm.key_value_metadata() {
            for x in metadata {
                println!("\t{} {:?}", x.key, x.value);
            }
        }

        println!(
            "version: {}, rows: {}, rowgroups:{}, created_by:{:?}",
            fm.version(),
            fm.num_rows(),
            metadata.num_row_groups(),
            fm.created_by(),
        );
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

    fn open(&mut self) -> Result<ParquetCursor<'_>> {
        Ok(ParquetCursor::new(&self.path))
    }
}
