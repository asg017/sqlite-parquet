mod column_chunks;
mod meta;
mod metadata;
mod parquet;

use sqlite3_loadable::{
    errors::Result,
    scalar::define_scalar_function,
    sqlite3_entrypoint, sqlite3_imports,
    table::{define_table_function, define_virtual_table},
};
use sqlite3ext_sys::sqlite3;

use crate::{
    column_chunks::ColumnChunksTable,
    meta::{parquet_debug, parquet_version},
    metadata::{MetadataTable},
    parquet::ParquetTable,
};

sqlite3_imports!();

#[sqlite3_entrypoint]
pub fn sqlite3_parquet_init(db: *mut sqlite3) -> Result<()> {
    define_scalar_function(db, "parquet_version", 0, parquet_version)?;
    define_scalar_function(db, "parquet_debug", 0, parquet_debug)?;

    define_virtual_table::<ParquetTable>(db, "parquet", None)?;
    define_table_function::<MetadataTable>(db, "parquet_metadata", None)?;
    define_table_function::<ColumnChunksTable>(db, "parquet_column_chunks", None)?;
    
    Ok(())
}
