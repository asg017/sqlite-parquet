mod column_chunks;
mod meta;
mod metadata;
mod parquet;

use sqlite_loadable::prelude::*;
use sqlite_loadable::{
    define_scalar_function, define_table_function, define_virtual_table, FunctionFlags, Result,
};

use crate::{
    column_chunks::ColumnChunksTable,
    meta::{parquet_debug, parquet_version},
    metadata::MetadataTable,
    parquet::ParquetTable,
};

#[sqlite_entrypoint]
pub fn sqlite3_parquet_init(db: *mut sqlite3) -> Result<()> {
    define_scalar_function(
        db,
        "parquet_version",
        0,
        parquet_version,
        FunctionFlags::DETERMINISTIC,
    )?;
    define_scalar_function(
        db,
        "parquet_debug",
        0,
        parquet_debug,
        FunctionFlags::DETERMINISTIC,
    )?;

    // `parquet0` is the versioned module name; `parquet` is an alias.
    define_virtual_table::<ParquetTable>(db, "parquet0", None)?;
    define_virtual_table::<ParquetTable>(db, "parquet", None)?;

    define_table_function::<MetadataTable>(db, "parquet_metadata", None)?;
    define_table_function::<ColumnChunksTable>(db, "parquet_column_chunks", None)?;

    Ok(())
}
