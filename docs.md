## Parquet Field Types to SQLite

| Parquet Field Type      | SQLite Result Type                                                  |
| ----------------------- | ------------------------------------------------------------------- |
| `Null`                  | `NULL`                                                              |
| `Bool` (bool)           | `1` if true, `0` if false                                           |
| `Byte` (i8)             | `INTEGER`                                                           |
| `Short` (i16)           | `INTEGER`                                                           |
| `Int` (i32)             | `INTEGER`                                                           |
| `Long` (i64)            | `INTEGER` - as `int64`                                              |
| `UByte` (u8)            | `INTEGER`                                                           |
| `UShort` (u16)          | `INTEGER`                                                           |
| `UInt` (u32)            | `INTEGER` - as `int32` if it fits, otherwise `int64`                |
| `ULong` (u64)           | `INTEGER` or `ERROR` - as `int64` if it fits, otherwise errors TODO |
| `Float` (f32)           | `REAL`                                                              |
| `Double` (f64)          | `REAL`                                                              |
| `Decimal` (Decimal)     | `BLOB` - TODO                                                       |
| `Str` (String)          | `TEXT`                                                              |
| `Bytes` (ByteArray)     | `BLOB`                                                              |
| `Date` (u32)            | `YYYY-MM-DD`                                                        |
| `TimestampMillis` (u64) | `INT64` - (TODO)                                                    |
| `TimestampMicros` (u64) | `YYYY-MM-DD HH:MM:SS.SSS`                                           |
| `Group` (Row)           | `JSON`                                                              |
| `ListInternal` (List)   | `JSON`                                                              |
| `MapInternal` (Map)     | `JSON`                                                              |
