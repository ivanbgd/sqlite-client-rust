//! # Constants
//!
//! Constants and types used throughout the application

pub type HeaderType = [u8; 100];
pub type VarintType = i64;
pub type ColumnNameOrd = (String, usize);

pub const HEADER: [u8; 100] = [0; 100];

pub const VARINT_MASK: u8 = 0b10000000;
pub const MAX_VARINT_LEN: usize = 9;

/// Maximum allowed schema table column size in bytes.
pub const SCHEMA_TABLE_FIELD_LEN: usize = 250;

/// Maximum allowed column size in bytes.
pub const COLUMN_SIZE: usize = 100;

pub const SELECT_PATTERN: &str = "select ";
pub const COUNT_PATTERN: &str = "count(*) ";
pub const FROM_PATTERN: &str = "from ";
pub const WHERE_PATTERN: &str = "where ";
pub const LIMIT_PATTERN: &str = "limit ";
