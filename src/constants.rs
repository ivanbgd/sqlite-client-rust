//! # Constants
//!
//! Constants and types used throughout the application

pub type HeaderType = [u8; 100];
pub type VarintType = i64;

pub const HEADER: [u8; 100] = [0; 100];

pub const VARINT_MASK: u8 = 0b10000000;
pub const MAX_VARINT_LEN: usize = 9;

pub const TABLE_NAME_LEN: usize = 100;
