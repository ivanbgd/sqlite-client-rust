//! Serial Type-related Functions
//!
//! See [2.1. Record Format](https://www.sqlite.org/fileformat.html#record_format).

use crate::constants::VarintType;
use crate::errors::VarintError;
use anyhow::Error;
use std::fs::File;
use std::io::Read;

#[derive(Debug, PartialEq)]
pub(crate) enum SerialTypeValue {
    Null(Option<u8>),
    Int8(i8),
    Int16(i16),
    Int24(i32),
    Int32(i32),
    Int48(i64),
    Int64(i64),
    Float64(f64),
    Zero,
    One,
    #[allow(dead_code)]
    Reserved10,
    #[allow(dead_code)]
    Reserved11,
    Blob(Vec<u8>),
    Text(String),
}

/// Takes a varint, `n`.
///
/// Returns content size of a record format serial type.
///
/// See [2.1. Record Format](https://www.sqlite.org/fileformat.html#record_format).
pub(crate) fn serial_type_to_content_size(
    n: VarintType,
) -> anyhow::Result<VarintType, VarintError> {
    let res = match n {
        0..=4 => n,
        5 => 6,
        6 | 7 => 8,
        8 | 9 => 0,
        10 | 11 => return Err(VarintError::Reserved(n)),
        12..=VarintType::MAX => {
            if n % 2 == 0 {
                (n - 12) / 2
            } else {
                (n - 13) / 2
            }
        }
        _ => return Err(VarintError::Unsupported(n)),
    };

    Ok(res)
}

/// Reads a single *serial type* from a database file.
///
/// Takes a varint, `n`.
///
/// Returns content of a record format serial type, wrapped in [`SerialTypeValue`].
///
/// See [2.1. Record Format](https://www.sqlite.org/fileformat.html#record_format).
///
/// Takes a file handle and reads from the current position in it.
///
/// Moves the pointer in the file handle by the number of bytes read, but also returns that number.
///
/// # Returns
///
/// Returns a 2-tuple of (content of a record format serial type, the number of bytes read).
pub(crate) fn read_serial_type_to_content(
    db_file: &mut File,
    n: VarintType,
) -> anyhow::Result<(SerialTypeValue, u64)> {
    let (val, read) = match n {
        0 => (SerialTypeValue::Null(None), 0u64),
        1 => {
            let mut cont = [0u8; 1];
            db_file.read_exact(&mut cont)?;
            (SerialTypeValue::Int8(i8::from_be_bytes([cont[0]])), 1)
        }
        2 => {
            let mut cont = [0u8; 2];
            db_file.read_exact(&mut cont)?;
            (
                SerialTypeValue::Int16(i16::from_be_bytes([cont[0], cont[1]])),
                2,
            )
        }
        3 => {
            let mut cont = [0u8; 3];
            db_file.read_exact(&mut cont)?;
            (
                SerialTypeValue::Int24(i32::from_be_bytes([0, cont[0], cont[1], cont[2]])),
                3,
            )
        }
        4 => {
            let mut cont = [0u8; 4];
            db_file.read_exact(&mut cont)?;
            (
                SerialTypeValue::Int32(i32::from_be_bytes([cont[0], cont[1], cont[2], cont[3]])),
                4,
            )
        }
        5 => {
            let mut cont = [0u8; 6];
            db_file.read_exact(&mut cont)?;
            (
                SerialTypeValue::Int48(i64::from_be_bytes([
                    0, 0, cont[0], cont[1], cont[2], cont[3], cont[4], cont[5],
                ])),
                6,
            )
        }
        6 => {
            let mut cont = [0u8; 8];
            db_file.read_exact(&mut cont)?;
            (
                SerialTypeValue::Int64(i64::from_be_bytes([
                    cont[0], cont[1], cont[2], cont[3], cont[4], cont[5], cont[6], cont[7],
                ])),
                8,
            )
        }
        7 => {
            let mut cont = [0u8; 8];
            db_file.read_exact(&mut cont)?;
            (
                SerialTypeValue::Float64(f64::from_be_bytes([
                    cont[0], cont[1], cont[2], cont[3], cont[4], cont[5], cont[6], cont[7],
                ])),
                8,
            )
        }
        8 => (SerialTypeValue::Zero, 0),
        9 => (SerialTypeValue::One, 0),
        10 | 11 => return Err(Error::from(VarintError::Reserved(n))),
        12..=VarintType::MAX => {
            if n % 2 == 0 {
                // Blob
                let size = (n as usize - 12) / 2;
                let mut contents = vec![0u8; size];
                db_file.read_exact(&mut contents)?;
                (SerialTypeValue::Blob(contents), size as u64)
            } else {
                // Text
                let size = (n as usize - 13) / 2;
                let mut contents = vec![0u8; size];
                db_file.read_exact(&mut contents)?;
                let contents = String::from_utf8(contents)?;
                (SerialTypeValue::Text(contents), size as u64)
            }
        }
        _ => return Err(Error::from(VarintError::Unsupported(n))),
    };

    Ok((val, read))
}

/// Takes a file handle and a supported integer length, and reads an integer of the given length from the
/// current position in the file and returns it.
///
/// Supported input values are `1..=6` for the length of the integer, `int_len`.
///
/// The returned value is a twos-complement integer.
///
/// Moves the pointer in the file handle by the number of bytes read, but also returns that number.
///
/// See [The Record Format](https://www.sqlite.org/fileformat.html#record_format).
///
/// # Returns
///
/// Returns a 2-tuple of (the decoded integer value, the number of bytes read).
pub fn decode_serial_type_integer(
    db_file: &mut File,
    mut int_len: u64,
) -> anyhow::Result<(i64, u64)> {
    assert!((1..=6).contains(&int_len));
    if int_len == 5 {
        int_len = 6;
    } else if int_len == 6 {
        int_len = 8;
    }

    let mut buf = [0u8; 8];
    for i in 0..int_len as usize {
        let mut byte = [0u8; 1];
        db_file.read_exact(&mut byte)?;
        buf[8 - int_len as usize + i] = byte[0];
    }
    let integer = i64::from_be_bytes(buf);

    Ok((integer, int_len))
}

#[cfg(test)]
mod tests {
    use crate::constants::VarintType;
    use crate::serial_type::{
        decode_serial_type_integer, read_serial_type_to_content, SerialTypeValue,
    };
    use std::fs::File;
    use std::io::{Seek, SeekFrom};

    #[test]
    fn read_serial_type_null() {
        // Not meant as a serial type contents in the DB file, but it serves the purpose of testing the function.
        let expected = SerialTypeValue::Null(None);
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x20)).unwrap(); // byte 0x00
        let n: VarintType = 0;
        let result = read_serial_type_to_content(&mut db_file, n).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(0, result.1);
    }

    #[test]
    fn read_serial_type_int8() {
        // Really meant as a serial type contents in the DB file.
        // This is a value from a "seq" column of the "sqlite_sequence" table in the "sample.db" file.
        let expected = SerialTypeValue::Int8(6_i8);
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x2ff3)).unwrap(); // byte 0x06
        let n: VarintType = 1;
        let result = read_serial_type_to_content(&mut db_file, n).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(1, result.1);
    }

    #[test]
    fn read_serial_type_int48() {
        // Not meant as a serial type contents in the DB file, but it serves the purpose of testing the function.
        let expected = SerialTypeValue::Int48(0x76_65_6c_20_4f_72);
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x3f20)).unwrap(); // bytes 0x76 0x65 0x6c 0x20 0x4f 0x72
        let n: VarintType = 5;
        let result = read_serial_type_to_content(&mut db_file, n).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(6, result.1);
    }

    #[test]
    fn read_serial_type_int64() {
        // Not meant as a serial type contents in the DB file, but it serves the purpose of testing the function.
        let expected = SerialTypeValue::Int64(0x76_65_6c_20_4f_72_61_6e);
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x3f20)).unwrap(); // bytes 0x76 0x65 0x6c 0x20 0x4f 0x72 0x61 0x6e
        let n: VarintType = 6;
        let result = read_serial_type_to_content(&mut db_file, n).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(8, result.1);
    }

    #[test]
    fn read_serial_type_text() {
        // Really meant as a serial type contents in the DB file.
        let expected = SerialTypeValue::Text("Golden Delicious".to_string());
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x1fa7)).unwrap(); // 16 bytes: 0x47 0x6f 0x6c 0x64 0x65 0x6e 0x20 ... 0x73
        let n: VarintType = 0x2d;
        let result = read_serial_type_to_content(&mut db_file, n).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(16, result.1);
    }

    /// Really meant as a serial type integer in the DB file; a single byte, though.
    /// This is the "apples" table's rootpage number, stored at offset 0xfa9 from the beginning of the DB file.
    #[test]
    fn decode_apples_rootpage() {
        let expected: i64 = 0x2; // dec 2
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x0fa9)).unwrap(); // byte 0x02
        let int_len = 1;
        let result = decode_serial_type_integer(&mut db_file, int_len).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(1, result.1);
    }

    /// Not meant as a serial type integer in the DB file, but it serves the purpose of testing the function;
    /// three bytes at offset 0x3f74.
    #[test]
    fn decode_three_byte_integer() {
        let expected: i64 = 0x6d656e; // dec 7169390
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x3f74)).unwrap(); // bytes 0x6d 0x65 0x6e
        let int_len = 3;
        let result = decode_serial_type_integer(&mut db_file, int_len).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(3, result.1);
    }

    /// Not meant as a serial type integer in the DB file, but it serves the purpose of testing the function;
    /// eight bytes at offset 0x3fa4.
    ///
    /// This number's MSByte has MSBit == 0, and this is a 64-bit signed integer number, so we're close to the
    /// limit, as we can't have the MSBit == 1, so this is a good test in that regard.
    ///
    /// Another thing which makes this test comprehensive is that we are converting serial type 6
    /// to content size 8.
    #[test]
    fn decode_eight_byte_integer() {
        let expected: i64 = 0x3b_54_61_6e_67_65_72_69; // dec 4275149073090441833
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x3fa4)).unwrap(); // hex bytes: 3b 54 61 6e  67 65 72 69
        let int_len = 6;
        let result = decode_serial_type_integer(&mut db_file, int_len).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(8, result.1);
    }
}
