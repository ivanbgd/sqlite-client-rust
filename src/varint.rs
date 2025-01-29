//! Varint-related Functions
//!
//! A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement integers that uses
//! less space for small positive values. A varint is between 1 and 9 bytes in length.
//! The varint consists of either zero or more bytes which have the high-order bit set followed by a single byte
//! with the high-order bit clear, or nine bytes, whichever is shorter.
//! The lower seven bits of each of the first eight bytes and all 8 bits of the ninth byte are used to reconstruct
//! the 64-bit twos-complement integer. Varints are big-endian: bits taken from the earlier byte of the varint
//! are more significant than bits taken from the later bytes.
//!
//! The record format makes extensive use of the variable-length integer or varint representation of
//! 64-bit signed integers defined above.

use crate::constants::{VarintType, MAX_VARINT_LEN, VARINT_MASK};
use anyhow::Result;
use std::fs::File;
use std::io::Read;

/// Reads a single *varint* from a database file.
///
/// Takes a file handle and reads from the current position in it.
///
/// Stops reading when it determines the varint end. So, it does **not** read excess bytes.
///
/// Moves the pointer in the file handle by the number of bytes read, but also returns that number.
///
/// # Returns
///
/// Returns a 2-tuple of (the decoded *varint* value, the number of bytes read).
pub(crate) fn read_varint(db_file: &mut File) -> Result<(VarintType, u64)> {
    let mut varint = [0u8; MAX_VARINT_LEN];

    let mut i = 0;
    loop {
        let mut byte = [0u8; 1];
        db_file.read_exact(&mut byte)?;
        varint[i] = byte[0];
        i += 1;
        if byte[0] & VARINT_MASK == 0 {
            break;
        }
    }

    Ok((decode_varint(&varint), i as u64))
}

/// Decodes a single *varint*.
///
/// Simply discards excess bytes at the end of input.
///
/// [varint at SQLite docs](https://www.sqlite.org/fileformat2.html#varint)
///
/// [varint at AskClees](https://askclees.com/2020/11/20/sqlite-databases-at-hex-level/)
fn decode_varint(buf: &[u8]) -> VarintType {
    let mut res: VarintType = 0;

    for byte in buf {
        res = (res << 7) | (byte & !VARINT_MASK) as VarintType;
        if byte & VARINT_MASK == 0 {
            break;
        }
    }

    res
}

/// A database file, "sample.db", is used and it is included in the repository, but its size is only 16 kiB.
#[cfg(test)]
mod tests {
    use crate::varint::{decode_varint, read_varint};
    use std::fs::File;
    use std::io::{Seek, SeekFrom};

    #[test]
    fn dec_varint_1a() {
        let expected = 0x45; // dec 69
        let buf = [0x45];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_1b() {
        let expected = 0x45; // dec 69
        let buf = [0x45, 0xff];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_1c() {
        let expected = 0x45; // dec 69
        let buf = [0x45, 0x46];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_2() {
        let expected = 0xa3; // dec 163
        let buf = [0x81, 0x23];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_3() {
        let expected = 0x181; // dec 384
        let buf = [0x83, 0x01];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_4a() {
        let expected = 0x80b4; // dec 32948
        let buf = [0x82, 0x81, 0x34];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_4b() {
        let expected = 0x80b4; // dec 32948
        let buf = [0x82, 0x81, 0x34, 0xff];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn dec_varint_4c() {
        let expected = 0x80b4; // dec 32948
        let buf = [0x82, 0x81, 0x34, 0x7f];
        let result = decode_varint(&buf);
        assert_eq!(expected, result);
    }

    #[test]
    fn read_varint_1a() {
        // Really meant as a varint in the DB file; a single byte, though.
        let expected = 0x78; // dec 120
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x0ec3)).unwrap(); // byte 0x78
        let result = read_varint(&mut db_file).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(1, result.1);
    }

    #[test]
    fn read_varint_2() {
        // Not meant as a varint in the DB file, but it serves the purpose of testing the function; two bytes.
        let expected = 0x78f; // dec 1935
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x6d)).unwrap(); // bytes 0x8f 0x0f
        let result = read_varint(&mut db_file).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(2, result.1);
    }

    #[test]
    fn read_varint_3() {
        // Not meant as a varint in the DB file, but it serves the purpose of testing the function; two bytes.
        let expected = 0xc7; // dec 199
        let mut db_file = File::open("sample.db").unwrap();
        let _pos = db_file.seek(SeekFrom::Start(0x0eca)).unwrap(); // bytes 0x81 0x47
        let result = read_varint(&mut db_file).unwrap();
        assert_eq!(expected, result.0);
        assert_eq!(2, result.1);
    }
}
