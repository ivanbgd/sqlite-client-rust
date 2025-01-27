//! Tables and related functions

use crate::constants::{HEADER, SCHEMA_TABLE_FIELD_LEN};
use crate::dot_cmd;
use crate::errors::DotCmdError;
use crate::page::PageType;
use crate::varint::{read_varint, serial_type_to_content_size};
use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// A list of metadata for all tables in the database.
///
/// The metadata are extracted from the schema table.
///
/// This is a tuple-struct that contains a vector of metadata for each table in DB.
pub(crate) struct TablesMeta(pub(crate) Vec<SchemaTable>);

impl TablesMeta {
    pub(crate) fn get_tbl_names(self) -> Vec<String> {
        let mut tbl_names = Vec::with_capacity(self.0.len());
        for table in self.0 {
            tbl_names.push(table.tbl_name);
        }
        tbl_names
    }
}

#[allow(unused)]
/// [The Schema Table](https://www.sqlite.org/schematab.html)
pub(crate) struct SchemaTable {
    pub(crate) tbl_type: String,
    pub(crate) name: String,
    pub(crate) tbl_name: String,
    pub(crate) rootpage: u64,
}

impl SchemaTable {
    pub(crate) fn new(tbl_type: String, name: String, tbl_name: String, rootpage: u64) -> Self {
        Self {
            tbl_type,
            name,
            tbl_name,
            rootpage,
        }
    }
}

/// Returns the tables metadata in a SQLite database.
///
/// Currently, we only support one level of `PageType::InteriorTable`.
///
/// This function moves the file pointer internally, but it always starts at the beginning of the database file.
// TODO: Perhaps traverse the entire B-Tree properly. Keep in mind that page 1 is different than other pages.
pub(crate) fn get_tables_meta(db_file_path: &str) -> Result<TablesMeta, DotCmdError> {
    let mut db_file = File::open(db_file_path)?;
    let mut _pos = db_file.seek(SeekFrom::Start(HEADER.len() as u64))?;

    let mut tables_meta = TablesMeta(Vec::new());

    let page_size = dot_cmd::page_size(db_file_path)?;
    let mut page_num = 1;

    // Determine the page type.
    // We start with the root page, whose page number is 1. Only page 1 contains a database header.
    // First check the type of the page 1. It's the first byte of the page header.
    // Page header for page 1 comes right after the DB header.
    // So, the type of page 1 is stored in the first byte after the DB header.
    // The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
    let page_header_len: i64;
    let mut page_type = [0u8; 1];
    db_file.read_exact(&mut page_type)?;
    let page_type = match page_type[0].into() {
        PageType::InteriorTable => {
            page_header_len = 12;
            PageType::InteriorTable
        }
        PageType::LeafTable => {
            page_header_len = 8;
            PageType::LeafTable
        }
        other => return Err(DotCmdError::UnsupportedPageType(other)),
    };

    // The two-byte integer at offset 3 gives the number of cells on the page.
    _pos = db_file.seek(SeekFrom::Current(2))?;
    let mut page_num_cells = [0u8; 2];
    db_file.read_exact(&mut page_num_cells)?;
    let page_num_cells = u16::from_be_bytes(page_num_cells) as u32;

    // Skip to the cell pointer array. We have already read 5 bytes of the page header.
    _pos = db_file.seek(SeekFrom::Current(page_header_len - 5))?;
    let mut cell_pointer_array: Vec<u16> = Vec::with_capacity(page_num_cells as usize);
    for _ in 0..page_num_cells {
        let mut cell_ptr = [0u8; 2]; // The offsets are relative to the start of the page.
        db_file.read_exact(&mut cell_ptr)?;
        cell_pointer_array.push(u16::from_be_bytes(cell_ptr));
    }
    // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first
    // and the right-most cell (the cell with the largest key) last.
    // Reverse the array for potentially faster file seek operations from now on.
    // cell_pointer_array.reverse();

    let mut page_start = (page_num - 1) * page_size as u32;

    // Cell content is stored in the cell content region of the b-tree page.
    // Visit each cell.
    for cell_ptr in cell_pointer_array {
        // The cell pointer offsets are relative to the start of the page.
        _pos = db_file.seek(SeekFrom::Start((page_start + cell_ptr as u32) as u64))?;
        // The format of a cell (B-tree Cell Format) depends on which kind of
        // b-tree page the cell appears on (the current page).
        match page_type {
            PageType::InteriorTable => {
                // Page number of left child
                let mut left_page_num = [0u8; 4];
                db_file.read_exact(&mut left_page_num)?;
                let left_page_num = u32::from_be_bytes(left_page_num);
                let offset = ((left_page_num - 1) * page_size as u32) as u64;
                _pos = db_file.seek(SeekFrom::Start(offset))?;
                // Determine the page type. It's the first byte of the page header.
                let mut page_type = [0u8; 1];
                db_file.read_exact(&mut page_type)?;
                match page_type[0].into() {
                    #[allow(unused_assignments)]
                    PageType::InteriorTable => {
                        page_num += 1;
                        page_start = (page_num - 1) * page_size as u32;
                        todo!() // TODO: Repeat the process.
                    }
                    PageType::LeafTable => {
                        parse_schema_table(&mut db_file, &mut tables_meta)?;
                    }
                    other => return Err(DotCmdError::UnsupportedPageType(other)),
                };
            }
            PageType::LeafTable => {
                parse_schema_table(&mut db_file, &mut tables_meta)?;
            }
            other => panic!("Page type {other:?} encountered where it shouldn't be!"),
        }
    }

    Ok(tables_meta)
}

/// Reads a table type, name and rootpage, and adds them to `tables_meta`.
///
/// Works on an individual table.
///
/// This function moves the file pointer internally.
///
/// Records are stored in [The Record Format](https://www.sqlite.org/fileformat.html#record_format).
///
/// They contain a record header and a record body.
///
/// For the record body definition, see [The Schema Table](https://www.sqlite.org/schematab.html).
///
/// The record format defines a sequence of values corresponding to columns in a table or index.
/// The record format specifies the number of columns, the datatype of each column, and the content of each column.
///
/// The record format makes extensive use of the variable-length integer or varint
/// representation of 64-bit signed integers.
///
/// A record contains a header and a body, in that order.
/// The header begins with a single varint which determines the total number of bytes in the header.
/// The varint value is the size of the header in bytes including the size varint itself.
/// Following the size varint are one or more additional varints, one per column.
/// These additional varints are called "serial type" numbers and determine the datatype of each column.
fn parse_schema_table(db_file: &mut File, tables_meta: &mut TablesMeta) -> anyhow::Result<()> {
    // We need to read all varints that come before the table name that we're trying to get,
    // because varints are of variable length, hence we don't know in advance how many bytes to skip
    // to get to the table name, i.e., we don't know the offset - it is not fixed.

    // Read a varint which is the total number of bytes of payload, including any overflow.
    let (_payload_size, _) = read_varint(db_file)?;
    // Read a varint which is the integer key, a.k.a. "rowid".
    let (_row_id, _) = read_varint(db_file)?;

    // Record header

    // Read a varint which is the size of the record header.
    let (_record_header_size, _) = read_varint(db_file)?;
    // Serial type for sqlite_schema.type (varint) -> Size of sqlite_schema.type:
    let schema_type = serial_type_to_content_size(read_varint(db_file)?.0)? as u64;
    // Serial type for sqlite_schema.name (varint) -> Size of sqlite_schema.name:
    let schema_name = serial_type_to_content_size(read_varint(db_file)?.0)? as u64;
    // Serial type for sqlite_schema.tbl_name (varint) -> Size of sqlite_schema.tbl_name:
    let schema_tbl_name = serial_type_to_content_size(read_varint(db_file)?.0)? as u64;
    // Serial type for sqlite_schema.rootpage (varint) -> A twos-complement integer.
    // For example, if 1, content size is 1 and the value is an 8-bit twos-complement integer.
    let schema_rootpage = serial_type_to_content_size(read_varint(db_file)?.0)? as u64;
    // Serial type for sqlite_schema.sql (varint) -> Size of sqlite_schema.sql:
    let _schema_sql = serial_type_to_content_size(read_varint(db_file)?.0)? as u64;

    // Record body

    let record_body_start = db_file.stream_position()?;
    let mut buf = [0u8; SCHEMA_TABLE_FIELD_LEN];

    // Schema table type
    db_file.read_exact(&mut buf)?;
    let tbl_type = String::from_utf8(Vec::from(&buf[0..schema_type as usize]))?;

    // Schema name
    let _pos = db_file.seek(SeekFrom::Start(record_body_start + schema_type))?;
    db_file.read_exact(&mut buf)?;
    let name = String::from_utf8(Vec::from(&buf[0..schema_name as usize]))?;

    // Schema table name
    let _pos = db_file.seek(SeekFrom::Start(
        record_body_start + schema_type + schema_name,
    ))?;
    db_file.read_exact(&mut buf)?;
    let tbl_name = String::from_utf8(Vec::from(&buf[0..schema_tbl_name as usize]))?;

    // Schema rootpage
    let _pos = db_file.seek(SeekFrom::Start(
        record_body_start + schema_type + schema_name + schema_tbl_name,
    ))?;
    let rootpage = decode_serial_type_integer(db_file, schema_rootpage)?.0 as u64;

    tables_meta
        .0
        .push(SchemaTable::new(tbl_type, name, tbl_name, rootpage));

    Ok(())
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
fn decode_serial_type_integer(db_file: &mut File, mut int_len: u64) -> Result<(i64, u64)> {
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
    use crate::tables::decode_serial_type_integer;
    use std::fs::File;
    use std::io::{Seek, SeekFrom};

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
