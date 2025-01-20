//! # Dot Command Handlers
//!
//! [3. Special commands to sqlite3 (dot-commands)](https://www.sqlite.org/cli.html#special_commands_to_sqlite3_dot_commands_)
//!
//! [Database File Format](https://www.sqlite.org/fileformat.html)

use crate::constants::{HEADER, TABLE_NAME_LEN};
use crate::errors::CmdError;
use crate::page::PageType;
use crate::varint::{decode_serial_type, read_varint};
use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

/// The `.dbinfo` dot command.
pub fn dot_dbinfo(db_file_path: &str) -> Result<()> {
    println!("database page size: {}", page_size(db_file_path)?);
    eprintln!("number of pages: {}", num_pages(db_file_path)?);
    println!(
        "number of tables: {}",
        tables_meta(db_file_path)?.num_tables
    );

    Ok(())
}

/// The `.tables` dot command.
///
/// Prints the names of the user tables in a SQLite database.
pub fn dot_tables(db_file_path: &str) -> Result<Vec<String>, CmdError> {
    Ok(tables_meta(db_file_path)?.table_names)
}

/// Returns the page size of a SQLite database.
fn page_size(db_file_path: &str) -> Result<u16> {
    let mut db_file = File::open(db_file_path)?;
    let mut header = HEADER;
    db_file.read_exact(&mut header)?;

    // The page size is stored at the 16th byte offset in database header, using 2 bytes in big-endian order.
    Ok(u16::from_be_bytes([header[16], header[17]]))
}

/// Returns the number of pages in a SQLite database.
fn num_pages(db_file_path: &str) -> Result<u32> {
    let mut db_file = File::open(db_file_path)?;
    let mut header = HEADER;
    db_file.read_exact(&mut header)?;

    // Number of pages is stored at the 28th byte offset in database header, using 4 bytes in big-endian order.
    // It is only valid if "File change counter" (offset 24, 4 bytes) is equal to
    // the "version-valid-for number" (offset 92, 4 bytes).
    // Source: https://torymur.github.io/sqlite-repr/
    if u32::from_be_bytes([header[24], header[25], header[26], header[27]])
        == u32::from_be_bytes([header[92], header[93], header[94], header[95]])
    {
        Ok(u32::from_be_bytes([
            header[28], header[29], header[30], header[31],
        ]))
    } else {
        // In all modern versions of SQLite databases, the number of pages should be recorded in the header.
        // If this information is blank, the number of pages can be assumed to be the size of the file divided
        // by the page size. Source: https://askclees.com/2020/11/20/sqlite-databases-at-hex-level/
        let file_len = db_file.metadata()?.len();
        let page_size = u16::from_be_bytes([header[16], header[17]]) as u64;
        Ok((file_len / page_size) as u32)
    }
}

struct TablesMeta {
    /// Total number of tables
    num_tables: u32,
    /// Names of user tables
    table_names: Vec<String>,
}

/// Returns the tables metadata in a SQLite database.
///
/// Currently, we only support one level of `PageType::InteriorTable`.
// TODO: Perhaps traverse the entire B-Tree properly. Keep in mind that page 1 is different than other pages.
fn tables_meta(db_file_path: &str) -> Result<TablesMeta, CmdError> {
    let mut db_file = File::open(db_file_path)?;
    let mut _pos = db_file.seek(SeekFrom::Start(HEADER.len() as u64))?;

    let mut total_num_cells = 0;
    let mut table_names = Vec::<String>::new();

    let page_size = page_size(db_file_path)?;
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
        other => return Err(CmdError::UnsupportedPageType(other)),
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
                        total_num_cells += 1;
                        get_table_name(&mut db_file, &mut table_names)?;
                    }
                    other => return Err(CmdError::UnsupportedPageType(other)),
                };
            }
            PageType::LeafTable => {
                total_num_cells += 1;
                get_table_name(&mut db_file, &mut table_names)?;
            }
            other => panic!("Page type {other:?} encountered where it shouldn't!"),
        }
    }

    Ok(TablesMeta {
        num_tables: total_num_cells,
        table_names,
    })
}

/// Reads a table name and adds it to `table_names`.
fn get_table_name(db_file: &mut File, table_names: &mut Vec<String>) -> Result<()> {
    // We need to read all varints that come before the table name that we're trying to get,
    // because varints are of variable length, hence we don't know in advance how many bytes to skip
    // to get to the table name, i.e., we don't know the offset - it is not fixed.

    // Read a varint which is the total number of bytes of payload, including any overflow.
    let (_payload_size, _) = read_varint(db_file)?;
    // Read a varint which is the integer key, a.k.a. "rowid".
    let (_row_id, _) = read_varint(db_file)?;

    // Records are stored in [record format](https://www.sqlite.org/fileformat.html#record_format).

    // Record header

    // Read a varint which is the size of the record header.
    let (_record_header_size, _) = read_varint(db_file)?;
    // Serial type for sqlite_schema.type (varint) -> Size of sqlite_schema.type:
    let schema_type = decode_serial_type(read_varint(db_file)?.0)?;
    // Serial type for sqlite_schema.name (varint) -> Size of sqlite_schema.name:
    let schema_name = decode_serial_type(read_varint(db_file)?.0)?;
    // Serial type for sqlite_schema.tbl_name (varint) -> Size of sqlite_schema.tbl_name:
    let schema_tbl_name = decode_serial_type(read_varint(db_file)?.0)?;
    // Serial type for sqlite_schema.rootpage (varint) -> 8-bit twos-complement integer:
    let _rootpage = decode_serial_type(read_varint(db_file)?.0)?;
    // Serial type for sqlite_schema.sql (varint) -> Size of sqlite_schema.sql:
    let _schema_sql = decode_serial_type(read_varint(db_file)?.0)?;
    eprintln!(
        "{} {} {} {} {} {} {} {}",
        _payload_size,
        _row_id,
        _record_header_size,
        schema_type,
        schema_name,
        schema_tbl_name,
        _rootpage,
        _schema_sql
    ); // todo rem

    // Record body
    // [The Schema Table](https://www.sqlite.org/schematab.html)
    // We are looking for "tbl_name", which is the third element.

    let _pos = db_file.seek(SeekFrom::Current(schema_type + schema_name))?;
    let mut buf = [0u8; TABLE_NAME_LEN];
    db_file.read_exact(&mut buf)?;
    let tbl_name = String::from_utf8(Vec::from(&buf[0..schema_tbl_name as usize]))?;
    if tbl_name != *"sqlite_sequence" {
        table_names.push(tbl_name);
    }

    Ok(())
}
