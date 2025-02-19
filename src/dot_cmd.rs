//! # Dot Command Handlers
//!
//! [3. Special commands to sqlite3 (dot-commands)](https://www.sqlite.org/cli.html#special_commands_to_sqlite3_dot_commands_)
//!
//! [Database File Format](https://www.sqlite.org/fileformat.html)

use crate::constants::DB_HEADER;
use crate::errors::DotCmdError;
use crate::tables;
use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

/// The `.dbinfo` dot command.
pub fn dot_dbinfo(db_file_path: &str) -> Result<()> {
    println!("database page size: {}", page_size(db_file_path)?);
    eprintln!("number of pages: {}", num_pages(db_file_path)?);
    println!(
        "number of tables: {}",
        tables::get_tables_meta(db_file_path)?.0.len()
    );
    eprintln!("text encoding: {}", text_encoding(db_file_path)?);

    Ok(())
}

/// The `.tables` dot command.
///
/// Prints the names of the user tables in a SQLite database.
pub fn dot_tables(db_file_path: &str) -> Result<Vec<String>, DotCmdError> {
    let tbl_names = tables::get_tables_meta(db_file_path)?.get_tbl_names();
    let mut table_names = vec![];
    for tbl_name in tbl_names {
        if tbl_name != *"sqlite_sequence" {
            table_names.push(tbl_name);
        }
    }
    table_names.sort();

    Ok(table_names)
}

/// Returns the page size in bytes of a SQLite database.
///
/// The size of a page is a power of two between 512 and 65536 inclusive.
///
/// The page size for a database file is determined by the 2-byte integer located at
/// an offset of 16 bytes from the beginning of the database file.
///
/// The database page size must be a power of two between 512 and 32768 inclusive,
/// or the value 1 representing a page size of 65536.
///
/// The value 65536 will not fit in a two-byte integer, so to specify a 65536-byte page size,
/// the value at offset 16 is 0x00 0x01.
///
/// https://www.sqlite.org/fileformat.html#page_size
pub(crate) fn page_size(db_file_path: &str) -> Result<u32> {
    let mut db_file = File::open(db_file_path)?;
    let mut header = DB_HEADER;
    db_file.read_exact(&mut header)?;

    // The page size is stored at the 16th byte offset in database header, using 2 bytes in big-endian order.
    let mut page_size = u16::from_be_bytes([header[16], header[17]]) as u32;
    if page_size == 1 {
        page_size = 65536;
    }

    Ok(page_size)
}

/// Returns the number of pages in a SQLite database.
fn num_pages(db_file_path: &str) -> Result<u32> {
    let mut db_file = File::open(db_file_path)?;
    let mut header = DB_HEADER;
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

/// Returns the text encoding of a SQLite database.
///
/// https://www.sqlite.org/fileformat2.html#enc
///
/// The text encoding is stored at the 56th byte offset in database header, using 4 bytes in big-endian order.
pub(crate) fn text_encoding(db_file_path: &str) -> Result<String> {
    let mut db_file = File::open(db_file_path)?;
    let mut header = DB_HEADER;
    db_file.read_exact(&mut header)?;

    let enc = u32::from_be_bytes([header[56], header[57], header[58], header[59]]);
    match enc {
        1 => Ok("utf-8".to_string()),
        2 => Ok("utf-16le".to_string()),
        3 => Ok("utf-16be".to_string()),
        v => bail!("The value {v} for text encoding is not allowed!"),
    }
}
