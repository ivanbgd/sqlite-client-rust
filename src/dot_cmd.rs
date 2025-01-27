//! # Dot Command Handlers
//!
//! [3. Special commands to sqlite3 (dot-commands)](https://www.sqlite.org/cli.html#special_commands_to_sqlite3_dot_commands_)
//!
//! [Database File Format](https://www.sqlite.org/fileformat.html)

use crate::constants::HEADER;
use crate::errors::DotCmdError;
use crate::tables;
use anyhow::Result;
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

    Ok(table_names)
}

/// Returns the page size of a SQLite database.
pub(crate) fn page_size(db_file_path: &str) -> Result<u16> {
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
