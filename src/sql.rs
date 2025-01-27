//! SQL Command Handlers
//!
//! [SQL As Understood By SQLite](https://www.sqlite.org/lang.html)

use crate::dot_cmd;
use crate::errors::SqlError;
use crate::tables::get_tables_meta;
use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// SQL `SELECT` command
///
/// `SELECT` itself and its subcommands are case-insensitive.
///
/// Supports:
/// - `COUNT(*) FROM <table>`
pub fn select(db_file_path: &str, command: &str) -> Result<String> {
    let (_, cmd) = command
        .split_once(" ")
        .ok_or_else(|| SqlError::SelectIncomplete)?;
    let (cmd, arg) = cmd
        .rsplit_once(" ")
        .ok_or_else(|| SqlError::SelectIncomplete)?;
    let arg = arg.strip_suffix(';').unwrap_or(arg);

    match cmd.to_lowercase().as_str() {
        "count(*) from" => {
            // Count rows in a table.
            if arg.trim() == "" {
                Err(SqlError::SelectIncomplete)?
            }
            let page_size = dot_cmd::page_size(db_file_path)?;
            let tables_meta = get_tables_meta(db_file_path)?;
            for table in tables_meta.0 {
                if table.tbl_name.eq_ignore_ascii_case(arg) {
                    // We've found the requested table, `arg`.
                    // Now we need to jump to its page and read the number of cells on the page from the page header.
                    let rootpage = table.rootpage;
                    // Pages are numbered from one, i.e., they are 1-indexed, so subtract one to get to the page.
                    let page_offset = (rootpage - 1) * page_size as u64;
                    // The two-byte integer at offset 3 gives the number of cells on the page.
                    let offset = page_offset + 3;
                    let mut db_file = File::open(db_file_path)?;
                    let mut buf = [0u8; 2];
                    let _pos = db_file.seek(SeekFrom::Start(offset))?;
                    db_file.read_exact(&mut buf)?;
                    // All multibyte values in the page header are big-endian.
                    let num_rows = u16::from_be_bytes(buf).to_string();
                    return Ok(num_rows);
                }
            }
            // In case a table with the given name, `arg`, does not exist in the database, return that error.
            Err(SqlError::MissingTable(arg.to_string()))?
        }
        _ => Err(SqlError::SelectUnsupported(cmd.to_string()))?,
    }

    Err(SqlError::SelectUnsupported(cmd.to_string()))?
}
