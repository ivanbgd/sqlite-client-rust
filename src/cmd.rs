//! # Command Handlers
//!
//! [Database File Format](https://www.sqlite.org/fileformat.html)

use crate::constants::HEADER;
use crate::errors::CmdError;
use crate::page::PageType;
use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

pub fn dbinfo(db_file_path: &str) -> Result<()> {
    println!("database page size: {}", page_size(db_file_path)?);
    eprintln!("number of pages: {}", num_pages(db_file_path)?);
    println!("number of tables: {}", num_tables(db_file_path)?);

    Ok(())
}

fn page_size(db_file_path: &str) -> Result<u16> {
    let mut db_file = File::open(db_file_path)?;
    let mut header = HEADER;
    db_file.read_exact(&mut header)?;

    // The page size is stored at the 16th byte offset in database header, using 2 bytes in big-endian order.
    Ok(u16::from_be_bytes([header[16], header[17]]))
}

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

// TODO: Perhaps traverse the entire B-Tree properly. Keep in mind that page 1 is different than other pages.
/// Currently, we only support one level of `PageType::InteriorTable`.
fn num_tables(db_file_path: &str) -> Result<u32, CmdError> {
    let mut db_file = File::open(db_file_path)?;
    let mut _pos = db_file.seek(SeekFrom::Start(HEADER.len() as u64))?;

    let mut total_num_cells = 0;

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

    _pos = db_file.seek(SeekFrom::Current(2))?;
    let mut page_num_cells = [0u8; 2];
    db_file.read_exact(&mut page_num_cells)?;
    let page_num_cells = u16::from_be_bytes(page_num_cells) as u32;

    // eprintln!("page_header_len = {page_header_len}"); // todo rem
    // eprintln!("page_num_cells = {page_num_cells}"); // todo rem
    // eprintln!("page_type = {page_type:?}"); // todo rem
    if page_type == PageType::LeafTable {
        total_num_cells += page_num_cells;
        return Ok(total_num_cells);
    }

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
    cell_pointer_array.reverse();

    let page_size = page_size(db_file_path)?;
    // eprintln!("page_size = {page_size:?}"); // todo rem

    let page_num = 1;
    let page_start = (page_num - 1) * page_size as u32;

    // Visit each cell.
    for cell_ptr in cell_pointer_array {
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
                    PageType::InteriorTable => {
                        todo!() // TODO: Repeat the process.
                    }
                    PageType::LeafTable => {
                        total_num_cells += 1;
                    }
                    other => return Err(CmdError::UnsupportedPageType(other)),
                };
            }
            other => panic!("Page type {other:?} encountered where it shouldn't!"),
        }
    }

    Ok(total_num_cells)
}
