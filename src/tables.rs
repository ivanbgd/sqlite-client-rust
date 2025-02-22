//! Tables and related functions
//!
//! [1.6. B-tree Pages](https://www.sqlite.org/fileformat.html#b_tree_pages)
//!
//! The b-tree corresponding to the sqlite_schema table is always a table b-tree and always has a root page of 1.
//! The sqlite_schema table contains the root page number for every other table and index in the database file.
//!
//! Each entry in a table b-tree consists of a 64-bit signed integer key and up to 2147483647 bytes of arbitrary data.
//! (The key of a table b-tree corresponds to the rowid of the SQL table that the b-tree implements.)
//! Interior table b-trees hold only keys and pointers to children. All data is contained in the table b-tree leaves.

use crate::constants::SCHEMA_TABLE_FIELD_LEN;
use crate::dot_cmd;
use crate::errors::DotCmdError;
use crate::page::{Page, PageType};
use crate::serial_type::{
    get_serial_type_integer, read_serial_type_integer, serial_type_to_content_size,
};
use crate::varint::{get_varint, read_varint};
use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// A list of metadata for all tables in the database.
///
/// The metadata are extracted from the schema table.
///
/// This is a tuple-struct that contains a hashmap of metadata for each table in DB, keyed by table name.
#[derive(Debug)]
pub(crate) struct TablesMeta(pub(crate) HashMap<String, SchemaTable>);

impl TablesMeta {
    /// Returns database objects' (tables and indexes) metadata that are stored in the `sqlite_schema` table.
    pub(crate) fn get_objects(self) -> Vec<SchemaTable> {
        let mut objects = Vec::with_capacity(self.0.len());
        for (_obj_name, object) in self.0 {
            objects.push(object);
        }
        objects
    }

    #[allow(unused)]
    /// Returns names of database objects (tables and indexes).
    ///
    /// These are the `name` fields, not the `tbl_name` fields.
    pub(crate) fn get_object_names(self) -> Vec<String> {
        let mut obj_names = Vec::with_capacity(self.0.len());
        for (obj_name, _obj) in self.0 {
            obj_names.push(obj_name);
        }
        obj_names
    }
}

/// Every SQLite database contains a single "schema table" that stores the schema for that database.
/// The schema for a database is a description of all of the other tables, indexes, triggers, and views that
/// are contained within the database. The schema table looks like this:
///
///     CREATE TABLE sqlite_schema(
///       type text,
///       name text,
///       tbl_name text,
///       rootpage integer,
///       sql text
///     );
///
/// The sqlite_schema table contains one row for each table, index, view, and trigger (collectively "objects")
/// in the schema, except there is no entry for the sqlite_schema table itself.
///
/// [The Schema Table](https://www.sqlite.org/schematab.html)
///
/// [2.6. Storage Of The SQL Database Schema](https://www.sqlite.org/fileformat2.html#ffschema)
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct SchemaTable {
    pub(crate) tbl_type: String,
    pub(crate) name: String,
    pub(crate) tbl_name: String,
    pub(crate) rootpage: u32,
    pub(crate) sql: String,
}

impl SchemaTable {
    fn new(tbl_type: String, name: String, tbl_name: String, rootpage: u32, sql: String) -> Self {
        Self {
            tbl_type,
            name,
            tbl_name,
            rootpage,
            sql,
        }
    }
}

/// Returns the tables metadata in an SQLite database.
///
/// The metadata are stored and read from the `sqlite_schema` table.
///
/// The b-tree corresponding to the sqlite_schema table is always a table b-tree and always has a root page of 1.
/// The sqlite_schema table contains the root page number for every other table and index in the database file.
pub(crate) fn get_tables_meta(db_file_path: &str) -> Result<TablesMeta, DotCmdError> {
    let mut tables_meta = TablesMeta(HashMap::new());

    let page_size = dot_cmd::page_size(db_file_path)?;
    let mut db_file = File::open(db_file_path)?;

    // Read in the page metadata.
    // We start with the (database) root page, whose page number is 1. Only page 1 contains a database header.
    // Page header for page 1 comes right after the DB header.
    // So, the type of page 1 is stored in the first byte after the DB header.
    // The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
    let page_num = 1u32;

    let page = Page::new(&mut db_file, page_size, page_num)?;

    traverse_btree(
        &mut db_file,
        page_size,
        page,
        &parse_schema_table,
        &mut tables_meta,
    )?;

    Ok(tables_meta)
}

type VisitType = dyn Fn(&Page, u16, &mut TablesMeta) -> Result<()>;

/// Traverses a B-tree, visiting each cell.
///
/// A node is a page.
///
/// Starts with the provided page, which, in general case, doesn't have to be the root page
/// of the entire database, but it should be a root page of a table.
fn traverse_btree(
    db_file: &mut File,
    page_size: u32,
    page: Page,
    visit_fn: &VisitType,
    tables_meta: &mut TablesMeta,
) -> Result<()> {
    in_order_rec_sqlite_schema(db_file, page_size, page, visit_fn, tables_meta)
}

/// Recursive implementation of in-order traversal for B-tree
///
/// Used for traversing the sqlite_schema table B-tree.
///
/// The b-tree corresponding to the sqlite_schema table is always a table b-tree and always has a root page of 1.
/// The sqlite_schema table contains the root page number for every other table and index in the database file.
fn in_order_rec_sqlite_schema(
    db_file: &mut File,
    page_size: u32,
    page: Page,
    visit_fn: &VisitType,
    tables_meta: &mut TablesMeta,
) -> Result<()> {
    let page_header = page.get_header();
    let page_type = page_header.page_type;

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            for cell_ptr in page.get_cell_ptr_array() {
                visit_fn(&page, cell_ptr, tables_meta)?;
            }
        }
        PageType::TableInterior => {
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                let page_num = &page.contents[cell_ptr as usize..][..4];
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(page_num.try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                in_order_rec_sqlite_schema(db_file, page_size, left_child, visit_fn, tables_meta)?;
            }
        }
        // The b-tree corresponding to the sqlite_schema table is always a table b-tree.
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }
    // Visit the rightmost child.
    if page_type == PageType::TableInterior {
        let page_num = page_header
            .rightmost_ptr
            .expect("Expected table interior page type.");
        // Rightmost pointer is page number of the rightmost child.
        let right_child = Page::new(db_file, page_size, page_num)?;
        in_order_rec_sqlite_schema(db_file, page_size, right_child, visit_fn, tables_meta)?;
    }

    Ok(())
}

/// Gets a table type, name, rootpage and sql from memory, and adds them to `tables_meta`.
///
/// Reads from the given `offset` in the page - the `offset` is relative to the page contents start.
///
/// Works on an individual table.
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
fn parse_schema_table(page: &Page, mut offset: u16, tables_meta: &mut TablesMeta) -> Result<()> {
    let cont = &*page.contents;

    // We need to read all varints that come before the table name that we're trying to get,
    // because varints are of variable length, hence we don't know in advance how many bytes to skip
    // to get to the table name, i.e., we don't know the offset - it is not fixed.

    // Read a varint which is the total number of bytes of payload, including any overflow.
    let _payload_size = get_varint(page, &mut offset)?;
    // Read a varint which is the integer key, a.k.a. "rowid".
    let _row_id = get_varint(page, &mut offset)?;

    // Record header

    // Read a varint which is the size of the record header.
    let _record_header_size = get_varint(page, &mut offset)?;
    // Serial type for sqlite_schema.type (varint) -> Size of sqlite_schema.type:
    let value = get_varint(page, &mut offset)?;
    let schema_type = serial_type_to_content_size(value)? as usize;
    // Serial type for sqlite_schema.name (varint) -> Size of sqlite_schema.name:
    let value = get_varint(page, &mut offset)?;
    let schema_name = serial_type_to_content_size(value)? as usize;
    // Serial type for sqlite_schema.tbl_name (varint) -> Size of sqlite_schema.tbl_name:
    let value = get_varint(page, &mut offset)?;
    let schema_tbl_name = serial_type_to_content_size(value)? as usize;
    // Serial type for sqlite_schema.rootpage (varint) -> A twos-complement integer.
    // For example, if 1, content size is 1 and the value is an 8-bit twos-complement integer.
    let value = get_varint(page, &mut offset)?;
    let schema_rootpage = serial_type_to_content_size(value)? as usize;
    // Serial type for sqlite_schema.sql (varint) -> Size of sqlite_schema.sql:
    let value = get_varint(page, &mut offset)?;
    let schema_sql = serial_type_to_content_size(value)? as usize;

    // Record body

    let mut offset = offset as usize;

    // Schema table type
    let tbl_type = String::from_utf8(Vec::from(&cont[offset..][..schema_type]))?.to_lowercase();
    offset += schema_type;

    // Schema name
    let name = String::from_utf8(Vec::from(&cont[offset..][..schema_name]))?.to_lowercase();
    offset += schema_name;

    // Schema table name
    let tbl_name = String::from_utf8(Vec::from(&cont[offset..][..schema_tbl_name]))?.to_lowercase();
    offset += schema_tbl_name;

    // Schema rootpage
    let rootpage = get_serial_type_integer(page, offset as u16, schema_rootpage as u64)?.0 as u32;
    offset += schema_rootpage;

    // Schema sql
    let sql = String::from_utf8(Vec::from(&cont[offset..][..schema_sql as usize]))?.to_lowercase();

    // Add schema for this table to the list.
    tables_meta.0.insert(
        name.clone(),
        SchemaTable::new(tbl_type, name, tbl_name, rootpage, sql),
    );

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////
//                                  File Variants                                         //
////////////////////////////////////////////////////////////////////////////////////////////

type VisitTypeFile = dyn Fn(&mut File, &mut TablesMeta) -> Result<()>;

#[allow(unused)]
/// Recursive implementation of in-order traversal for B-tree
fn in_order_rec_from_file(
    db_file: &mut File,
    page_size: u32,
    page: Page,
    visit_fn: &VisitTypeFile,
    tables_meta: &mut TablesMeta,
) -> Result<()> {
    let page_start = ((page.page_num - 1) * page_size) as u64;
    let page_header = page.get_header();
    let page_type = page_header.page_type;

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            for cell_ptr in page.get_cell_ptr_array() {
                let _pos = db_file.seek(SeekFrom::Start(page_start + cell_ptr as u64))?;
                visit_fn(db_file, tables_meta)?;
            }
        }
        PageType::TableInterior => {
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                let _pos = db_file.seek(SeekFrom::Start(page_start + cell_ptr as u64))?;
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let mut buf = [0u8; 4];
                db_file.read_exact(&mut buf)?;
                let page_num = u32::from_be_bytes(buf);
                let left_child = Page::new(db_file, page_size, page_num)?;
                in_order_rec_from_file(db_file, page_size, left_child, visit_fn, tables_meta)?;
            }
        }
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }
    // Visit the rightmost child.
    if page_type == PageType::TableInterior {
        let page_num = page_header.rightmost_ptr.expect("Expected interior table.");
        // Rightmost pointer is page number of the rightmost child.
        let right_child = Page::new(db_file, page_size, page_num)?;
        in_order_rec_from_file(db_file, page_size, right_child, visit_fn, tables_meta)?;
    }

    Ok(())
}

#[allow(unused)]
/// Reads a table type, name, rootpage and sql from a database file, and adds them to `tables_meta`.
///
/// Works on an individual table.
///
/// This function expects the file pointer to be at the beginning of the schema table.
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
fn parse_schema_table_from_file(db_file: &mut File, tables_meta: &mut TablesMeta) -> Result<()> {
    // eprintln!("{:08x}", db_file.stream_position()?);

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
    let schema_sql = serial_type_to_content_size(read_varint(db_file)?.0)? as u64;

    // Record body

    let mut offset = db_file.stream_position()?; // Record body start, i.e., current position.
    let mut buf = [0u8; SCHEMA_TABLE_FIELD_LEN];

    // Schema table type
    let _pos = db_file.seek(SeekFrom::Start(offset))?; // Not needed in this place, but kept for consistency.
    db_file.read_exact(&mut buf)?;
    let tbl_type = String::from_utf8(Vec::from(&buf[0..schema_type as usize]))?.to_lowercase();

    // Schema name
    offset += schema_type;
    let _pos = db_file.seek(SeekFrom::Start(offset))?;
    db_file.read_exact(&mut buf)?;
    let name = String::from_utf8(Vec::from(&buf[0..schema_name as usize]))?.to_lowercase();

    // Schema table name
    offset += schema_name;
    let _pos = db_file.seek(SeekFrom::Start(offset))?;
    db_file.read_exact(&mut buf)?;
    let tbl_name = String::from_utf8(Vec::from(&buf[0..schema_tbl_name as usize]))?.to_lowercase();

    // Schema rootpage
    offset += schema_tbl_name;
    let _pos = db_file.seek(SeekFrom::Start(offset))?;
    let rootpage = read_serial_type_integer(db_file, schema_rootpage)?.0 as u32;

    // Schema sql
    offset += schema_rootpage;
    let _pos = db_file.seek(SeekFrom::Start(offset))?;
    db_file.read_exact(&mut buf)?;
    let sql = String::from_utf8(Vec::from(&buf[0..schema_sql as usize]))?.to_lowercase();

    // Add schema for this table to the list.
    tables_meta.0.insert(
        name.clone(),
        SchemaTable::new(tbl_type, name, tbl_name, rootpage, sql),
    );

    Ok(())
}
