//! SQL Command Handlers
//!
//! [SQL As Understood By SQLite](https://www.sqlite.org/lang.html)
//!
//! [Database File Format](https://www.sqlite.org/fileformat.html)
//!
//! [B-tree Pages](https://www.sqlite.org/fileformat.html#b_tree_pages)
//!
//! [ROWIDs and the INTEGER PRIMARY KEY](https://www.sqlite.org/lang_createtable.html#rowid)

use crate::constants::{
    ColumnNameOrd, VarintType, COUNT_PATTERN, FROM_PATTERN, SELECT_PATTERN, WHERE_PATTERN,
};
use crate::dot_cmd;
use crate::errors::SqlError;
use crate::page::{Page, PageType};
use crate::serial_type::{
    get_serial_type_to_content, serial_type_to_content_size, SerialTypeValue,
};
use crate::tables::{get_tables_meta, SchemaTable};
use crate::varint::get_varint;
use anyhow::Result;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fs::File;
use std::iter::zip;

/// Columns names, `WHERE` arguments (column name and value), table name, number of rows
type CliArgsRest<'a> = (
    Cow<'a, str>,
    Option<(Cow<'a, str>, Cow<'a, str>)>,
    Cow<'a, str>,
    u64,
);

/// SQL `SELECT` command
///
/// `SELECT` itself, its subcommands, column and table names are case-insensitive.
///
/// Column and table names can be wrapped in double-quotes.
///
/// Column names can be provided in arbitrary order and can be repeated.
///
/// Semicolon at the end of the statement is supported but optional.
///
/// `WHERE` and `LIMIT` are optional.
///
/// `WHERE` clause's value part is case-sensitive, i.e., the part that comes after the equals sign.
///
/// Supports:
/// - `SELECT COUNT(*) FROM <table>`
/// - `SELECT <column> FROM <table> LIMIT <limit>`
/// - `SELECT <column1>, <column2>, ... <columnN> FROM <table> LIMIT <limit>;`
/// - `SELECT <column1>, <column2>, ... <columnN> FROM <table> WHERE <where_clause> LIMIT <limit>`
/// - `SELECT * FROM <table> WHERE <where_clause> LIMIT <limit>`
///
/// [SELECT](https://www.sqlite.org/lang_select.html)
pub fn select(db_file_path: &str, mut command: &str) -> Result<Vec<String>> {
    command = command.trim();
    assert!(command.to_lowercase().starts_with(SELECT_PATTERN));
    let (_select, mut rest) = command
        .split_once(' ')
        .ok_or_else(|| SqlError::SelectIncomplete)?;
    rest = rest.trim().strip_suffix(';').unwrap_or(rest).trim();

    if rest.to_lowercase().contains(FROM_PATTERN) {
        if rest.to_lowercase().starts_with(COUNT_PATTERN) {
            // `SELECT COUNT(*) FROM <table>` - Count rows in a table.
            rest = rest[COUNT_PATTERN.len()..].trim();
            if rest.to_lowercase().starts_with(FROM_PATTERN) {
                rest = rest[FROM_PATTERN.len()..].trim();
                if rest.is_empty() {
                    Err(SqlError::SelectIncomplete)?
                }
                return Ok(vec![
                    select_count_rows_in_table(db_file_path, rest)?.to_string()
                ]);
            } else {
                Err(SqlError::SelectIncomplete)?
            }
        } else {
            // `SELECT * FROM <table> WHERE <where_clause> LIMIT <limit>`
            // `SELECT <column1>, ..., <column_n> FROM <table> WHERE <where_clause> LIMIT <limit>`
            let mut cli_args_rest = parse_cli_args_rest(db_file_path, rest)?;
            let column_names = &mut cli_args_rest.0;
            let table_name = &cli_args_rest.2;
            let tables_meta = get_tables_meta(db_file_path)?;
            if !tables_meta.0.contains_key(&**table_name) {
                // In case a table with the given name, `table_name`, does not exist in the database, return that error.
                Err(SqlError::NoSuchTable(table_name.to_string()))?
            }
            let table = &tables_meta.0[&**table_name];
            let all_cols = get_all_columns_names(table);
            let all_cols = &all_cols.join(",");
            if column_names.trim().contains("*") {
                *column_names = column_names.replace("*", all_cols).into();
            }
            return select_columns_from_table(db_file_path, cli_args_rest);
        }
    }

    Err(SqlError::SelectUnsupported(command.to_string()))?
}

/// `SELECT COUNT(*) FROM <table>`
///
/// Returns error if the table does not exist.
fn select_count_rows_in_table(db_file_path: &str, table_name: &str) -> Result<u64> {
    let table_name = &table_name.to_lowercase();
    let tables_meta = get_tables_meta(db_file_path)?;

    if tables_meta.0.contains_key(table_name) {
        // We've found the requested table, `table_name`.
        let table = &tables_meta.0[table_name];
        // Now we need to jump to its pages and read the requested data. We start with its root page.
        let page_num = table.rootpage;
        let page_size = dot_cmd::page_size(db_file_path)?;
        let mut db_file = File::open(db_file_path)?;
        let page = Page::new(&mut db_file, page_size, page_num)?;
        let num_rows = count_rows_in_order_rec(&mut db_file, page_size, page)?;
        Ok(num_rows)
    } else {
        // In case a table with the given name, `table_name`, does not exist in the database, return that error.
        Err(SqlError::NoSuchTable(table_name.to_string()))?
    }
}

/// Recursive implementation of in-order traversal for B-tree, for counting rows in a table
///
/// Traverses a B-tree, visiting each node (page).
///
/// Starts with the provided page, which, in general case, doesn't have to be the root page
/// of the entire database, but it should be a root page of a table.
///
/// Returns total number of data rows in a table, i.e., the total number of cells in table leaf pages.
///
/// Table interior pages don't store data. Their cells store pointers and don't count toward data rows.
fn count_rows_in_order_rec(db_file: &mut File, page_size: u32, page: Page) -> Result<u64> {
    let mut num_rows = 0u64;

    // let page_start = (page.page_num - 1) * page_size;
    let page_header = page.get_header();
    let page_type = page_header.page_type;
    let cont = &page.contents;
    assert_eq!(
        page_header.num_cells as usize,
        page.get_cell_ptr_array().len()
    );

    // We are keeping the two index types below for completeness, but they don't occur here.
    // From official documentation: "All pages within each complete b-tree are of the same type: either table or index."
    assert!(page_type.eq(&PageType::TableLeaf) || page_type.eq(&PageType::TableInterior));

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            return Ok(page_header.num_cells as u64);
        }
        PageType::TableInterior => {
            // Visit the rightmost child first, for performance reasons.
            // eprintln!("R {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            let page_num = page_header
                .rightmost_ptr
                .expect("Expected PageType::TableInterior.");
            // Rightmost pointer is page number of the rightmost child.
            let right_child = Page::new(db_file, page_size, page_num)?;
            num_rows += count_rows_in_order_rec(db_file, page_size, right_child)?;

            // Visit left children.
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(cont[cell_ptr as usize..][..4].try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                num_rows += count_rows_in_order_rec(db_file, page_size, left_child)?;
            }
        }
        PageType::IndexLeaf => {
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            return Ok(page_header.num_cells as u64);
        }
        PageType::IndexInterior => {
            // Visit the rightmost child first, for performance reasons.
            // eprintln!("R {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            let page_num = page_header
                .rightmost_ptr
                .expect("Expected PageType::IndexInterior.");
            // Rightmost pointer is page number of the rightmost child.
            let right_child = Page::new(db_file, page_size, page_num)?;
            num_rows += count_rows_in_order_rec(db_file, page_size, right_child)?;

            // Visit left children.
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                // Cell contains: (Page number of left child, Number of bytes of payload, Payload, ...) as (u32, varint, byte array, ...).
                let page_num = u32::from_be_bytes(cont[cell_ptr as usize..][..4].try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                num_rows += count_rows_in_order_rec(db_file, page_size, left_child)? + 1;
            }
        }
    }

    Ok(num_rows)
}

/// Returns all columns' names, `WHERE` arguments (column name and value), table name and number of rows.
///
/// - `SELECT <column_1>, ..., <column_n> FROM <table> WHERE <where_clause> LIMIT <limit>`
/// - `SELECT * FROM <table> WHERE <where_clause> LIMIT <limit>`
///
/// Column names can be provided in arbitrary order and can be repeated.
fn parse_cli_args_rest<'a>(db_file_path: &str, mut rest: &str) -> Result<CliArgsRest<'a>> {
    let from_pos = rest
        .to_lowercase()
        .find(FROM_PATTERN)
        .ok_or_else(|| SqlError::SelectIncomplete)?;
    let column_names = &rest[..from_pos];
    rest = rest[from_pos + FROM_PATTERN.len()..].trim();
    if rest.is_empty() {
        Err(SqlError::SelectIncomplete)?
    }

    let where_args = get_where_args(rest)?.map(|(column, value)| (column.to_lowercase(), value));

    let mut rest = rest.trim().trim_matches('"').to_lowercase();

    if let Some((ref where_col_name, where_col_value)) = where_args {
        rest = rest.replace(WHERE_PATTERN, "");
        rest = rest.replace(where_col_name, "");
        rest = rest.replace('=', "");
        rest = rest.replace(&format!("'{}'", where_col_value.to_lowercase()), "");
    }

    let (mut table_name, limit) = rest.split_once("limit").unwrap_or((&rest, ""));
    table_name = table_name.trim().trim_matches('"');
    let num_rows = if !limit.is_empty() {
        let limit = limit
            .trim()
            .parse::<u64>()
            .map_err(|_| SqlError::LimitParsingError(limit.trim().to_string()))?;
        limit.min(select_count_rows_in_table(db_file_path, table_name)?)
    } else {
        select_count_rows_in_table(db_file_path, table_name)?
    };

    let where_args = where_args.map(|(name, value)| (name.into(), value.to_string().into()));

    Ok((
        column_names.to_string().into(),
        where_args,
        table_name.to_string().into(),
        num_rows,
    ))
}

/// Returns the result of a general `SELECT` query, i.e., all matching rows.
///
/// `SELECT <column_1>, ..., <column_n> FROM <table> WHERE <where_clause> LIMIT <limit>`
///
/// `SELECT * FROM <table> WHERE <where_clause> LIMIT <limit>`
///
/// Column names can be provided in arbitrary order and can be repeated.
///
/// Supports indexed tables.
///
/// Returns error if the table, or at least one of the columns, doesn't exist.
fn select_columns_from_table(
    db_file_path: &str,
    cli_args_rest: CliArgsRest,
) -> Result<Vec<String>> {
    let (column_names, where_args, table_name, num_rows) = cli_args_rest;

    let tables_meta = get_tables_meta(db_file_path)?;
    // dbg!(&tables_meta);

    if !tables_meta.0.contains_key(&*table_name) {
        // In case a table with the given name, `table_name`, does not exist in the database, return that error.
        Err(SqlError::NoSuchTable(table_name.to_string()))?
    }

    let page_size = dot_cmd::page_size(db_file_path)?;
    let db_file = &mut File::open(db_file_path)?;

    let mut result = Vec::with_capacity(num_rows as usize);
    let mut is_indexed = false;

    let mut indexed_table = &SchemaTable::new(
        "".to_string(),
        "".to_string(),
        "".to_string(),
        0,
        "".to_string(),
    );
    for object in tables_meta.0.values() {
        if object.name == table_name && object.tbl_type == "table" && where_args.is_some() {
            indexed_table = object;
            break;
        }
    }
    for object in tables_meta.0.values() {
        // We have to compare by table names, and not over names, because both table and index types for
        // an indexed table have the same table name, and we differentiate between them using table type.
        // They are stored in the hash map using their names as keys.
        if object.tbl_name == table_name && object.tbl_type == "index" && where_args.is_some() {
            is_indexed = true;
            let index_page_num = object.rootpage;
            let index_page = Page::new(db_file, page_size, index_page_num)?;
            indexed_select_where(
                db_file,
                page_size,
                &index_page,
                &column_names,
                &where_args,
                indexed_table,
                &mut result,
            )?;
            break;
        }
    }

    if !is_indexed {
        // We've found the requested table, `table_name`.
        let table = &tables_meta.0[&*table_name];
        // Now we need to jump to its pages and read the requested data.
        let page_num = table.rootpage;
        let page = Page::new(db_file, page_size, page_num)?;
        non_indexed_select(
            db_file,
            page_size,
            &page,
            &column_names,
            &where_args,
            table,
            &mut result,
        )?;
    }

    let limit = num_rows.min(result.len() as u64) as usize;
    let result = result[..limit].to_owned();

    Ok(result)
}

/// A `SELECT` business logic in case an index and a `WHERE` clause are used.
///
/// Assumes a table with row IDs, i.e., a table that is not `WITHOUT ROWID`.
fn indexed_select_where<'a>(
    db_file: &mut File,
    page_size: u32,
    index_page: &Page,
    column_names: &str,
    where_arg: &Option<(Cow<'a, str>, Cow<'a, str>)>,
    table: &SchemaTable,
    result: &mut Vec<String>,
) -> Result<()> {
    let where_arg = where_arg.as_ref().expect("Expected a WHERE arg.");
    let where_arg = (&*where_arg.0, &*where_arg.1);
    let (_index_key, index_value) = (where_arg.0, where_arg.1);

    // Search (scan) through the index and only later, in the end, map to the table, to speed up the data retrieval.
    let mut row_ids: Vec<VarintType> = vec![];
    get_indexed_row_ids(db_file, page_size, index_page, index_value, &mut row_ids)?;

    let (desired_columns, num_all_cols, where_column) =
        column_order_where(column_names, table, where_arg)?;

    let (where_col_name, where_col_value) = where_arg;
    let (_where_col_name, where_col_ord) = where_column;
    let where_triple = (where_col_name, where_col_ord, where_col_value);

    let table_page = Page::new(db_file, page_size, table.rootpage)?;

    for row_id in row_ids {
        indexed_select_columns_in_order_rec_where(
            db_file,
            page_size,
            &table_page,
            num_all_cols,
            &desired_columns,
            &where_triple,
            row_id,
            result,
        )?;
    }

    Ok(())
}

/// Recursive implementation of in-order traversal for B-tree, for an index object
///
/// Works with index pages.
///
/// Designed for tables with row IDs, i.e., tables that are not `WITHOUT ROWID`.
///
/// Scans the index in a non-unique way (one-to-many), so this is an index range scan (in Oracle terminology).
///
/// Returns row IDs that match the index value (non-unique).
fn get_indexed_row_ids(
    db_file: &mut File,
    page_size: u32,
    index_page: &Page,
    demanded_index_value: &str,
    row_ids: &mut Vec<VarintType>,
) -> Result<()> {
    // let page_start = (index_page.page_num - 1) * page_size;
    let page_header = index_page.get_header();
    let page_type = page_header.page_type;
    let num_cells = page_header.num_cells as usize;
    let cont = &index_page.contents;
    // eprint!("page_start = 0x{page_start:08x}, page_num: 0x{:08x?}, num_cells: {}, right child: 0x{:08x?}    ", index_page.page_num, num_cells, page_header.rightmost_ptr);

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::IndexLeaf => {
            // eprintln!("L IndexLeaf {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:08x?}, num_cells: {}", index_page.page_num, page_header.num_cells);
            let cell_ptr_array = index_page.get_cell_ptr_array();
            let last_cell = cell_ptr_array[num_cells - 1];
            let mut offset = last_cell;
            let _payload_len = get_varint(index_page, &mut offset)?;
            let _header_len = get_varint(index_page, &mut offset)?;
            let index_ser_type = get_varint(index_page, &mut offset)?;
            let _row_id_ser_type = get_varint(index_page, &mut offset)?;
            let last_index_val =
                get_index_contents_from_serial_type(index_page, &mut offset, index_ser_type)?;
            if demanded_index_value.le(&last_index_val) {
                for cell_ptr in cell_ptr_array {
                    // A cell is: Number of bytes of payload and payload itself. We can ignore overflows in this project, and we indeed do.
                    // Payload is a record (header and body). A record contains a header and a body, in that order.
                    // Body is a key (of any supported type for a column - a serial type) and a number, which is a tuple of an index value and a row id.
                    let mut offset = cell_ptr;
                    let _payload_len = get_varint(index_page, &mut offset)?;
                    // The header begins with a single varint which determines the total number of bytes in the header.
                    // The varint value is the size of the header in bytes including the size varint itself.
                    let _header_len = get_varint(index_page, &mut offset)?;
                    // Following the size varint are one or more additional varints, one per column.
                    // These additional varints are called "serial type" numbers and determine the datatype of each column.
                    // In case of index, we know it's a two-tuple.
                    let index_ser_type = get_varint(index_page, &mut offset)?;
                    let row_id_ser_type = get_varint(index_page, &mut offset)?;
                    let index_val = get_index_contents_from_serial_type(
                        index_page,
                        &mut offset,
                        index_ser_type,
                    )?;

                    if demanded_index_value.eq(&index_val) {
                        let row_id_value = get_index_contents_from_serial_type(
                            index_page,
                            &mut offset,
                            row_id_ser_type,
                        )?;
                        row_ids.push(row_id_value.parse()?);
                    } else if demanded_index_value.lt(&index_val) {
                        // Early-stopping: no need to check further.
                        // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key)
                        // first and the right-most cell (the cell with the largest key) last.
                        break;
                    }
                }
            }
        }
        PageType::IndexInterior => {
            // eprintln!("L IndexInterior {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:08x?}, num_cells: {}", index_page.page_num, page_header.num_cells);

            // Visit the rightmost child first, to save execution time in case we don't have to loop over left children,
            // which is in 50% of cases on average.
            // Perhaps the order isn't intuitive, to first go right and then left, but it saves time.
            let rightmost_page_num = page_header
                .rightmost_ptr
                .expect("Expected PageType::IndexInterior and the rightmost child.");
            // Rightmost pointer is page number of the rightmost child.
            let rightmost_child = Page::new(db_file, page_size, rightmost_page_num)?;
            get_indexed_row_ids(
                db_file,
                page_size,
                &rightmost_child,
                demanded_index_value,
                row_ids,
            )?;

            // Visit left children. We loop only in 50% of cases on average.
            for cell_ptr in index_page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                // Cell contains: (Page number of left child, Number of bytes of payload, Payload, ...) as (u32, varint, byte array, ...).
                // We can ignore overflows in this project, and we indeed do.
                let mut offset = cell_ptr + 4; // The +4 is to skip "Page number of left child".
                let _payload_len = get_varint(index_page, &mut offset)?;
                let _header_len = get_varint(index_page, &mut offset)?;
                let index_ser_type = get_varint(index_page, &mut offset)?;
                let row_id_ser_type = get_varint(index_page, &mut offset)?;
                let index_val =
                    get_index_contents_from_serial_type(index_page, &mut offset, index_ser_type)?;

                if demanded_index_value.gt(&index_val) {
                    // Just go right to the next cell as our key is greater than the current one.
                    continue;
                } else if demanded_index_value.le(&index_val) {
                    let left_page_num =
                        u32::from_be_bytes(cont[cell_ptr as usize..][..4].try_into()?);
                    let left_child = Page::new(db_file, page_size, left_page_num)?;
                    get_indexed_row_ids(
                        db_file,
                        page_size,
                        &left_child,
                        demanded_index_value,
                        row_ids,
                    )?;
                    if demanded_index_value.eq(&index_val) {
                        let row_id_value = get_index_contents_from_serial_type(
                            index_page,
                            &mut offset,
                            row_id_ser_type,
                        )?;
                        row_ids.push(row_id_value.parse()?);
                    }
                    // We cannot stop early because indexes are NOT unique in general case (unlike row IDs).
                    // We need to iterate further.
                }
            }
        }
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }

    Ok(())
}

/// Recursive implementation of indexed in-order traversal for B-tree, for selecting columns from a table
///
/// Assumes a table with row IDs, i.e., a table that is not `WITHOUT ROWID`.
///
/// Works with table pages and with a single row, passed in as `row_id`.
///
/// Scans the table in a unique way, as row IDs are unique.
///
/// Used with an indexed table and a `WHERE` clause.
///
/// Traverses a B-tree using an index, so it doesn't visit each node (page), i.e., it doesn't perform a full-table scan.
///
/// Starts with the provided page, which, in general case, doesn't have to be the root page
/// of the entire database, but it should be a root page of a table.
///
/// Table interior pages don't store data. Their cells store pointers and don't count toward data rows.
#[allow(clippy::too_many_arguments)]
fn indexed_select_columns_in_order_rec_where(
    db_file: &mut File,
    page_size: u32,
    page: &Page,
    num_all_cols: usize,
    desired_columns: &Vec<(String, usize)>,
    where_triple: &(&str, usize, &str),
    demanded_row_id: VarintType,
    result: &mut Vec<String>,
) -> Result<()> {
    // let page_start = (page.page_num - 1) * page_size;
    let page_header = page.get_header();
    let page_type = page_header.page_type;
    let num_cells = page_header.num_cells as usize;
    let cont = &page.contents;
    // eprintln!("page_start = 0x{page_start:08x}, page_num: 0x{:08x?}, num_cells: {}, right child: 0x{:08x?}", page.page_num, num_cells, page_header.rightmost_ptr);

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            // A cell is: Number of bytes of payload, row ID and payload itself. We can ignore overflows in this project, and we indeed do.
            // Payload is a record (header and body). A record contains a header and a body, in that order.
            // Body is a key (of any supported type for a column - a serial type) and a number, which is a tuple of an index value and a row id.
            let cell_ptr_array = page.get_cell_ptr_array();
            let mut first_cell = cell_ptr_array[0];
            let _payload_size = get_varint(page, &mut first_cell)?;
            let lowest_row_id = get_varint(page, &mut first_cell)?;
            let mut last_cell = cell_ptr_array[num_cells - 1];
            let _payload_size = get_varint(page, &mut last_cell)?;
            let highest_row_id = get_varint(page, &mut last_cell)?;
            if demanded_row_id >= lowest_row_id && demanded_row_id <= highest_row_id {
                get_indexed_column_data_for_single_row_on_table_leaf_page_where(
                    page,
                    num_all_cols,
                    desired_columns,
                    where_triple,
                    demanded_row_id,
                    result,
                )?;
            }
        }
        PageType::TableInterior => {
            // Visit the rightmost child first, to save execution time in case we don't have to loop over left children,
            // which is in 50% of cases on average.
            // Perhaps the order isn't intuitive, to first go right and then left, but it saves time.
            let rightmost_page_num = page_header
                .rightmost_ptr
                .expect("Expected PageType::IndexInterior and the rightmost child.");
            // Rightmost pointer is page number of the rightmost child.
            let rightmost_child = Page::new(db_file, page_size, rightmost_page_num)?;
            indexed_select_columns_in_order_rec_where(
                db_file,
                page_size,
                &rightmost_child,
                num_all_cols,
                desired_columns,
                where_triple,
                demanded_row_id,
                result,
            )?;

            // Visit left children. We loop only in 50% of cases on average.
            //
            // Checking the demanded row ID against the lowest and highest row IDs in the cell pointer array first,
            // in order to decide which path to take and potentially skip the following loop, doesn't shorten
            // the execution time. I tried it and I removed it.
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let mut offset = cell_ptr + 4; // The +4 is to skip "Page number of left child".
                let current_row_id = get_varint(page, &mut offset)?;

                if demanded_row_id > current_row_id {
                    // Just go right to the next cell as our key is greater than the current one.
                    continue;
                } else if demanded_row_id <= current_row_id {
                    let left_page_num =
                        u32::from_be_bytes(cont[cell_ptr as usize..][..4].try_into()?);
                    let left_child = Page::new(db_file, page_size, left_page_num)?;
                    indexed_select_columns_in_order_rec_where(
                        db_file,
                        page_size,
                        &left_child,
                        num_all_cols,
                        desired_columns,
                        where_triple,
                        demanded_row_id,
                        result,
                    )?;
                    // We can stop early because row IDs are unique (unlike in case of indexes).
                    // There's no need to iterate any further.
                    return Ok(());
                }
            }
        }
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }

    Ok(())
}

/// A `SELECT` business logic in case an index is not used.
///
/// This performs a full-table scan.
fn non_indexed_select<'a>(
    db_file: &mut File,
    page_size: u32,
    page: &Page,
    column_names: &str,
    where_arg: &Option<(Cow<'a, str>, Cow<'a, str>)>,
    table: &SchemaTable,
    result: &mut Vec<String>,
) -> Result<()> {
    // We choose between two pairs of similar functions because of a performance optimization.
    // Generally, we don't need two functions for either functionality.
    match where_arg {
        None => {
            let (desired_columns, num_all_cols) = column_order(column_names, table)?;

            select_columns_in_order_rec(
                db_file,
                page_size,
                page,
                num_all_cols,
                &desired_columns,
                result,
            )?;
        }
        Some(where_arg) => {
            let where_arg = (&*where_arg.0, &*where_arg.1);

            let (desired_columns, num_all_cols, where_column) =
                column_order_where(column_names, table, where_arg)?;

            let (where_col_name, where_col_value) = where_arg;
            let (_where_col_name, where_col_ord) = where_column;
            let where_triple = (where_col_name, where_col_ord, where_col_value);

            select_columns_in_order_rec_where(
                db_file,
                page_size,
                page,
                num_all_cols,
                &desired_columns,
                &where_triple,
                result,
            )?;
        }
    }

    Ok(())
}

/// Recursive implementation of in-order traversal for B-tree, for selecting columns from a table
///
/// Used without `WHERE` clause.
///
/// Traverses a B-tree, visiting each node (page).
///
/// Starts with the provided page, which, in general case, doesn't have to be the root page
/// of the entire database, but it should be a root page of a table.
///
/// Table interior pages don't store data. Their cells store pointers and don't count toward data rows.
fn select_columns_in_order_rec(
    db_file: &mut File,
    page_size: u32,
    page: &Page,
    num_all_cols: usize,
    desired_columns: &Vec<(String, usize)>,
    result: &mut Vec<String>,
) -> Result<()> {
    let page_header = page.get_header();
    let page_type = page_header.page_type;

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            get_columns_data_for_all_rows_on_table_leaf_page(
                page,
                num_all_cols,
                desired_columns,
                result,
            )?;
        }
        PageType::TableInterior => {
            // Visit left children.
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                let page_num = &page.contents[cell_ptr as usize..][..4];
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(page_num.try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                select_columns_in_order_rec(
                    db_file,
                    page_size,
                    &left_child,
                    num_all_cols,
                    desired_columns,
                    result,
                )?;
            }
            // Visit the rightmost child. For some reason, in this function *only*, we need to visit it last.
            let page_num = page_header.rightmost_ptr.expect("Expected interior table.");
            // Rightmost pointer is page number of the rightmost child.
            let right_child = Page::new(db_file, page_size, page_num)?;
            select_columns_in_order_rec(
                db_file,
                page_size,
                &right_child,
                num_all_cols,
                desired_columns,
                result,
            )?;
        }
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }

    Ok(())
}

/// Recursive implementation of in-order traversal for B-tree, for selecting columns from a table
///
/// Used with `WHERE` clause.
///
/// Traverses a B-tree, visiting each node (page), performing a full-table scan.
///
/// Starts with the provided page, which, in general case, doesn't have to be the root page
/// of the entire database, but it should be a root page of a table.
///
/// Table interior pages don't store data. Their cells store pointers and don't count toward data rows.
fn select_columns_in_order_rec_where(
    db_file: &mut File,
    page_size: u32,
    page: &Page,
    num_all_cols: usize,
    desired_columns: &Vec<(String, usize)>,
    where_triple: &(&str, usize, &str),
    result: &mut Vec<String>,
) -> Result<()> {
    let page_header = page.get_header();
    let page_type = page_header.page_type;

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            get_columns_data_for_all_rows_on_table_leaf_page_where(
                page,
                num_all_cols,
                desired_columns,
                where_triple,
                result,
            )?;
        }
        PageType::TableInterior => {
            // Visit the rightmost child first, for performance reasons.
            let page_num = page_header.rightmost_ptr.expect("Expected interior table.");
            // Rightmost pointer is page number of the rightmost child.
            let right_child = Page::new(db_file, page_size, page_num)?;
            select_columns_in_order_rec_where(
                db_file,
                page_size,
                &right_child,
                num_all_cols,
                desired_columns,
                where_triple,
                result,
            )?;
            // Visit left children.
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                let page_num = &page.contents[cell_ptr as usize..][..4];
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(page_num.try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                select_columns_in_order_rec_where(
                    db_file,
                    page_size,
                    &left_child,
                    num_all_cols,
                    desired_columns,
                    where_triple,
                    result,
                )?;
            }
        }
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }

    Ok(())
}

/// Returns arguments of a `WHERE` clause if it exists, otherwise `None`.
///
/// The arguments are a column name and a column value, and both are returned in their original case.
fn get_where_args(command_rest: &str) -> Result<Option<(&str, &str)>> {
    match command_rest.to_lowercase().find(WHERE_PATTERN) {
        Some(pos) => {
            let rest = &command_rest[pos + WHERE_PATTERN.len()..];
            let (mut column, mut rest) = rest
                .split_once('=')
                .ok_or_else(|| SqlError::WhereIncomplete)?;
            column = column.trim();
            rest = rest.trim().strip_suffix(';').unwrap_or(rest).trim();
            rest = rest
                .strip_prefix('\'')
                .ok_or_else(|| SqlError::WhereIncomplete)?;
            let (value, _rest) = rest
                .split_once('\'')
                .ok_or_else(|| SqlError::WhereIncomplete)?;
            Ok(Some((column, value)))
        }
        None => Ok(None),
    }
}

/// Returns the order of the requested columns by retrieving it from the "sqlite_schema" table's "sql" column.
///
/// Supports working with both tables and indexes, but is generally meant for working with the "table" type,
/// as desired columns are stored in tables, and not in their potential indexes.
///
/// Concretely, returns a tuple of: (a vector of desired columns, the number of all columns in the table).
///
/// A desired column is a tuple itself which comprises a column name and its ordinal.
fn column_order(column_names: &str, table: &SchemaTable) -> Result<(Vec<(String, usize)>, usize)> {
    let desired_column_names = column_names.trim().to_lowercase();
    let desired_column_names = desired_column_names
        .split(',')
        .map(|col| col.trim().trim_matches('"'));
    let desired_column_names = desired_column_names.collect::<Vec<_>>();

    let mut columns_found = Vec::with_capacity(desired_column_names.len());
    let mut column_ordinals = Vec::with_capacity(desired_column_names.len());

    // dbg!(table);
    let table_type = table.tbl_type.to_lowercase();
    let table_type = table_type.as_str();
    let table_name = &table.tbl_name;
    let index_name = &table.name;

    // We are keeping the index type below for completeness, but it doesn't occur here.
    assert_eq!("table", table_type);

    let sql = table.sql.to_lowercase();
    let mut sql = sql.trim();

    match table_type {
        "table" => {
            sql = sql
                .strip_prefix(&format!("create table {table_name}"))
                .unwrap_or(sql)
                .trim();
            sql = sql
                .strip_prefix(&format!("create table \"{table_name}\""))
                .unwrap_or(sql)
                .trim();
            sql = sql.strip_prefix('(').unwrap_or(sql).trim();
            sql = sql.strip_suffix(')').unwrap_or(sql).trim();
        }
        "index" => {
            sql = sql
                .strip_prefix(&format!("create index {index_name}"))
                .unwrap_or(sql)
                .trim();
            sql = sql
                .strip_prefix(&format!("create index \"{index_name}\""))
                .unwrap_or(sql)
                .trim();
            sql = sql.strip_prefix('(').unwrap_or(sql).trim();
            sql = sql
                .strip_prefix(&format!("on {table_name}"))
                .unwrap_or(sql)
                .trim();
            sql = sql.strip_prefix('(').unwrap_or(sql).trim();
            sql = sql.strip_suffix(')').unwrap_or(sql).trim();
            sql = sql.strip_suffix(')').unwrap_or(sql).trim();
        }
        other => panic!("Expected table type 'table' or 'index'; got '{other}'."),
    }

    // The outer loop is over columns that a user wants. This is primarily meant for the "table" type, not for "index".
    for &column_name in &desired_column_names {
        // The middle loop is over all lines that are stored in the "sqlite_schema" table's "sql" column.
        for (mut column_ordinal, mut line) in sql.lines().enumerate() {
            line = line.trim().trim_matches(',').trim();
            let cols = line.split(',');
            // The inner loop is over all columns that are stored on the line (be it one or more columns per line).
            for col in cols {
                let col = col.trim();
                let quote_start = col.find('"');
                let quote_end = col.rfind('"');
                let col_name = match quote_start {
                    Some(start) => match quote_end {
                        Some(end) => &col[start + 1..end],
                        None => panic!("Expected closing quote in '{col}'."),
                    },
                    None => col.split_once(' ').unwrap_or((col, "")).0,
                };
                if col_name.to_lowercase() == column_name {
                    columns_found.push(column_name);
                    column_ordinals.push(column_ordinal);
                }
                column_ordinal += 1;
            }
        }
    }

    let num_all_cols = sql
        .lines()
        .flat_map(|line| line.split(','))
        .filter(|elt| !elt.is_empty())
        .count();

    let requested_cols: HashSet<&&str> = HashSet::from_iter(&desired_column_names);
    let found_cols: HashSet<&&str> = HashSet::from_iter(&columns_found);
    let not_found: HashSet<_> = requested_cols.difference(&found_cols).collect();
    let not_found: Vec<_> = not_found.iter().map(|col| col.to_string()).collect();
    if !not_found.is_empty() {
        Err(SqlError::NoSuchColumns(
            not_found.join(", ").trim_end_matches(", ").to_string(),
        ))?
    }

    assert_eq!(desired_column_names.len(), columns_found.len());
    assert_eq!(desired_column_names.len(), column_ordinals.len());

    let desired_columns: Vec<(String, usize)> = zip(
        desired_column_names.iter().map(|col| col.to_string()),
        column_ordinals,
    )
    .collect();

    Ok((desired_columns, num_all_cols))
}

/// Returns the order of the requested columns by retrieving it from the "sqlite_schema" table's "sql" column.
///
/// Concretely, returns a tuple of: (a vector of desired columns, the number of all columns in the table,
/// and a `WHERE` column).
///
/// A desired column and the WHERE column are tuples themselves which comprise a column name and its ordinal.
fn column_order_where(
    column_names: &str,
    table: &SchemaTable,
    where_arg: (&str, &str),
) -> Result<(Vec<ColumnNameOrd>, usize, ColumnNameOrd)> {
    let desired_column_names = column_names.trim().to_lowercase();
    let desired_column_names = desired_column_names
        .split(',')
        .map(|col| col.trim().trim_matches('"'));
    let desired_column_names = desired_column_names.collect::<Vec<_>>();

    let where_col_name = where_arg.0.trim().to_lowercase();
    let mut where_col_ord = usize::MAX;

    let mut columns_found = Vec::with_capacity(desired_column_names.len());
    let mut column_ordinals = Vec::with_capacity(desired_column_names.len());

    // dbg!(table);
    let table_type = table.tbl_type.to_lowercase();
    let table_type = table_type.as_str();
    let table_name = &table.tbl_name;

    assert_eq!("table", table_type);

    let sql = table.sql.to_lowercase();
    let mut sql = sql.trim();

    sql = sql
        .strip_prefix(&format!("create table {table_name}"))
        .unwrap_or(sql)
        .trim();
    sql = sql
        .strip_prefix(&format!("create table \"{table_name}\""))
        .unwrap_or(sql)
        .trim();
    sql = sql.strip_prefix('(').unwrap_or(sql).trim();
    sql = sql.strip_suffix(')').unwrap_or(sql).trim();

    // The outer loop is over columns that a user wants. This is primarily meant for the "table" type, not for "index".
    for &column_name in &desired_column_names {
        // The middle loop is over all lines that are stored in the "sqlite_schema" table's "sql" column.
        for (mut column_ordinal, mut line) in sql.lines().enumerate() {
            line = line.trim().trim_matches(',').trim();
            let cols = line.split(',');
            // The inner loop is over all columns that are stored on the line (be it one or more columns per line).
            for col in cols {
                let col = col.trim();
                let quote_start = col.find('"');
                let quote_end = col.rfind('"');
                let col_name = match quote_start {
                    Some(start) => match quote_end {
                        Some(end) => &col[start + 1..end],
                        None => panic!("Expected closing quote in '{col}'."),
                    },
                    None => col.split_once(' ').unwrap_or((col, "")).0,
                };
                if col_name.to_lowercase() == column_name {
                    columns_found.push(column_name);
                    column_ordinals.push(column_ordinal);
                }
                column_ordinal += 1;
            }
        }
    }

    // Also check for the WHERE column.
    for (mut column_ordinal, mut line) in sql.lines().enumerate() {
        line = line.trim().trim_matches(',').trim();
        let cols = line.split(',');
        for col in cols {
            let col = col.trim();
            let quote_start = col.find('"');
            let quote_end = col.rfind('"');
            let col_name = match quote_start {
                Some(start) => match quote_end {
                    Some(end) => &col[start..=end],
                    None => panic!("Expected closing quote in '{col}'."),
                },
                None => col.split_once(' ').unwrap_or((col, "")).0,
            };
            if col_name.to_lowercase() == where_col_name {
                where_col_ord = column_ordinal;
                break;
            }
            column_ordinal += 1;
        }
    }

    let num_all_cols = sql
        .lines()
        .flat_map(|line| line.split(','))
        .filter(|elt| !elt.is_empty())
        .count();

    let requested_cols: HashSet<&&str> = HashSet::from_iter(&desired_column_names);
    let found_cols: HashSet<&&str> = HashSet::from_iter(&columns_found);
    let not_found: HashSet<_> = requested_cols.difference(&found_cols).collect();
    let not_found: Vec<_> = not_found.iter().map(|col| col.to_string()).collect();
    if !not_found.is_empty() {
        Err(SqlError::NoSuchColumns(
            not_found.join(", ").trim_end_matches(", ").to_string(),
        ))?
    }

    assert_eq!(desired_column_names.len(), columns_found.len());
    assert_eq!(desired_column_names.len(), column_ordinals.len());

    let desired_columns: Vec<(String, usize)> = zip(
        desired_column_names.iter().map(|col| col.to_string()),
        column_ordinals,
    )
    .collect();

    let where_column = (where_col_name, where_col_ord);

    Ok((desired_columns, num_all_cols, where_column))
}

/// Returns all column names in the table.
fn get_all_columns_names(table: &SchemaTable) -> Vec<String> {
    let mut all_cols = Vec::new();

    // dbg!(table);
    let table_type = table.tbl_type.to_lowercase();
    let table_type = table_type.as_str();
    let table_name = &table.tbl_name;

    assert_eq!("table", table_type);

    let sql = table.sql.to_lowercase();
    let mut sql = sql.trim();

    sql = sql
        .strip_prefix(&format!("create table {table_name}"))
        .unwrap_or(sql)
        .trim();
    sql = sql
        .strip_prefix(&format!("create table \"{table_name}\""))
        .unwrap_or(sql)
        .trim();
    sql = sql.strip_prefix('(').unwrap_or(sql).trim();
    sql = sql.strip_suffix(')').unwrap_or(sql).trim();

    // The outer loop is over all lines that are stored in the "sqlite_schema" table's "sql" column.
    for mut line in sql.lines() {
        line = line.trim().trim_matches(',').trim();
        let cols = line.split(',');
        // The inner loop is over all columns that are stored on the line (be it one or more columns per line).
        for col in cols {
            let col = col.trim();
            let quote_start = col.find('"');
            let quote_end = col.rfind('"');
            let col_name = match quote_start {
                Some(start) => match quote_end {
                    Some(end) => &col[start + 1..end],
                    None => panic!("Expected closing quote in '{col}'."),
                },
                None => col.split_once(' ').unwrap_or((col, "")).0,
            };
            all_cols.push(col_name.to_string());
        }
    }
    all_cols
}

/// Gets data from desired columns for all rows on the table leaf page.
///
/// Takes the in-out `result` parameter which it adds to, so added items can be used
/// through that parameter after the function returns.
///
/// It is optimized, so it doesn't support the `WHERE` clause.
fn get_columns_data_for_all_rows_on_table_leaf_page(
    page: &Page,
    num_all_cols: usize,
    desired_columns: &Vec<(String, usize)>,
    result: &mut Vec<String>,
) -> Result<()> {
    // "The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number
    // of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and
    // the right-most cell (the cell with the largest key) last." - from the official documentation.
    let cell_ptr_array = page.get_cell_ptr_array();

    // Loop over all rows (cells) on the page. A row is a cell, so num_rows == page_header.num_cells.
    for i in 0..page.get_header().num_cells {
        let cell_ptr = cell_ptr_array[i as usize];

        let mut row_result = String::new();

        // Loop over the desired columns.
        for (column_name, column_ordinal) in desired_columns {
            let mut offset = cell_ptr;

            // B-tree Cell Format
            let _payload_size = get_varint(page, &mut offset)?;
            let row_id = get_varint(page, &mut offset)?;

            if *column_name == "id" {
                row_result += &(row_id.to_string() + "|");
                continue;
            }

            // Now comes payload, as a byte array, or actual rows (records).
            // Payload, either table b-tree data or index b-tree keys, is always in the "record format".
            // The record format defines a sequence of values corresponding to columns in a table or index.
            // The record format specifies the number of columns, the datatype of each column, and the content of each column.
            // The record format makes extensive use of the variable-length integer or varint representation of 64-bit signed integers.
            // A record contains a header and a body, in that order.
            // The header begins with a single varint which determines the total number of bytes in the header.
            // The varint value is the size of the header in bytes including the size varint itself.
            let header_start = offset; // Needed for an optimization below.
            let header_size = get_varint(page, &mut offset)?;
            // Following the size varint are one or more additional varints, one per column.
            // These additional varints are called "serial type" numbers and determine the datatype of each column.
            // The header size varint and serial type varints will usually consist of a single byte, but they could be longer, so let's determine the size in bytes.
            let read = (offset - header_start) as u64;
            let _column_data_size = header_size as u64 - read; // Not used.

            let mut column_serial_type = 0;
            // Our column's offset after record header. Used for an optimization only.
            let mut column_offset = 0;
            // We are looking for our column only and early-stopping when we find it.
            // We don't want to read and extract sizes of all columns.
            // That wouldn't be efficient, especially in case there's a lot of columns in the table.
            for col_ind in 0..num_all_cols {
                let col_serial_type = get_varint(page, &mut offset)?;
                column_serial_type = col_serial_type;
                let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
                if col_ind == *column_ordinal {
                    break;
                }
                column_offset += column_content_size;
            }
            let mut offset = header_start + header_size as u16 + column_offset as u16;
            // eprintln!("offset = 0x{offset:04x}");
            let column_contents = get_column_contents_from_serial_type_column_name(
                page,
                &mut offset,
                column_serial_type,
                column_name,
            )?;
            row_result += &(column_contents + "|");
        }

        result.push(row_result.trim_end_matches('|').to_string());
    }

    Ok(())
}

/// Gets data from desired columns for all rows on the table leaf page, taking an optional `WHERE` clause into account.
///
/// Takes the in-out `result` parameter which it adds to, so added items can be used
/// through that parameter after the function returns.
///
/// `WHERE` triple is a 3-tuple of a column name, the column ordinal and a desired column value.
fn get_columns_data_for_all_rows_on_table_leaf_page_where(
    page: &Page,
    num_all_cols: usize,
    desired_columns: &Vec<(String, usize)>,
    where_triple: &(&str, usize, &str),
    result: &mut Vec<String>,
) -> Result<()> {
    let (where_col_name, where_col_ord, where_col_value) = *where_triple;

    // "The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number
    // of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and
    // the right-most cell (the cell with the largest key) last." - from the official documentation.
    let cell_ptr_array = page.get_cell_ptr_array();
    assert_eq!(cell_ptr_array.len(), page.get_header().num_cells.into());

    // Loop over all rows (cells) on the page. A row is a cell, so num_rows == page_header.num_cells.
    for i in 0..page.get_header().num_cells {
        let cell_ptr = cell_ptr_array[i as usize];

        let mut row_result = String::new();

        // Loop over the desired columns.
        for (column_name, column_ordinal) in desired_columns {
            let mut offset = cell_ptr;

            // B-tree Cell Format
            let _payload_size = get_varint(page, &mut offset)?;
            let row_id = get_varint(page, &mut offset)?;

            if *column_name == "id" {
                row_result += &(row_id.to_string() + "|");
                continue;
            }

            // Now comes payload, as a byte array, or actual rows (records).
            let header_start = offset; // Needed for an optimization below.
            let header_size = get_varint(page, &mut offset)?;

            // Following the size varint are one or more additional varints, one per column.
            let mut column_serial_type = 0;
            // Our column's offset after record header. Used for an optimization only.
            let mut column_offset = 0;
            // We are looking for our column only and early-stopping when we find it.
            // We don't want to read and extract sizes of all columns.
            // That wouldn't be efficient, especially in case there's a lot of columns in the table.
            for col_ind in 0..num_all_cols {
                let col_serial_type = get_varint(page, &mut offset)?;
                column_serial_type = col_serial_type;
                let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
                if col_ind == *column_ordinal {
                    break;
                }
                column_offset += column_content_size;
            }
            let mut offset = header_start + header_size as u16 + column_offset as u16;
            // eprintln!("offset = 0x{offset:04x}");
            let column_contents = get_column_contents_from_serial_type_column_name(
                page,
                &mut offset,
                column_serial_type,
                column_name,
            )?;
            row_result += &(column_contents + "|");
        }

        // Also check for the WHERE column.
        let mut offset = cell_ptr;
        let _payload_size = get_varint(page, &mut offset)?;
        let row_id = get_varint(page, &mut offset)?;
        if where_col_name == "id" {
            if where_col_value == row_id.to_string() {
                result.push(row_result.trim_end_matches('|').to_string());
                // Row ID is unique so we can safely break.
                break;
            }
            continue;
        }
        let header_start = offset;
        let header_size = get_varint(page, &mut offset)?;
        let mut column_serial_type = 0;
        let mut column_offset = 0;
        let mut col_ord = usize::MAX;
        for col_ind in 0..num_all_cols {
            let col_serial_type = get_varint(page, &mut offset)?;
            column_serial_type = col_serial_type;
            let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
            if col_ind == where_col_ord {
                col_ord = col_ind;
                break;
            }
            column_offset += column_content_size;
        }
        let mut offset = header_start + header_size as u16 + column_offset as u16;
        // eprintln!("offset = 0x{offset:04x}");
        let column_contents = get_column_contents_from_serial_type_column_name(
            page,
            &mut offset,
            column_serial_type,
            where_col_name,
        )?;
        if (where_col_ord == col_ord) && (where_col_value == column_contents) {
            result.push(row_result.trim_end_matches('|').to_string());
        }
    }

    Ok(())
}

/// Gets data from desired columns for a single row on the table leaf page,
/// taking a provided `WHERE` clause into account.
///
/// Assumes a table with row IDs, i.e., a table that is not `WITHOUT ROWID`.
///
/// Takes a leaf page and a row ID.
///
/// Takes the in-out `result` parameter which it adds to, so added items can be used
/// through that parameter after the function returns.
///
/// `WHERE` triple is a 3-tuple of a column name, the column ordinal and a desired column value.
fn get_indexed_column_data_for_single_row_on_table_leaf_page_where(
    page: &Page,
    num_all_cols: usize,
    desired_columns: &Vec<(String, usize)>,
    where_triple: &(&str, usize, &str),
    row_id: VarintType,
    result: &mut Vec<String>,
) -> Result<()> {
    let (where_col_name, where_col_ord, where_col_value) = *where_triple;

    // "The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number
    // of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and
    // the right-most cell (the cell with the largest key) last." - from the official documentation.
    let cell_ptr_array = page.get_cell_ptr_array();
    assert_eq!(cell_ptr_array.len(), page.get_header().num_cells.into());

    let i = cell_ptr_array
        .binary_search_by_key(&row_id, |&cell_ptr| {
            let mut offset = cell_ptr;
            // B-tree Cell Format
            let _payload_size = get_varint(page, &mut offset).unwrap();
            // Row ID
            get_varint(page, &mut offset).expect("Expected to convert cell varint into row ID")
        })
        .unwrap_or_else(|_| panic!("Expected to contain row ID {row_id}"));

    let cell_ptr = cell_ptr_array[i];

    let mut row_result = String::new();

    // Loop over the desired columns.
    for (column_name, column_ordinal) in desired_columns {
        let mut offset = cell_ptr;

        // B-tree Cell Format
        let _payload_size = get_varint(page, &mut offset)?;
        let row_id = get_varint(page, &mut offset)?;

        if *column_name == "id" {
            row_result += &(row_id.to_string() + "|");
            continue;
        }

        // Now comes payload, as a byte array, or actual rows (records).
        let header_start = offset; // Needed for an optimization below.
        let header_size = get_varint(page, &mut offset)?;

        // Following the size varint are one or more additional varints, one per column.
        let mut column_serial_type = 0;
        // Our column's offset after record header. Used for an optimization only.
        let mut column_offset = 0;
        // We are looking for our column only and early-stopping when we find it.
        // We don't want to read and extract sizes of all columns.
        // That wouldn't be efficient, especially in case there's a lot of columns in the table.
        for col_ind in 0..num_all_cols {
            let col_serial_type = get_varint(page, &mut offset)?;
            column_serial_type = col_serial_type;
            let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
            if col_ind == *column_ordinal {
                break;
            }
            column_offset += column_content_size;
        }
        let mut offset = header_start + header_size as u16 + column_offset as u16;
        // eprintln!("offset = 0x{offset:04x}");
        let column_contents = get_column_contents_from_serial_type_column_name(
            page,
            &mut offset,
            column_serial_type,
            column_name,
        )?;
        row_result += &(column_contents + "|");
    }

    // Also check for the WHERE column.
    let mut offset = cell_ptr;
    let _payload_size = get_varint(page, &mut offset)?;
    let row_id = get_varint(page, &mut offset)?;
    if where_col_name == "id" {
        if where_col_value == row_id.to_string() {
            result.push(row_result.trim_end_matches('|').to_string());
            // Row ID is unique so we can safely break (return).
            return Ok(());
        }
        return Ok(());
    }
    let header_start = offset;
    let header_size = get_varint(page, &mut offset)?;
    let mut column_serial_type = 0;
    let mut column_offset = 0;
    let mut col_ord = usize::MAX;
    for col_ind in 0..num_all_cols {
        let col_serial_type = get_varint(page, &mut offset)?;
        column_serial_type = col_serial_type;
        let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
        if col_ind == where_col_ord {
            col_ord = col_ind;
            break;
        }
        column_offset += column_content_size;
    }
    let mut offset = header_start + header_size as u16 + column_offset as u16;
    // eprintln!("offset = 0x{offset:04x}");
    let column_contents = get_column_contents_from_serial_type_column_name(
        page,
        &mut offset,
        column_serial_type,
        where_col_name,
    )?;
    if (where_col_ord == col_ord) && (where_col_value == column_contents) {
        result.push(row_result.trim_end_matches('|').to_string());
    }

    Ok(())
}

/// Returns index contents for a given serial type.
fn get_index_contents_from_serial_type(
    page: &Page,
    offset: &mut u16,
    serial_type: VarintType,
) -> Result<String> {
    let old_offset = *offset;
    let contents = match get_serial_type_to_content(page, offset, serial_type)? {
        (SerialTypeValue::Null(_), _read) => "".to_string(),
        (SerialTypeValue::Zero, _read) => "".to_string(),
        (SerialTypeValue::One, _read) => "".to_string(),
        (SerialTypeValue::Text(column_contents), _read) => column_contents,
        (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
        (SerialTypeValue::Int16(column_contents), _read) => column_contents.to_string(),
        (SerialTypeValue::Int24(column_contents), _read) => column_contents.to_string(),
        stv => panic!(
            "Got unexpected type: {stv:?}; read {} byte(s).",
            *offset - old_offset
        ),
    };

    Ok(contents)
}

/// Returns column contents for a given serial type with a given desired column name.
fn get_column_contents_from_serial_type_column_name(
    page: &Page,
    offset: &mut u16,
    column_serial_type: VarintType,
    column_name: &str,
) -> Result<String> {
    let old_offset = *offset;
    let column_contents = match get_serial_type_to_content(page, offset, column_serial_type)? {
        (SerialTypeValue::Null(_), _read) => "".to_string(),
        (SerialTypeValue::Text(column_contents), _read) => column_contents,
        (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
        stv => panic!(
            "Got unexpected type for column '{column_name}': {stv:?}; read {} byte(s).",
            *offset - old_offset
        ),
    };

    Ok(column_contents)
}

#[cfg(test)]
mod tests {
    use crate::sql::select;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////            The Sample Database           ///////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn select_count_rows_apples() {
        let command = "   SeLEcT    cOUnt(*)   FroM   apPLes   ;   ";
        let expected = 4.to_string();
        let result = &select("sample.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[test]
    fn select_count_rows_sqlite_sequence() {
        let command = "   SeLEcT    cOUnt(*)   FroM   SQLite_Sequence   ;   ";
        let expected = 2.to_string();
        let result = &select("sample.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[test]
    fn select_count_rows_oranges() {
        let command = "   SeLEcT    cOUnt(*)   FroM   OrangeS    ";
        let expected = 6.to_string();
        let result = &select("sample.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[test]
    fn select_name_from_apples1() {
        let command = "SeleCT nAme frOM APPles";
        let mut expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples2() {
        let command = "   SeleCT    nAme   frOM   APPles   ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples3() {
        let command = "   SeleCT    nAme   frOM   APPles  ;   ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples4() {
        let command = "   SeleCT    \"nAme\"   frOM   APPles  ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples5() {
        let command = "   SeleCT    nAme   frOM   \"APPles\"  ;  ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples6() {
        let command = "   SeleCT    \"nAme\"   frOM   \"APPles\"   ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples_limit_003() {
        let command = "   SeleCT    \"nAme\"   frOM   \"APPles\"   LIMit   003  ;  ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples_limit_010() {
        let command = "   SeleCT    \"nAme\"   frOM   \"APPles\"   LIMit   010   ";
        let expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
            "Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[should_panic(expected = "No such table: 'apple'")]
    #[test]
    fn select_name_from_apple() {
        let command = "select name from apple";
        let expected = vec!["a"];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[should_panic(expected = "No such columns: 'names'")]
    #[test]
    fn select_names_from_apples() {
        let command = "select names from apples";
        let expected = vec!["a"];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_color_from_apples() {
        let command = "   SeleCT    cOLor   frOM   APPles   ";
        let expected = vec![
            "Light Green".to_string(),
            "Red".to_string(),
            "Blush Red".to_string(),
            "Yellow".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_sqlite_sequence() {
        let command = "   SeleCT    nAme   frOM   SQLite_Sequence  ;  ";
        let expected = vec!["apples".to_string(), "oranges".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_seq_from_sqlite_sequence() {
        let command = "   SeleCT    seQ   frOM   SQLite_Sequence  ;  ";
        let expected = vec!["4".to_string(), "6".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_oranges() {
        let command = "select name from oranges";
        let expected = vec![
            "Mandarin".to_string(),
            "Tangelo".to_string(),
            "Tangerine".to_string(),
            "Clementine".to_string(),
            "Valencia Orange".to_string(),
            "Navel Orange".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_description_from_oranges() {
        let command = "select description from oranges";
        let expected = vec![
            "great for snacking".to_string(),
            "sweet and tart".to_string(),
            "great for sweeter juice".to_string(),
            "usually seedless, great for snacking".to_string(),
            "best for juicing".to_string(),
            "sweet with slight bitterness".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_from_apples1() {
        let command = "   SeleCT    Id   frOM   APPles  ;   ";
        let expected = (1..=4).map(|id| id.to_string()).collect::<Vec<String>>();
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_from_apples2() {
        let command = "   SeleCT    Id   frOM   APPles   ";
        let expected = (1..=4).map(|id| id.to_string()).collect::<Vec<String>>();
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_color_from_apples() {
        let command = "   SeleCT    nAme, ColoR   frOM   APPles  ;   ";
        let expected = vec![
            "Granny Smith|Light Green".to_string(),
            "Fuji|Red".to_string(),
            "Honeycrisp|Blush Red".to_string(),
            "Golden Delicious|Yellow".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples() {
        let command = "   SeleCT   Id  ,  nAme  ,  ColoR   frOM   APPles  ;   ";
        let expected = vec![
            "1|Granny Smith|Light Green".to_string(),
            "2|Fuji|Red".to_string(),
            "3|Honeycrisp|Blush Red".to_string(),
            "4|Golden Delicious|Yellow".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_color_name_from_apples() {
        let command = "   SeleCT    nAme, ColoR,NaME   frOM   APPles  ";
        let expected = vec![
            "Granny Smith|Light Green|Granny Smith".to_string(),
            "Fuji|Red|Fuji".to_string(),
            "Honeycrisp|Blush Red|Honeycrisp".to_string(),
            "Golden Delicious|Yellow|Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_name_from_apples() {
        let command = "   SeleCT  iD  ,  nAme, ColoR,NaME   frOM   APPles     ";
        let expected = vec![
            "1|Granny Smith|Light Green|Granny Smith".to_string(),
            "2|Fuji|Red|Fuji".to_string(),
            "3|Honeycrisp|Blush Red|Honeycrisp".to_string(),
            "4|Golden Delicious|Yellow|Golden Delicious".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_id_id_from_apples() {
        let command = "   SeleCT  iD  ,  nAme, ColoR,Id,iD   frOM   APPles     ";
        let expected = vec![
            "1|Granny Smith|Light Green|1|1".to_string(),
            "2|Fuji|Red|2|2".to_string(),
            "3|Honeycrisp|Blush Red|3|3".to_string(),
            "4|Golden Delicious|Yellow|4|4".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[should_panic(expected = "No such columns: ")]
    #[test]
    fn select_aaa_id_name_bbb_color_name_ccc_from_apples() {
        let command = "  SeleCT  Aaa , iD , nAme, bBb , ColoR,NaME , ccC  frOM   APPles  ";
        let expected = vec!["a"];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_seq_seq_from_sqlite_sequence() {
        let command = "   SeleCT    nAme,sEq   ,   SeQ   frOM   SQLite_Sequence  ;  ";
        let expected = vec!["apples|4|4".to_string(), "oranges|6|6".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_name_name_seq_seq_seq_name_name_name_seq_seq_seq_from_sqlite_sequence() {
        let command =
            "select name,name,name,seq,seq,seq,name,name,name,seq,seq,seq from sqlite_sequence";
        let expected = vec![
            "apples|apples|apples|4|4|4|apples|apples|apples|4|4|4".to_string(),
            "oranges|oranges|oranges|6|6|6|oranges|oranges|oranges|6|6|6".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_description_from_oranges() {
        let command = "   SeleCT    nAme  ,  descRiption   frOM   orangeS   ";
        let expected = vec![
            "Mandarin|great for snacking".to_string(),
            "Tangelo|sweet and tart".to_string(),
            "Tangerine|great for sweeter juice".to_string(),
            "Clementine|usually seedless, great for snacking".to_string(),
            "Valencia Orange|best for juicing".to_string(),
            "Navel Orange|sweet with slight bitterness".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_name_name_from_oranges() {
        let command = "   SeleCT    nAme  ,  namE  ,  Name  frOM   orangeS  ;  ";
        let expected = vec![
            "Mandarin|Mandarin|Mandarin".to_string(),
            "Tangelo|Tangelo|Tangelo".to_string(),
            "Tangerine|Tangerine|Tangerine".to_string(),
            "Clementine|Clementine|Clementine".to_string(),
            "Valencia Orange|Valencia Orange|Valencia Orange".to_string(),
            "Navel Orange|Navel Orange|Navel Orange".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_description_from_oranges_limit_0() {
        let command = "   SeleCT   Id  ,  nAme  ,  descRiption   frOM   orangeS  liMit   0  ;  ";
        let expected: Vec<String> = vec![];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_description_from_oranges_limit_3() {
        let command = "   SeleCT   Id  ,  nAme  ,  descRiption   frOM   orangeS  liMit   3  ;  ";
        let expected = vec![
            "1|Mandarin|great for snacking".to_string(),
            "2|Tangelo|sweet and tart".to_string(),
            "3|Tangerine|great for sweeter juice".to_string(),
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples_where_color_yellow_lowercase() {
        let command = "SELECT name FROM apples WHERE color = 'yellow'";
        let expected: Vec<String> = vec![];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples_where_color_yellow() {
        let command = "SELECT name FROM apples WHERE color = 'Yellow'";
        let expected = vec!["Golden Delicious".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_color_from_apples_where_color_yellow() {
        let command = "SELECT color FROM apples WHERE color = 'Yellow'";
        let expected = vec!["Yellow".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_color_from_apples_where_name_granny_smith() {
        let command = "SELECT color FROM apples WHERE name = 'Granny Smith'";
        let expected = vec!["Light Green".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_color_name_from_apples_where_name_granny_smith() {
        let command = "SELECT color, name FROM apples WHERE name = 'Granny Smith'";
        let expected = vec!["Light Green|Granny Smith".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples_where_color_blush_red() {
        let command = "SELECT id, name, color FROM apples WHERE color = 'Blush Red'";
        let expected = vec!["3|Honeycrisp|Blush Red".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_from_apples_where_color_blush_red() {
        let command = "SELECT id FROM apples WHERE color = 'Blush Red'";
        let expected = vec!["3".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_id_color_color_name_name_id_id_from_apples_where_color_blush_red() {
        let command =
            "SELECT id,id,color,color,name,name,id,id FROM apples WHERE color = 'Blush Red'";
        let expected = vec!["3|3|Blush Red|Blush Red|Honeycrisp|Honeycrisp|3|3".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples_where_id_3() {
        let command = "SELECT id, name, color FROM apples WHERE id = '3'";
        let expected = vec!["3|Honeycrisp|Blush Red".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_color_from_apples_where_id_3() {
        let command = "SELECT name, color FROM apples WHERE id = '3'";
        let expected = vec!["Honeycrisp|Blush Red".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_id_color_color_name_name_id_id_from_apples_where_id_3() {
        let command = "SELECT id,id,color,color,name,name,id,id FROM apples WHERE id = '3'";
        let expected = vec!["3|3|Blush Red|Blush Red|Honeycrisp|Honeycrisp|3|3".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples_where_color_light_green_limit_1_a() {
        let command = "SELECT id, name, color FROM apples WHERE color = 'Light Green' LIMIT 1";
        let expected = vec!["1|Granny Smith|Light Green".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples_where_color_light_green_limit_1_b() {
        let command = "SELECT id, name, color FROM apples WHERE color='Light Green' LIMIT 1";
        let expected = vec!["1|Granny Smith|Light Green".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples_where_color_light_green_limit_1_c() {
        let command =
            " SELECT  Id , Name , Color  FROM  Apples  WHERE  Color  =  'Light Green'  LIMIT  1 ; ";
        let expected = vec!["1|Granny Smith|Light Green".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_asterisk_asterisk_id_asterisk_color_from_apples() {
        let command = "SELECT id, *, *, id, *, color FROM apples";
        let expected = vec![
            "1|1|Granny Smith|Light Green|1|Granny Smith|Light Green|1|1|Granny Smith|Light Green|Light Green".to_string(),
            "2|2|Fuji|Red|2|Fuji|Red|2|2|Fuji|Red|Red".to_string(),
            "3|3|Honeycrisp|Blush Red|3|Honeycrisp|Blush Red|3|3|Honeycrisp|Blush Red|Blush Red".to_string(),
            "4|4|Golden Delicious|Yellow|4|Golden Delicious|Yellow|4|4|Golden Delicious|Yellow|Yellow".to_string()
        ];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_asterisk_asterisk_id_asterisk_color_from_apples_where_color_red() {
        let command = "SELECT id, *, *, id, *, color FROM apples WHERE color = 'Red'";
        let expected = vec!["2|2|Fuji|Red|2|Fuji|Red|2|2|Fuji|Red|Red".to_string()];
        let result = select("sample.db", command).unwrap();
        assert_eq!(expected, result);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////         The Superheroes Database         ///////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn select_count_rows_superheroes() {
        let command = "SELECT COUNT(*) FROM superheroes";
        let expected = 6895.to_string();
        let result = &select("test_dbs/superheroes.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[test]
    fn select_id_from_superheroes() {
        let command = "SELECT id FROM superheroes";
        let expected: Vec<String> = (1u16..=6895).map(|n| n.to_string()).collect::<Vec<_>>();
        let result = select("test_dbs/superheroes.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_superheroes_where_eye_color_pink_eyes() {
        let command = "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes'";
        let expected = vec![
            "297|Stealth (New Earth)".to_string(),
            "790|Tobias Whale (New Earth)".to_string(),
            "1085|Felicity (New Earth)".to_string(),
            "2729|Thrust (New Earth)".to_string(),
            "3289|Angora Lapin (New Earth)".to_string(),
            "3913|Matris Ater Clementia (New Earth)".to_string(),
        ];
        let result = select("test_dbs/superheroes.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_superheroes_where_eye_color_pink_eyes_limit_3() {
        let command = "SELECT id, name FROM superheroes WHERE eye_color = 'Pink Eyes' LIMIT 3";
        let expected = vec![
            "297|Stealth (New Earth)".to_string(),
            "790|Tobias Whale (New Earth)".to_string(),
            "1085|Felicity (New Earth)".to_string(),
        ];
        let result = select("test_dbs/superheroes.db", command).unwrap();
        assert_eq!(expected, result);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////          The Companies Database          ///////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn select_count_rows_companies() {
        let command = "SELECT COUNT(*) FROM companies";
        let expected = 55991.to_string();
        let result = &select("test_dbs/companies.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[ignore = "WHERE not supported with COUNT(*)"]
    #[test]
    fn select_count_rows_companies_where_country_myanmar() {
        let command = "SELECT COUNT(*) FROM companies WHERE country = 'myanmar'";
        let expected = 799.to_string();
        let result = &select("test_dbs/companies.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[test]
    fn select_id_from_companies_limit_10() {
        let command = "SELECT id FROM companies LIMIT 10";
        let expected: Vec<String> = [159, 168, 193, 238, 654, 673, 782, 931, 966, 1247]
            .map(|n| n.to_string())
            .to_vec();
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_companies_limit_5() {
        let command = "SELECT id, name FROM companies LIMIT 5";
        let expected = vec![
            "159|global computer services llc".to_string(),
            "168|noble foods co".to_string(),
            "193|ipf softwares".to_string(),
            "238|adamjee group".to_string(),
            "654|art gallery line".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_count_through_id_name_from_companies_where_country_dominican_republic() {
        let command = "SELECT id, name FROM companies WHERE country = 'dominican republic'";
        let expected = 1881;
        let result = select("test_dbs/companies.db", command).unwrap().len();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_companies_where_country_eritrea() {
        let command = "SELECT id, name FROM companies WHERE country = 'eritrea'";
        let expected = vec![
            "121311|unilink s.c.".to_string(),
            "2102438|orange asmara it solutions".to_string(),
            "5729848|zara mining share company".to_string(),
            "6634629|asmara rental".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_companies_where_country_eritrea_limit_3() {
        let command = "SELECT id, name FROM companies WHERE country = 'eritrea' LIMIT 3";
        let expected = vec![
            "121311|unilink s.c.".to_string(),
            "2102438|orange asmara it solutions".to_string(),
            "5729848|zara mining share company".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_count_through_id_name_from_companies_where_country_estonia() {
        let command = "SELECT id, name FROM companies WHERE country = 'estonia'";
        let expected = 0;
        let result = select("test_dbs/companies.db", command).unwrap().len();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_companies_where_country_micronesia() {
        let command = "SELECT id, name FROM companies WHERE country = 'micronesia'";
        let expected = vec![
            "1307865|college of micronesia".to_string(),
            "3696903|nanofabrica".to_string(),
            "4023193|fsm statistics".to_string(),
            "6132291|vital energy micronesia".to_string(),
            "6387751|fsm development bank".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_count_through_id_name_from_companies_where_country_myanmar() {
        let command = "SELECT id, name FROM companies WHERE country = 'myanmar'";
        let expected = 799;
        let result = select("test_dbs/companies.db", command).unwrap().len();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_count_through_id_name_from_companies_where_country_spain() {
        let command = "SELECT id, name FROM companies WHERE country = 'spain'";
        let expected = 0;
        let result = select("test_dbs/companies.db", command).unwrap().len();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_companies_where_country_north_korea() {
        let command = "SELECT id, name FROM companies WHERE country = 'north korea'";
        let expected = vec![
            "986681|isn network company limited".to_string(),
            "1573653|initial innovation limited".to_string(),
            "2828420|beacon point ltd".to_string(),
            "3485462|pyongyang university of science & technology (pust)".to_string(),
            "3969653|plastoform industries ltd".to_string(),
            "4271599|korea national insurance corporation".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_count_through_id_name_from_companies_where_country_tanzania() {
        let command = "SELECT id, name FROM companies WHERE country = 'tanzania'";
        let expected = 1053;
        let result = select("test_dbs/companies.db", command).unwrap().len();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_from_companies_where_country_tonga() {
        let command = "SELECT id, name FROM companies WHERE country = 'tonga'";
        let expected = vec![
            "361142|tonga communications corporation".to_string(),
            "3186430|tonga development bank".to_string(),
            "3583436|leiola group limited".to_string(),
            "4796634|royco amalgamated company limited".to_string(),
            "7084593|tonga business enterprise centre".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_all_columns_from_companies_where_country_eritrea() {
        let command = "SELECT * FROM companies WHERE country = 'eritrea'";
        let expected = vec![
            "121311|unilink s.c.|unilinksc.com|2014.0|computer software|1 - 10||eritrea|0|2".to_string(),
            "2102438|orange asmara it solutions|orangeasmara.com|2008.0|information technology and services|1 - 10|asmara, maekel, eritrea|eritrea|0|4".to_string(),
            "5729848|zara mining share company|zaramining.com|2013.0|mining & metals|51 - 200||eritrea|45|60".to_string(),
            "6634629|asmara rental|asmararental.com|2010.0|real estate|1 - 10||eritrea|1|1".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_size_range_from_companies_limit_5() {
        let command = "SELECT id, \"size range\" FROM companies LIMIT 5";
        let expected = vec![
            "159|51 - 200".to_string(),
            "168|1 - 10".to_string(),
            "193|11 - 50".to_string(),
            "238|1 - 10".to_string(),
            "654|1 - 10".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_size_range_from_companies_where_country_eritrea_limit_3() {
        let command = "SELECT id, \"size range\" FROM companies WHERE country = 'eritrea' LIMIT 3";
        let expected = vec![
            "121311|1 - 10".to_string(),
            "2102438|1 - 10".to_string(),
            "5729848|51 - 200".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[ignore = "indexed table but not by the WHERE column"]
    #[test]
    fn select_id_size_range_from_companies_where_size_range_limit_5() {
        let command =
            "SELECT id, \"size range\" FROM companies WHERE \"size range\" = '11 - 50' LIMIT 5";
        let expected = vec![
            "193|11 - 50".to_string(),
            "782|11 - 50".to_string(),
            "1282|11 - 50".to_string(),
            "2442|11 - 50".to_string(),
            "2466|11 - 50".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_asterisk_from_companies_limit_5() {
        let command = "SELECT * FROM companies LIMIT 5";
        let expected = vec![
            "159|global computer services llc|globcom-oman.com|1998.0|computer software|51 - 200|burnsville, minnesota, united states|oman|24|35".to_string(),
            "168|noble foods co|||logistics and supply chain|1 - 10||guernsey|1|1".to_string(),
            "193|ipf softwares|ipfsoftwares.com|2013.0|information technology and services|11 - 50|dar es salaam, dar es salaam, tanzania|tanzania|5|7".to_string(),
            "238|adamjee group|adamjee.mu|1979.0|real estate|1 - 10|grand baie, riviere du rempart, mauritius|mauritius|1|1".to_string(),
            "654|art gallery line|artgalleryline.com|2003.0|fine art|1 - 10|tbilisi, dushet'is raioni, georgia|georgia|2|2".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }

    #[ignore = "indexed table but not by the WHERE column"]
    #[test]
    fn select_asterisk_from_companies_where_size_range_limit_5() {
        let command = "SELECT * FROM companies WHERE \"size range\" = '11 - 50' LIMIT 5";
        let expected = vec![
            "193|ipf softwares|ipfsoftwares.com|2013.0|information technology and services|11 - 50|dar es salaam, dar es salaam, tanzania|tanzania|5|7".to_string(),
            "782|ameco international travel|amecotravel.com|1977.0|airlines/aviation|11 - 50||fiji|5|6".to_string(),
            "1282|formesan dominicana srl|formesan.com.co||civil engineering|11 - 50|santo domingo oeste, santo domingo, dominican republic|dominican republic|9|17".to_string(),
            "2442|f5 llc|f5innovative.az|2014.0|information technology and services|11 - 50|baku, baki, azerbaijan|azerbaijan|6|14".to_string(),
            "2466|ascot barclay cyber security group|ascotbarclay.com|2004.0|security and investigations|11 - 50|london, greater london, united kingdom|guernsey|4|12".to_string(),
        ];
        let result = select("test_dbs/companies.db", command).unwrap();
        assert_eq!(expected, result);
    }
}
