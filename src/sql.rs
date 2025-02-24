//! SQL Command Handlers
//!
//! [SQL As Understood By SQLite](https://www.sqlite.org/lang.html)

use crate::constants::{ColumnNameOrd, COUNT_PATTERN, FROM_PATTERN, SELECT_PATTERN, WHERE_PATTERN};
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
type CliArgsRest = (
    Cow<'static, str>,
    Option<(Cow<'static, str>, Cow<'static, str>)>,
    Cow<'static, str>,
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
    assert!(page_type.eq(&PageType::TableLeaf) || page_type.eq(&PageType::TableInterior));

    // The format of a cell (B-tree Cell Format) depends on which kind of b-tree page the cell appears on (the current page).
    match page_type {
        PageType::TableLeaf => {
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            return Ok(page_header.num_cells as u64);
        }
        PageType::TableInterior => {
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(cont[cell_ptr as usize..][..4].try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                num_rows += count_rows_in_order_rec(db_file, page_size, left_child)?;
            }
            // Visit the rightmost child.
            // eprintln!("R {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            let page_num = page_header
                .rightmost_ptr
                .expect("Expected PageType::TableInterior.");
            // Rightmost pointer is page number of the rightmost child.
            let right_child = Page::new(db_file, page_size, page_num)?;
            num_rows += count_rows_in_order_rec(db_file, page_size, right_child)?;
        }
        PageType::IndexLeaf => {
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            return Ok(page_header.num_cells as u64);
        }
        PageType::IndexInterior => {
            // eprintln!("L {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                // Cell contains: (Page number of left child, Number of bytes of payload, Payload, ...) as (u32, varint, byte array, ...).
                let page_num = u32::from_be_bytes(cont[cell_ptr as usize..][..4].try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                num_rows += count_rows_in_order_rec(db_file, page_size, left_child)? + 1;
            }
            // Visit the rightmost child.
            // eprintln!("R {page_type:?} => page_start = 0x{page_start:08x}, page_num: 0x{:04x?}, num_cells: {}", page.page_num, page_header.num_cells);
            let page_num = page_header
                .rightmost_ptr
                .expect("Expected PageType::IndexInterior.");
            // Rightmost pointer is page number of the rightmost child.
            let right_child = Page::new(db_file, page_size, page_num)?;
            num_rows += count_rows_in_order_rec(db_file, page_size, right_child)?;
        }
    }

    Ok(num_rows)
}

/// Returns columns names, `WHERE` arguments (column name and value), table name and number of rows.
///
/// `SELECT <column_1>, ..., <column_n> FROM <table> WHERE <where_clause> LIMIT <limit>`
///
/// Column names can be provided in arbitrary order and can be repeated.
fn parse_cli_args_rest(db_file_path: &str, mut rest: &str) -> Result<CliArgsRest> {
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

/// `SELECT <column_1>, ..., <column_n> FROM <table> WHERE <where_clause> LIMIT <limit>`
///
/// Column names can be provided in arbitrary order and can be repeated.
///
/// Returns error if the table, or at least one of the columns, doesn't exist.
fn select_columns_from_table(
    db_file_path: &str,
    cli_args_rest: CliArgsRest,
) -> Result<Vec<String>> {
    let (column_names, where_args, table_name, num_rows) = cli_args_rest;

    let tables_meta = get_tables_meta(db_file_path)?;
    // eprintln!("tables_meta: {tables_meta:#?}");

    // TODO: See if there's an index in the DB. This makes more sense with the `WHERE` clause, but see if it makes some sense here, too.

    // TODO: Search through the index and only in the end map to the table, to speed up the search.

    if !tables_meta.0.contains_key(&*table_name) {
        // In case a table with the given name, `table_name`, does not exist in the database, return that error.
        Err(SqlError::NoSuchTable(table_name.to_string()))?
    }

    let mut result = Vec::with_capacity(num_rows as usize);

    let page_size = dot_cmd::page_size(db_file_path)?;
    let db_file = &mut File::open(db_file_path)?;

    // We've found the requested table, `table_name`.
    let table = &tables_meta.0[&*table_name];
    // Now we need to jump to its pages and read the requested data.
    let page_num = table.rootpage;

    let page = Page::new(db_file, page_size, page_num)?;

    // We choose between two pairs of similar functions because of a performance optimization.
    // Generally, we don't need two functions for either functionality.
    match where_args {
        None => {
            let (desired_columns, num_all_cols) = column_order(&column_names, table)?;

            select_columns_in_order_rec(
                db_file,
                page_size,
                page,
                num_all_cols,
                &desired_columns,
                &mut result,
            )?;
        }
        Some(where_args) => {
            let where_args = (&*where_args.0, &*where_args.1);

            let (desired_columns, num_all_cols, where_column) =
                column_order_where(&column_names, table, where_args)?;

            let (where_col_name, where_col_value) = where_args;
            let (_where_col_name, where_col_ord) = where_column;
            let where_triple = (where_col_name, where_col_ord, where_col_value);

            select_columns_in_order_rec_where(
                db_file,
                page_size,
                page,
                num_all_cols,
                &desired_columns,
                &where_triple,
                &mut result,
            )?;
        }
    }

    let limit = num_rows.min(result.len() as u64) as usize;
    let result = result[..limit].to_owned();

    Ok(result)
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
    page: Page,
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
                &page,
                num_all_cols,
                desired_columns,
                result,
            )?;
        }
        PageType::TableInterior => {
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                let page_num = &page.contents[cell_ptr as usize..][..4];
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(page_num.try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                select_columns_in_order_rec(
                    db_file,
                    page_size,
                    left_child,
                    num_all_cols,
                    desired_columns,
                    result,
                )?;
            }
            // Visit the rightmost child.
            if page_type == PageType::TableInterior {
                let page_num = page_header.rightmost_ptr.expect("Expected interior table.");
                // Rightmost pointer is page number of the rightmost child.
                let right_child = Page::new(db_file, page_size, page_num)?;
                select_columns_in_order_rec(
                    db_file,
                    page_size,
                    right_child,
                    num_all_cols,
                    desired_columns,
                    result,
                )?;
            }
        }
        // TODO: Add index types?! They should map to table types.
        other => panic!("Page type {other:?} encountered where it shouldn't be!"),
    }

    Ok(())
}

/// Recursive implementation of in-order traversal for B-tree, for selecting columns from a table
///
/// Used with `WHERE` clause.
///
/// Traverses a B-tree, visiting each node (page).
///
/// Starts with the provided page, which, in general case, doesn't have to be the root page
/// of the entire database, but it should be a root page of a table.
///
/// Table interior pages don't store data. Their cells store pointers and don't count toward data rows.
fn select_columns_in_order_rec_where(
    db_file: &mut File,
    page_size: u32,
    page: Page,
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
                &page,
                num_all_cols,
                desired_columns,
                where_triple,
                result,
            )?;
        }
        PageType::TableInterior => {
            for cell_ptr in page.get_cell_ptr_array() {
                // Let's jump to the cell. The cell pointer offsets are relative to the start of the page.
                let page_num = &page.contents[cell_ptr as usize..][..4];
                // Cell contains: (Page number of left child, Rowid) as (u32, varint).
                let page_num = u32::from_be_bytes(page_num.try_into()?);
                let left_child = Page::new(db_file, page_size, page_num)?;
                select_columns_in_order_rec_where(
                    db_file,
                    page_size,
                    left_child,
                    num_all_cols,
                    desired_columns,
                    where_triple,
                    result,
                )?;
            }
            // Visit the rightmost child.
            if page_type == PageType::TableInterior {
                let page_num = page_header.rightmost_ptr.expect("Expected interior table.");
                // Rightmost pointer is page number of the rightmost child.
                let right_child = Page::new(db_file, page_size, page_num)?;
                select_columns_in_order_rec_where(
                    db_file,
                    page_size,
                    right_child,
                    num_all_cols,
                    desired_columns,
                    where_triple,
                    result,
                )?;
            }
        }
        // TODO: Add index types?! They should map to table types.
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

    // eprintln!("table: {table:#?}");
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
    where_args: (&str, &str),
) -> Result<(Vec<ColumnNameOrd>, usize, ColumnNameOrd)> {
    let desired_column_names = column_names.trim().to_lowercase();
    let desired_column_names = desired_column_names
        .split(',')
        .map(|col| col.trim().trim_matches('"'));
    let desired_column_names = desired_column_names.collect::<Vec<_>>();

    let where_col_name = where_args.0.trim().to_lowercase();
    let mut where_col_ord = usize::MAX;

    let mut columns_found = Vec::with_capacity(desired_column_names.len());
    let mut column_ordinals = Vec::with_capacity(desired_column_names.len());

    // eprintln!("table: {table:#?}");
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

    eprintln!("table: {table:#?}");
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
            let old_offset = offset;
            let column_contents =
                match get_serial_type_to_content(page, &mut offset, column_serial_type)? {
                    (SerialTypeValue::Null(_), _read) => "".to_string(),
                    (SerialTypeValue::Text(column_contents), _read) => column_contents,
                    (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
                    stv => panic!(
                        "Got unexpected type for column '{column_name}': {stv:?}; read {} byte(s).",
                        offset - old_offset
                    ),
                };
            row_result += &(column_contents + "|");
        }

        result.push(row_result.trim_end_matches('|').to_string());
    }

    Ok(())
}

/// Gets data from desired columns for all rows on the page, taking an optional `WHERE` clause into account.
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

    // Loop over all rows (cells) on the page. A row is a cell, so num_rows == page_header.num_cells,
    // but a user can LIMIT the number of rows, so we have to account for that.
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
            let old_offset = offset;
            let column_contents =
                match get_serial_type_to_content(page, &mut offset, column_serial_type)? {
                    (SerialTypeValue::Null(_), _read) => "".to_string(),
                    (SerialTypeValue::Text(column_contents), _read) => column_contents,
                    (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
                    stv => panic!(
                        "Got unexpected type for column '{column_name}': {stv:?}; read {} byte(s).",
                        offset - old_offset
                    ),
                };
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
        let old_offset = offset;
        let column_contents =
            match get_serial_type_to_content(page, &mut offset, column_serial_type)? {
                (SerialTypeValue::Null(_), _read) => "".to_string(),
                (SerialTypeValue::Text(column_contents), _read) => column_contents,
                (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
                stv => panic!(
                    "Got unexpected type for column '{where_col_name}': {stv:?}; read {} byte(s).",
                    offset - old_offset
                ),
            };
        if (where_col_ord == col_ord) && (where_col_value == column_contents) {
            result.push(row_result.trim_end_matches('|').to_string());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::sql::select;

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

    #[test]
    fn select_count_rows_companies() {
        let command = "SELECT COUNT(*) FROM companies";
        let expected = 55991.to_string();
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
