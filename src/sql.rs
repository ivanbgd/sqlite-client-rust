//! SQL Command Handlers
//!
//! [SQL As Understood By SQLite](https://www.sqlite.org/lang.html)

use crate::dot_cmd;
use crate::errors::SqlError;
use crate::serial_type::{
    read_serial_type_to_content, serial_type_to_content_size, SerialTypeValue,
};
use crate::tables::{get_tables_meta, SchemaTable};
use crate::varint::read_varint;
use anyhow::Result;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::iter::zip;

/// SQL `SELECT` command
///
/// `SELECT` itself, its subcommands, column and table names are case-insensitive.
///
/// Column and table names can be wrapped in double-quotes.
///
/// Column names can be provided in arbitrary order and can be repeated.
///
/// Semicolon at the end of the statement is supported and optional.
///
/// `LIMIT` is optional.
///
/// Supports:
/// - `SELECT COUNT(*) FROM <table>`
/// - `SELECT <column> FROM <table> LIMIT <limit>`
/// - `SELECT <column1>, <column2>, ... <columnN> FROM <table> LIMIT <limit>;`
///
/// [SELECT](https://www.sqlite.org/lang_select.html)
pub fn select(db_file_path: &str, command: &str) -> Result<Vec<String>> {
    let command = command.trim().to_lowercase();
    assert!(command.starts_with("select"));
    let (_select, mut rest) = command
        .split_once(' ')
        .ok_or_else(|| SqlError::SelectIncomplete)?;
    rest = rest.trim().strip_suffix(';').unwrap_or(rest).trim();

    if rest.contains("from") {
        if rest.starts_with("count(*)") {
            // `SELECT COUNT(*) FROM <table>` - Count rows in a table.
            rest = rest.strip_prefix("count(*)").expect("count(*)").trim();
            if rest.starts_with("from") {
                rest = rest.strip_prefix("from").expect("from").trim();
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
            // `SELECT <column1>, ..., <column_n> FROM <table> LIMIT <limit>`
            let (columns, mut rest) = rest
                .split_once("from")
                .ok_or_else(|| SqlError::SelectIncomplete)?;
            rest = rest.trim();
            if rest.is_empty() {
                Err(SqlError::SelectIncomplete)?
            }
            return select_columns_from_table(db_file_path, columns, rest);
        }
    }

    Err(SqlError::SelectUnsupported(command.to_string()))?
}

/// `SELECT COUNT(*) FROM <table>`
///
/// A leaf page for the table is assumed.
///
/// Returns error if the table does not exist.
fn select_count_rows_in_table(db_file_path: &str, table_name: &str) -> Result<u16> {
    let table_name = &table_name.to_lowercase();
    let tables_meta = get_tables_meta(db_file_path)?;

    if tables_meta.0.contains_key(table_name) {
        // We've found the requested table, `table_name`.
        let table = &tables_meta.0[table_name];
        // Now we need to jump to its page and read the requested data.
        let rootpage = table.rootpage;
        // Pages are numbered from one, i.e., they are 1-indexed, so subtract one to get to the page.
        let page_size = dot_cmd::page_size(db_file_path)?;
        let page_offset = (rootpage - 1) * page_size as u64;
        let mut db_file = File::open(db_file_path)?;

        // The two-byte integer at offset 3 gives the number of cells on the page.
        // This is part of the page header, and it represents the number of rows on this page.
        let offset = page_offset + 3;
        let mut buf = [0u8; 2];
        let _pos = db_file.seek(SeekFrom::Start(offset))?;
        db_file.read_exact(&mut buf)?;
        // All multibyte values in the page header are big-endian.
        let num_rows = u16::from_be_bytes(buf);

        Ok(num_rows)
    } else {
        // In case a table with the given name, `table_name`, does not exist in the database, return that error.
        Err(SqlError::NoSuchTable(table_name.to_string()))?
    }
}

/// `SELECT <column_1>, ..., <column_n> FROM <table> LIMIT <limit>`
///
/// Column names can be provided in arbitrary order and can be repeated.
///
/// A leaf page for the table is assumed.
///
/// Returns error if the table, or at least one of the columns, doesn't exist.
fn select_columns_from_table(
    db_file_path: &str,
    column_names: &str,
    rest: &str,
) -> Result<Vec<String>> {
    let rest = &rest.trim().trim_matches('"').to_lowercase();
    let (mut table_name, limit) = rest.split_once("limit").unwrap_or((rest, ""));
    table_name = table_name.trim().trim_matches('"');
    let num_rows = if !limit.is_empty() {
        let limit = limit
            .trim()
            .parse::<u64>()
            .map_err(|_| SqlError::LimitParsingError(limit.trim().to_string()))?;
        limit.min(select_count_rows_in_table(db_file_path, table_name)? as u64)
    } else {
        select_count_rows_in_table(db_file_path, table_name)? as u64
    };

    let tables_meta = get_tables_meta(db_file_path)?;

    if !tables_meta.0.contains_key(table_name) {
        // In case a table with the given name, `table_name`, does not exist in the database, return that error.
        Err(SqlError::NoSuchTable(table_name.to_string()))?
    }

    // We've found the requested table, `table_name`.
    let table = &tables_meta.0[table_name];
    // Now we need to jump to its page and read the requested data.
    let rootpage = table.rootpage;
    // Pages are numbered from one, i.e., they are 1-indexed, so subtract one to get to the page.
    let page_size = dot_cmd::page_size(db_file_path)?;
    let page_offset = (rootpage - 1) * page_size as u64;
    let db_file = &mut File::open(db_file_path)?;

    let (desired_columns, num_all_cols) = column_order(column_names, table_name, table)?;

    let mut result = Vec::with_capacity(num_rows as usize);

    // Since this is a leaf page, its header is eight-bytes long, so we have to skip that many bytes
    // from the start of the page to get to the cell pointer array which represents offsets to the cell contents,
    // i.e., offsets to rows.
    let mut offset = page_offset + 8;

    // "The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number
    // of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and
    // the right-most cell (the cell with the largest key) last." - from the official documentation.
    // Loop over rows.
    for _ in 0..num_rows {
        let mut buf = [0u8; 2];
        let _pos = db_file.seek(SeekFrom::Start(offset))?;
        db_file.read_exact(&mut buf)?;
        // All multibyte values in the page header are big-endian.
        let row_offset = u16::from_be_bytes(buf);
        get_columns_data_for_row(
            db_file,
            num_all_cols,
            page_offset,
            row_offset,
            &desired_columns,
            &mut result,
        )?;
        offset += 2;
    }

    Ok(result)
}

/// Returns the order of the requested columns by retrieving it from the "sqlite_schema" table's "sql" column.
fn column_order(
    column_names: &str,
    table_name: &str,
    table: &SchemaTable,
) -> Result<(Vec<(String, usize)>, usize)> {
    let column_names = column_names.trim().to_lowercase();
    let column_names = column_names
        .split(',')
        .map(|col| col.trim().trim_matches('"'));
    let column_names = column_names.collect::<Vec<_>>();

    let num_all_cols;
    let mut column_ordinals = Vec::with_capacity(column_names.len());
    let mut columns_found = Vec::with_capacity(column_names.len());
    let mut sql = table.sql.as_str();

    sql = match sql.strip_suffix("\n)") {
        Some(sql) => sql,
        None => sql.strip_suffix(')').expect("strip suffix ')'"),
    };

    match sql.strip_prefix(format!("create table {}\n(\n", table_name).as_str()) {
        Some(sql) => {
            // The outer loop is over columns that a user wants.
            for &column_name in &column_names {
                // The inner loop is over all lines that are stored in the "sqlite_schema" table's "sql" column.
                for (column_ordinal, line) in sql.lines().enumerate() {
                    if line.trim().to_lowercase().starts_with(column_name) {
                        columns_found.push(column_name);
                        column_ordinals.push(column_ordinal);
                        break;
                    }
                }
            }
            num_all_cols = sql.lines().collect::<Vec<_>>().len();
        }
        None => {
            sql = match sql.strip_prefix(format!("create table {}(", table_name).as_str()) {
                Some(sql) => sql,
                None => match sql.strip_prefix(format!("create table {} (", table_name).as_str()) {
                    Some(sql) => sql,
                    None => {
                        match sql
                            .strip_prefix(format!("create table \"{}\" (", table_name).as_str())
                        {
                            Some(sql) => sql,
                            None => sql
                                .strip_prefix(format!("create table \"{}\"(", table_name).as_str())
                                .unwrap_or_else(|| panic!("strip prefix: sql = '{sql}'")),
                        }
                    }
                },
            };
            let columns = sql
                .split(',')
                .map(|col| col.trim().trim_matches('"'))
                .collect::<Vec<_>>();
            num_all_cols = columns.len();

            // The outer loop is over columns that a user wants.
            for column_name in &column_names {
                // The inner loop is over all columns that exist in the table,
                // whose names were fetched above from the "sqlite_schema" table's "sql" column.
                for (column_ordinal, col) in columns.iter().enumerate() {
                    if col.trim().starts_with(column_name) {
                        columns_found.push(*column_name);
                        column_ordinals.push(column_ordinal);
                        break;
                    }
                }
            }
        }
    };

    let requested_cols: HashSet<&&str> = HashSet::from_iter(&column_names);
    let found_cols: HashSet<&&str> = HashSet::from_iter(&columns_found);
    let not_found: HashSet<_> = requested_cols.difference(&found_cols).collect();
    let not_found: Vec<_> = not_found.iter().map(|col| col.to_string()).collect();
    if !not_found.is_empty() {
        Err(SqlError::NoSuchColumns(
            not_found.join(", ").trim_end_matches(", ").to_string(),
        ))?
    }

    assert_eq!(column_names.len(), columns_found.len());
    assert_eq!(column_names.len(), column_ordinals.len());

    let desired_columns: Vec<(String, usize)> = zip(
        column_names.iter().map(|col| col.to_string()),
        column_ordinals,
    )
    .collect();

    Ok((desired_columns, num_all_cols))
}

/// Gets data from desired columns for a single row.
fn get_columns_data_for_row(
    db_file: &mut File,
    num_all_cols: usize,
    page_offset: u64,
    row_offset: u16,
    desired_columns: &Vec<(String, usize)>,
    result: &mut Vec<String>,
) -> Result<()> {
    let mut row_result = String::new();

    // Loop over columns.
    for (column_name, column_ordinal) in desired_columns {
        let offset = page_offset + row_offset as u64;
        let _pos = db_file.seek(SeekFrom::Start(offset))?;

        // B-tree Cell Format
        let (_payload_size, _read) = read_varint(db_file)?;
        let (row_id, _read) = read_varint(db_file)?;

        if *column_name == "id" {
            // row_result.push(row_id + "|");
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
        let header_start = db_file.stream_position()?; // Needed for an optimization below.
        let (header_size, read) = read_varint(db_file)?;
        // Following the size varint are one or more additional varints, one per column.
        // These additional varints are called "serial type" numbers and determine the datatype of each column.
        // The header size varint and serial type varints will usually consist of a single byte, but they could be longer, so let's determine the size in bytes.
        let _column_data_size = header_size as u64 - read; // Not used.

        let mut column_serial_type = 0;
        // Our column's offset after record header. Used for an optimization only.
        let mut column_offset = 0;
        // We are looking for our column only and early-stopping when we find it.
        // We don't want to read and extract sizes of all columns.
        // That wouldn't be efficient, especially in case there's a lot of columns in the table.
        for col_ind in 0..num_all_cols {
            let (col_serial_type, _read) = read_varint(db_file)?;
            column_serial_type = col_serial_type;
            let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
            if col_ind == *column_ordinal {
                break;
            }
            column_offset += column_content_size;
        }
        let offset = header_start + header_size as u64 + column_offset;
        let _pos = db_file.seek(SeekFrom::Start(offset))?;
        // eprintln!("_pos = {_pos:04x}, offset = {offset:04x}");
        let column_contents = match read_serial_type_to_content(db_file, column_serial_type)? {
            (SerialTypeValue::Text(column_contents), _read) => column_contents,
            (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
            (stv, read) => panic!(
                "Got unexpected type for column '{column_name}': {stv:?}; read {read} byte(s)."
            ),
        };
        row_result += &(column_contents + "|");
    }
    result.push(row_result.trim_end_matches('|').to_string());

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
    fn select_name_from_apples3() {
        let command = "   SeleCT    nAme   frOM   APPles  ;   ";
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
    fn select_name_from_apples4() {
        let command = "   SeleCT    \"nAme\"   frOM   APPles  ";
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
    fn select_name_from_apples5() {
        let command = "   SeleCT    nAme   frOM   \"APPles\"  ;  ";
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
    fn select_name_from_apples6() {
        let command = "   SeleCT    \"nAme\"   frOM   \"APPles\"   ";
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
    fn select_name_from_apples_limit_003() {
        let command = "   SeleCT    \"nAme\"   frOM   \"APPles\"   LIMit   003  ;  ";
        let mut expected = vec![
            "Granny Smith".to_string(),
            "Fuji".to_string(),
            "Honeycrisp".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_apples_limit_010() {
        let command = "   SeleCT    \"nAme\"   frOM   \"APPles\"   LIMit   010   ";
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
        let mut expected = vec![
            "Light Green".to_string(),
            "Red".to_string(),
            "Blush Red".to_string(),
            "Yellow".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_sqlite_sequence() {
        let command = "   SeleCT    nAme   frOM   SQLite_Sequence  ;  ";
        let mut expected = vec!["apples".to_string(), "oranges".to_string()];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_seq_from_sqlite_sequence() {
        let command = "   SeleCT    seQ   frOM   SQLite_Sequence  ;  ";
        let mut expected = vec!["4".to_string(), "6".to_string()];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_from_oranges() {
        let command = "select name from oranges";
        let mut expected = vec![
            "Mandarin".to_string(),
            "Tangelo".to_string(),
            "Tangerine".to_string(),
            "Clementine".to_string(),
            "Valencia Orange".to_string(),
            "Navel Orange".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_description_from_oranges() {
        let command = "select description from oranges";
        let mut expected = vec![
            "great for snacking".to_string(),
            "sweet and tart".to_string(),
            "great for sweeter juice".to_string(),
            "usually seedless, great for snacking".to_string(),
            "best for juicing".to_string(),
            "sweet with slight bitterness".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_from_apples1() {
        let command = "   SeleCT    Id   frOM   APPles  ;   ";
        let mut expected = (1..=4).map(|id| id.to_string()).collect::<Vec<String>>();
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_from_apples2() {
        let command = "   SeleCT    Id   frOM   APPles   ";
        let mut expected = (1..=4).map(|id| id.to_string()).collect::<Vec<String>>();
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_color_from_apples() {
        let command = "   SeleCT    nAme, ColoR   frOM   APPles  ;   ";
        let mut expected = vec![
            "Granny Smith|Light Green".to_string(),
            "Fuji|Red".to_string(),
            "Honeycrisp|Blush Red".to_string(),
            "Golden Delicious|Yellow".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_from_apples() {
        let command = "   SeleCT   Id  ,  nAme  ,  ColoR   frOM   APPles  ;   ";
        let mut expected = vec![
            "1|Granny Smith|Light Green".to_string(),
            "2|Fuji|Red".to_string(),
            "3|Honeycrisp|Blush Red".to_string(),
            "4|Golden Delicious|Yellow".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_color_name_from_apples() {
        let command = "   SeleCT    nAme, ColoR,NaME   frOM   APPles  ";
        let mut expected = vec![
            "Granny Smith|Light Green|Granny Smith".to_string(),
            "Fuji|Red|Fuji".to_string(),
            "Honeycrisp|Blush Red|Honeycrisp".to_string(),
            "Golden Delicious|Yellow|Golden Delicious".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_name_from_apples() {
        let command = "   SeleCT  iD  ,  nAme, ColoR,NaME   frOM   APPles     ";
        let mut expected = vec![
            "1|Granny Smith|Light Green|Granny Smith".to_string(),
            "2|Fuji|Red|Fuji".to_string(),
            "3|Honeycrisp|Blush Red|Honeycrisp".to_string(),
            "4|Golden Delicious|Yellow|Golden Delicious".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_color_id_id_from_apples() {
        let command = "   SeleCT  iD  ,  nAme, ColoR,Id,iD   frOM   APPles     ";
        let mut expected = vec![
            "1|Granny Smith|Light Green|1|1".to_string(),
            "2|Fuji|Red|2|2".to_string(),
            "3|Honeycrisp|Blush Red|3|3".to_string(),
            "4|Golden Delicious|Yellow|4|4".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
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
        let mut expected = vec!["apples|4|4".to_string(), "oranges|6|6".to_string()];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_name_name_seq_seq_seq_name_name_name_seq_seq_seq_from_sqlite_sequence() {
        let command =
            "select name,name,name,seq,seq,seq,name,name,name,seq,seq,seq from sqlite_sequence";
        let mut expected = vec![
            "apples|apples|apples|4|4|4|apples|apples|apples|4|4|4".to_string(),
            "oranges|oranges|oranges|6|6|6|oranges|oranges|oranges|6|6|6".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_description_from_oranges() {
        let command = "   SeleCT    nAme  ,  descRiption   frOM   orangeS   ";
        let mut expected = vec![
            "Mandarin|great for snacking".to_string(),
            "Tangelo|sweet and tart".to_string(),
            "Tangerine|great for sweeter juice".to_string(),
            "Clementine|usually seedless, great for snacking".to_string(),
            "Valencia Orange|best for juicing".to_string(),
            "Navel Orange|sweet with slight bitterness".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_name_name_name_from_oranges() {
        let command = "   SeleCT    nAme  ,  namE  ,  Name  frOM   orangeS  ;  ";
        let mut expected = vec![
            "Mandarin|Mandarin|Mandarin".to_string(),
            "Tangelo|Tangelo|Tangelo".to_string(),
            "Tangerine|Tangerine|Tangerine".to_string(),
            "Clementine|Clementine|Clementine".to_string(),
            "Valencia Orange|Valencia Orange|Valencia Orange".to_string(),
            "Navel Orange|Navel Orange|Navel Orange".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }

    #[test]
    fn select_id_name_description_from_oranges_limit_3() {
        let command = "   SeleCT   Id  ,  nAme  ,  descRiption   frOM   orangeS  liMit   3  ;  ";
        let mut expected = vec![
            "1|Mandarin|great for snacking".to_string(),
            "2|Tangelo|sweet and tart".to_string(),
            "3|Tangerine|great for sweeter juice".to_string(),
        ];
        expected.sort();
        let mut result = select("sample.db", command).unwrap();
        result.sort();
        assert_eq!(expected, result);
    }
}
