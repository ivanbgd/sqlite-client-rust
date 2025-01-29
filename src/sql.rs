//! SQL Command Handlers
//!
//! [SQL As Understood By SQLite](https://www.sqlite.org/lang.html)

use crate::dot_cmd;
use crate::errors::SqlError;
use crate::serial_type::{
    read_serial_type_to_content, serial_type_to_content_size, SerialTypeValue,
};
use crate::tables::get_tables_meta;
use crate::varint::read_varint;
use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// SQL `SELECT` command
///
/// `SELECT` itself, its subcommands, column and table names are case-insensitive.
///
/// Supports:
/// - `SELECT COUNT(*) FROM <table>`
/// - `SELECT <column> FROM <table>`
///
/// [SELECT](https://www.sqlite.org/lang_select.html)
pub fn select(db_file_path: &str, command: &str) -> Result<Vec<String>> {
    let command = command.trim().to_lowercase();
    assert!(command.starts_with("select"));
    let (_select, mut rest) = command
        .split_once(' ')
        .ok_or_else(|| SqlError::SelectIncomplete)?;
    rest = rest.trim().strip_suffix(';').unwrap_or(rest);

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
            // `SELECT <column> FROM <table>`
            let (column, mut rest) = rest
                .split_once(' ')
                .ok_or_else(|| SqlError::SelectIncomplete)?;
            rest = rest.trim();
            if rest.starts_with("from") {
                rest = rest.strip_prefix("from").expect("from").trim();
                if rest.is_empty() {
                    Err(SqlError::SelectIncomplete)?
                }
                return select_column_from_table(db_file_path, column, rest);
            } else {
                Err(SqlError::SelectIncomplete)?
            }
        }
    }

    Err(SqlError::SelectUnsupported(command.to_string()))?
}

/// `SELECT COUNT(*) FROM <table>`
///
/// A leaf page is assumed.
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

/// `SELECT <column> FROM <table>`
///
/// A leaf page is assumed.
///
/// Returns error if the table does not exist.
fn select_column_from_table(
    db_file_path: &str,
    column_name: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let column_name = &column_name.to_lowercase();
    let table_name = &table_name.to_lowercase();
    let num_rows = select_count_rows_in_table(db_file_path, table_name)? as u64;
    let tables_meta = get_tables_meta(db_file_path)?;

    if tables_meta.0.contains_key(table_name) {
        // We've found the requested table, `table_name`.
        let table = &tables_meta.0[table_name];
        // Now we need to jump to its page and read the requested data.
        let rootpage = table.rootpage;
        // Pages are numbered from one, i.e., they are 1-indexed, so subtract one to get to the page.
        let page_size = dot_cmd::page_size(db_file_path)?;
        let page_offset = (rootpage - 1) * page_size as u64;
        let db_file = &mut File::open(db_file_path)?;

        // The order of the column can be retrieved from the "sqlite_schema" table's "sql" column.
        // That's where we could obtain the column type from, too.
        let mut column_found = false;
        let mut column_ordinal = 0;
        let num_cols;
        let mut sql = table.sql.as_str();
        if table_name.eq("sqlite_sequence") {
            sql = sql.strip_suffix(')').expect(")");
            sql = sql
                .strip_prefix("create table sqlite_sequence(")
                .expect("create table sqlite_sequence");
            let columns = sql.split(',').collect::<Vec<_>>();
            for col in &columns {
                if col == column_name {
                    column_found = true;
                    break;
                }
                column_ordinal += 1;
            }
            num_cols = columns.len();
        } else {
            sql = match sql.strip_suffix("\n)") {
                Some(sql) => sql,
                None => sql.strip_suffix(')').expect("strip suffix ')'"),
            };
            match sql.strip_prefix(format!("create table {}\n(\n", table_name).as_str()) {
                Some(sql) => {
                    for line in sql.lines() {
                        if line.trim().to_lowercase().starts_with(column_name) {
                            column_found = true;
                            break;
                        }
                        column_ordinal += 1;
                    }
                    num_cols = sql.lines().collect::<Vec<_>>().len();
                }
                None => {
                    sql = match sql.strip_prefix(format!("create table {} (", table_name).as_str())
                    {
                        Some(sql) => sql,
                        None => sql
                            .strip_prefix(format!("create table \"{}\" (", table_name).as_str())
                            .unwrap_or_else(|| {
                                panic!("strip prefix: create table \"{}\" (", table_name)
                            }),
                    };
                    let columns = sql.split(',').collect::<Vec<_>>();
                    for col in &columns {
                        if col.trim().starts_with(column_name) {
                            column_found = true;
                            break;
                        }
                        column_ordinal += 1;
                    }
                    num_cols = columns.len();
                }
            };
        }
        if !column_found {
            Err(SqlError::NoSuchColumn(column_name.to_string()))?
        }

        let mut result = Vec::with_capacity(num_rows as usize);

        // Since this is a leaf page, its header is eight-bytes long, so we have to skip that many bytes
        // from the start of the page to get to the cell pointer array which represents offsets to the cell contents,
        // i.e., offsets to rows.
        //
        // The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number
        // of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
        // The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and
        // the right-most cell (the cell with the largest key) last.
        let mut offset = page_offset + 8;
        let mut row_offsets = Vec::new();
        for _ in 0..num_rows {
            let mut buf = [0u8; 2];
            let _pos = db_file.seek(SeekFrom::Start(offset))?;
            db_file.read_exact(&mut buf)?;
            // All multibyte values in the page header are big-endian.
            row_offsets.push(u16::from_be_bytes(buf));
            offset += 2;
        }
        // We don't have to loop twice, but we'll do it for clarity. This code is not meant for production.
        // Loop over rows.
        for row_offset in row_offsets {
            let offset = page_offset + row_offset as u64;
            let _pos = db_file.seek(SeekFrom::Start(offset))?;
            // B-tree Cell Format
            let (_payload_size, _read) = read_varint(db_file)?;
            let (_row_id, _read) = read_varint(db_file)?;
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
            let _column_data_size = header_size as u64 - read;
            let mut column_serial_type = 0;
            // Our column's offset after record header. Used for an optimization only.
            let mut column_offset = 0;
            // We are looking for our column only and early-stopping when we find it.
            // We don't want to read and extract sizes of all columns.
            // That wouldn't be efficient, especially in case there's a lot of columns.
            for col_ind in 0..num_cols {
                let (col_serial_type, _read) = read_varint(db_file)?;
                column_serial_type = col_serial_type;
                let column_content_size = serial_type_to_content_size(column_serial_type)? as u64;
                if col_ind == column_ordinal {
                    break;
                }
                column_offset += column_content_size;
            }
            let offset = header_start + header_size as u64 + column_offset;
            let _pos = db_file.seek(SeekFrom::Start(offset))?;
            let column_contents = match read_serial_type_to_content(db_file, column_serial_type)? {
                (SerialTypeValue::Text(column_contents), _read) => column_contents,
                (SerialTypeValue::Int8(column_contents), _read) => column_contents.to_string(),
                (stv, read) => panic!(
                    "Got unexpected type for column '{column_name}': {stv:?}; read {read} byte(s)."
                ),
            };
            result.push(column_contents);
        }

        Ok(result)
    } else {
        // In case a table with the given name, `table_name`, does not exist in the database, return that error.
        Err(SqlError::NoSuchTable(table_name.to_string()))?
    }
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
        let command = "   SeLEcT    cOUnt(*)   FroM   OrangeS   ;   ";
        let expected = 6.to_string();
        let result = &select("sample.db", command).unwrap()[0];
        assert_eq!(expected, *result);
    }

    #[test]
    fn select_name_from_apples() {
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
    fn select_color_from_apples() {
        let command = "   SeleCT    cOLor   frOM   APPles  ;   ";
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
}
