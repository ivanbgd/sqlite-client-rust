//! # SQLite Application

use anyhow::{bail, Result};
use codecrafters_sqlite::dot_cmd::{dot_dbinfo, dot_tables};
use codecrafters_sqlite::sql::select;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }
    let db_file_path = &args[1];

    // Parse command and act accordingly
    let command = &args[2];

    // SQL commands
    if command.to_lowercase().starts_with("select") {
        println!("{}", select(db_file_path, command)?);
    } else {
        // CLI (dot) commands
        match command.to_lowercase().as_str() {
            ".dbinfo" => {
                dot_dbinfo(db_file_path)?;
            }
            ".tables" => {
                let names = dot_tables(db_file_path)?;
                for name in names {
                    print!("{name} ");
                }
                println!();
            }
            _ => bail!("Missing or invalid command passed: {}", command),
        }
    }

    Ok(())
}
