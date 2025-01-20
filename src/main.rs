//! # SQLite Application

use anyhow::{bail, Result};
use codecrafters_sqlite::dot_cmd::{dot_dbinfo, dot_tables};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let db_file_path = &args[1];
            dot_dbinfo(db_file_path)?;
        }
        ".tables" => {
            let db_file_path = &args[1];
            let names = dot_tables(db_file_path)?;
            for name in names {
                print!("{name} ");
            }
            println!();
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
