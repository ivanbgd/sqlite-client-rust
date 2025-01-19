//! # SQLite Application

use anyhow::{bail, Result};
use codecrafters_sqlite::cmd::dbinfo;

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
            dbinfo(db_file_path)?;
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
