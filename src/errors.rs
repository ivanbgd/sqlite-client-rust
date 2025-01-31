//! # Errors
//!
//! Error types and helper functions used in the library

use crate::constants::VarintType;
use crate::page::PageType;
use thiserror::Error;

/// Application errors
#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error(transparent)]
    CmdError(#[from] DotCmdError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors related to working with [`crate::dot_cmd`]
#[derive(Debug, Error)]
pub enum DotCmdError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unsupported PageType: {0:?}")]
    UnsupportedPageType(PageType),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors related to working with [`crate::sql`]
#[derive(Debug, Error)]
pub enum SqlError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unsupported PageType: {0:?}")]
    UnsupportedPageType(PageType),

    #[error("SELECT incomplete")]
    SelectIncomplete,

    #[error("SELECT unsupported variant: '{0}'")]
    SelectUnsupported(String),

    #[error("LIMIT parsing error: '{0}'")]
    LimitParsingError(String),

    #[error("No such table: '{0}'")]
    NoSuchTable(String),

    #[error("No such columns: '{0}'")]
    NoSuchColumns(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors related to working with [`crate::varint`]
#[derive(Debug, Error)]
pub enum VarintError {
    #[error("Reserved VarintType: {0}")]
    Reserved(VarintType),

    #[error("Unsupported VarintType: {0}")]
    Unsupported(VarintType),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
