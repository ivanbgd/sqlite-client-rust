//! # Errors
//!
//! Error types and helper functions used in the library

use crate::page::PageType;
use thiserror::Error;

/// Application errors
#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error(transparent)]
    CmdError(#[from] CmdError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors related to working with [`crate::cmd`]
#[derive(Debug, Error)]
pub enum CmdError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Unsupported PageType: {0:?}")]
    UnsupportedPageType(PageType),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
