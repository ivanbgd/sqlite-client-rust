//! # Page

#[derive(Debug, PartialEq)]
pub enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0a,
    LeafTable = 0x0d,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            v => panic!("Wrong page type: {v}"),
        }
    }
}
