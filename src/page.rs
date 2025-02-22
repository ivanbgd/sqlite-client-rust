//! # Page
//!
//! [1.6. B-tree Pages](https://www.sqlite.org/fileformat.html#b_tree_pages)
//!
//! A b-tree page is either a table b-tree page or an index b-tree page.
//! All pages within each complete b-tree are of the same type: either table or index.
//! There is one table b-tree in the database file for each rowid table in the database schema,
//! including system tables such as sqlite_schema.
//! There is one index b-tree in the database file for each index in the schema,
//! including implied indexes created by uniqueness constraints.
//!
//! Each entry in a table b-tree consists of a 64-bit signed integer key and up to 2147483647 bytes of arbitrary data.
//! (The key of a table b-tree corresponds to the rowid of the SQL table that the b-tree implements.)
//! Interior table b-trees hold only keys and pointers to children. All data is contained in the table b-tree leaves.

use crate::constants::DB_HEADER;
use anyhow::Result;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// B-tree page
#[derive(Debug, Clone)]
pub(crate) struct Page {
    pub(crate) contents: Vec<u8>,
    pub(crate) page_num: u32,
}

impl Page {
    /// Populates the [`Page`] structure by reading its entire contents at once from the database file.
    ///
    /// The desired page number is passed in through the `page_num` parameter.
    ///
    /// It reads a page in its entirety, i.e., in its full size.
    /// Page 1 is no exception because we don't skip reading the database header from it.
    ///
    /// Namely, page 1 (the database root page) is different from other pages in that it starts with
    /// a 100 byte-long database header, and its page header comes right after the database header.
    ///
    /// Every other page begins with a page header.
    ///
    /// Every page, including page 1, has a [`PageHeader`], which is 8 or 12 bytes long,
    /// and then it has some other data after it.
    ///
    /// This function doesn't discard the database header.
    ///
    /// This is important for the implementation of other [`Page`] functions as they have to skip
    /// the database header, but this makes using [`Page`]s much easier as they are kept uniform.
    pub(crate) fn new(db_file: &mut File, page_size: u32, page_num: u32) -> Result<Self> {
        assert!(page_num > 0, "Page number must be > 0.");

        // Pages are numbered from one, i.e., they are 1-indexed, so subtract one to get to the page.
        let page_start = (page_num - 1) * page_size;
        db_file.seek(SeekFrom::Start(page_start as u64))?;

        let mut contents = vec![0; page_size as usize];
        db_file.read_exact(&mut contents)?;

        Ok(Self { contents, page_num })
    }

    /// Returns the [`PageHeader`], which is 8 or 12 bytes long.
    ///
    /// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
    pub(crate) fn get_header(&self) -> PageHeader {
        let cont = if self.page_num == 1 {
            &self.contents[DB_HEADER.len()..][..DB_HEADER.len() + 12]
        } else {
            &self.contents[..12]
        };

        let (page_type, _page_header_len) = self.get_page_type();
        let freeblock_start = u16::from_be_bytes([cont[1], cont[2]]);
        let num_cells = u16::from_be_bytes([cont[3], cont[4]]);
        let cell_content_start = u16::from_be_bytes([cont[5], cont[6]]);
        let num_fragmented = cont[7];

        let mut rightmost_ptr = None;
        if page_type == PageType::TableInterior || page_type == PageType::IndexInterior {
            rightmost_ptr = Some(u32::from_be_bytes([cont[8], cont[9], cont[10], cont[11]]));
        }

        PageHeader {
            page_type,
            freeblock_start,
            num_cells,
            cell_content_start,
            num_fragmented,
            rightmost_ptr,
        }
    }

    /// Returns the cell pointer array from the page.
    ///
    /// The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number
    /// of cells on the btree. The cell pointer array consists of K 2-byte integer offsets to the cell contents.
    /// The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first
    /// and the right-most cell (the cell with the largest key) last.
    pub(crate) fn get_cell_ptr_array(&self) -> Vec<u16> {
        let page_num_cells = self.get_header().num_cells;
        let mut cell_pointer_array: Vec<u16> = Vec::with_capacity(page_num_cells as usize);
        let offset = if self.page_num == 1 {
            DB_HEADER.len()
        } else {
            0
        } + self.get_page_type().1 as usize;

        for i in 0..page_num_cells as usize {
            // The offsets are relative to the start of the page.
            let cell_ptr: [u8; 2] = [
                self.contents[offset + i * 2],
                self.contents[offset + i * 2 + 1],
            ];
            cell_pointer_array.push(u16::from_be_bytes(cell_ptr));
        }

        cell_pointer_array
    }

    /// Returns the [`PageType`], and the page header length in bytes.
    ///
    /// The page type is the first byte of the page header, be it page 1 of the database or any other page.
    ///
    /// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
    fn get_page_type(&self) -> (PageType, u8) {
        let page_type_byte = if self.page_num == 1 {
            self.contents[DB_HEADER.len()]
        } else {
            self.contents[0]
        };

        let (page_type, page_header_len) = match page_type_byte.into() {
            PageType::IndexInterior => (PageType::IndexInterior, 12),
            PageType::TableInterior => (PageType::TableInterior, 12),
            PageType::IndexLeaf => (PageType::IndexLeaf, 8),
            PageType::TableLeaf => (PageType::TableLeaf, 8),
        };

        (page_type, page_header_len)
    }
}

/// B-tree page header
///
/// The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages.
#[allow(unused)]
#[derive(Debug)]
pub(crate) struct PageHeader {
    pub(crate) page_type: PageType,
    pub(crate) freeblock_start: u16,
    pub(crate) num_cells: u16,
    pub(crate) cell_content_start: u16,
    pub(crate) num_fragmented: u8,
    pub(crate) rightmost_ptr: Option<u32>,
}

#[derive(Debug, PartialEq)]
pub enum PageType {
    IndexInterior = 0x02,
    TableInterior = 0x05,
    IndexLeaf = 0x0a,
    TableLeaf = 0x0d,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0x02 => PageType::IndexInterior,
            0x05 => PageType::TableInterior,
            0x0a => PageType::IndexLeaf,
            0x0d => PageType::TableLeaf,
            v => panic!("Wrong page type: {v}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::dot_cmd::page_size;
    use crate::page::{Page, PageType};
    use std::fs::File;

    #[test]
    fn page_header_sample_page_1() {
        let page_size = page_size("sample.db").unwrap();
        let mut db_file = File::open("sample.db").unwrap();
        let page = Page::new(&mut db_file, page_size, 1).unwrap();
        let page_header = page.get_header();
        assert_eq!(PageType::TableLeaf, page_header.page_type);
        assert_eq!(0, page_header.freeblock_start);
        assert_eq!(3, page_header.num_cells);
        assert_eq!(0x0ec3, page_header.cell_content_start);
        assert_eq!(0, page_header.num_fragmented);
        assert_eq!(None, page_header.rightmost_ptr);
        assert_eq!(8, page.get_page_type().1);
    }

    #[test]
    fn page_header_sample_page_2() {
        let page_size = page_size("sample.db").unwrap();
        let mut db_file = File::open("sample.db").unwrap();
        let page = Page::new(&mut db_file, page_size, 2).unwrap();
        let page_header = page.get_header();
        assert_eq!(PageType::TableLeaf, page_header.page_type);
        assert_eq!(0, page_header.freeblock_start);
        assert_eq!(4, page_header.num_cells);
        assert_eq!(0x0fa1, page_header.cell_content_start);
        assert_eq!(0, page_header.num_fragmented);
        assert_eq!(None, page_header.rightmost_ptr);
        assert_eq!(8, page.get_page_type().1);
    }

    #[test]
    fn page_header_superheroes_page_1() {
        let page_size = page_size("test_dbs/superheroes.db").unwrap();
        let mut db_file = File::open("test_dbs/superheroes.db").unwrap();
        let page = Page::new(&mut db_file, page_size, 1).unwrap();
        let page_header = page.get_header();
        assert_eq!(PageType::TableLeaf, page_header.page_type);
        assert_eq!(0x0fc9, page_header.freeblock_start);
        assert_eq!(2, page_header.num_cells);
        assert_eq!(0x0e8e, page_header.cell_content_start);
        assert_eq!(0, page_header.num_fragmented);
        assert_eq!(None, page_header.rightmost_ptr);
        assert_eq!(8, page.get_page_type().1);
    }

    #[test]
    fn page_header_page_header_superheroes_page_2() {
        let page_size = page_size("test_dbs/superheroes.db").unwrap();
        let mut db_file = File::open("test_dbs/superheroes.db").unwrap();
        let page = Page::new(&mut db_file, page_size, 2).unwrap();
        let page_header = page.get_header();
        assert_eq!(PageType::TableInterior, page_header.page_type);
        assert_eq!(0, page_header.freeblock_start);
        assert_eq!(0x006c, page_header.num_cells);
        assert_eq!(0x0d7a, page_header.cell_content_start);
        assert_eq!(0, page_header.num_fragmented);
        assert_eq!(Some(0x00_00_00_e8), page_header.rightmost_ptr);
        assert_eq!(12, page.get_page_type().1);
    }

    #[test]
    fn cell_ptr_array_sample_page_1() {
        let page_size = page_size("sample.db").unwrap();
        let mut db_file = File::open("sample.db").unwrap();
        let cell_ptr_array = Page::new(&mut db_file, page_size, 1)
            .unwrap()
            .get_cell_ptr_array();
        assert_eq!([0x0f8f, 0x0f3d, 0x0ec3], *cell_ptr_array);
    }

    #[test]
    fn cell_ptr_array_sample_page_2() {
        let page_size = page_size("sample.db").unwrap();
        let mut db_file = File::open("sample.db").unwrap();
        let cell_ptr_array = Page::new(&mut db_file, page_size, 2)
            .unwrap()
            .get_cell_ptr_array();
        assert_eq!([0x0fe3, 0x0fd6, 0x0fbd, 0x0fa1], *cell_ptr_array);
    }

    #[test]
    /// This is a table interior page.
    fn cell_ptr_array_superheroes_page_2() {
        let page_size = page_size("test_dbs/superheroes.db").unwrap();
        let mut db_file = File::open("test_dbs/superheroes.db").unwrap();
        let cell_ptr_array = Page::new(&mut db_file, page_size, 2)
            .unwrap()
            .get_cell_ptr_array();
        assert_eq!([0x0ffb, 0x0ff6, 0x0ff0, 0x0fea], cell_ptr_array[..4]);
    }
}
