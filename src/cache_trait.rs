use crate::buffer::Buffer;
use crate::write_and_read::{Reader, Writer};

pub trait Cache: Sized + Default {
    /// at least one of check_cache and add_to_cache should be true
    fn get_page(&self, file_reader: &mut Reader, page_num: u64, check_cache: bool, add_to_cache: bool) -> Buffer;
    fn write_page(&mut self, file_writer: &mut Writer, page_num: u64, buffer: Buffer);
}

#[derive(Default)]
pub struct NoCache;

impl Cache for NoCache {
    fn get_page(&self, file_reader: &mut Reader, page_num: u64, _check_cache: bool, _add_to_cache: bool) -> Buffer {
        let mut buffer = Buffer::new();
        file_reader.read_page(&mut buffer, page_num);
        buffer
    }

    fn write_page(&mut self, file_writer: &mut Writer, _page_num: u64, buffer: Buffer) {
        file_writer.write_page(&buffer);
    }
}