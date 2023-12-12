use std::os::unix::fs::FileExt;
use crate::buffer::{Buffer, PAGE_SIZE_I64};
use crate::write_and_read::{Reader, Writer};

pub trait Read {
    fn read(&self, file_reader: &mut Reader, page_number: usize) -> Buffer;
    fn write(&self, file_writer: &mut Writer, page_number: usize, buffer: &Buffer);
    fn write_all(&self, file_name: String, page_number: usize, pages: Vec<Buffer>);
}

fn test_read<A: Read + Default>(){
    let mut reader = A::default();
    let mut writer = Writer::new("test".to_string());
    for i in 0 .. 10 {
        let mut buffer = Buffer::new();
        buffer.as_mut_slice_i64().fill(i as i64);
        reader.write(& mut writer, i, &buffer);
    }
    for i in 0 .. 10 {
        let buffer = reader.read(& mut Reader::new("test"), i);
        assert_eq!(buffer.as_slice_i64(), [i as i64; PAGE_SIZE_I64]);
    }
}

#[derive(Default)]
pub struct FileReadWriter;
impl Read for FileReadWriter {
    fn read(&self, file_reader: &mut Reader, page_number: usize) -> Buffer {
        let mut buffer = Buffer::new();
        file_reader.file.read_at(&mut buffer.as_mut_slice_u8(), page_number as u64 * 4096).unwrap();
        buffer
    }
    fn write(&self, file_writer: &mut Writer, page_number: usize, buffer: &Buffer) {
        file_writer.file.write_at( &buffer.as_slice_u8(),page_number as u64 * 4096).unwrap();
    }
    fn write_all(&self, file_name: String, page_number: usize, pages: Vec<Buffer>) {
        let mut file_writer = Writer::new(file_name);
        for (i, mut page) in pages.into_iter().enumerate() {
            file_writer.file.write_at( &page.as_slice_u8(),(page_number + i) as u64 * 4096).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_read_file(){
        test_read::<FileReadWriter>();
    }
}