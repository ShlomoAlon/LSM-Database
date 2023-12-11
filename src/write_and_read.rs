use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use crate::buffer::{Buffer, PAGE_SIZE};

pub(crate) struct Reader{
    file: File,
}

impl Reader {
    pub(crate) fn new(file_name: &str) -> Self {
        let file = OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(file_name).unwrap();
        Self {
            file,
        }
    }

    pub fn file_size(&self) -> u64{
        self.file.metadata().unwrap().len()
    }

    pub fn read_at_page(&mut self, buffer: &mut Buffer, page_num: u64){
        self.read_at(buffer, page_num * PAGE_SIZE as u64);
    }
    fn read_at(&mut self, buffer: &mut Buffer, offset: u64){
        debug_assert!(offset % PAGE_SIZE as u64 == 0);
        let result = self.file.read_exact_at(& mut buffer.b, offset);
        match result {
            Ok(_) => {
                buffer.size = PAGE_SIZE;
            }
            Err(err) => {
                let result = self.file.read_at(& mut buffer.b, offset).unwrap();
                buffer.size = result;
            }
        }
    }
}

pub(crate) struct Writer{
    file: File,
    pub name: String

}

impl Writer {
    pub(crate) fn new(file_name: String) -> Self {
        let file = OpenOptions::new().write(true).create(true).custom_flags(libc::O_DIRECT).truncate(true).open(file_name.to_string()).unwrap();
        Self {
            file,
            name: file_name,
        }
    }


    pub(crate) fn write(&mut self, buffer: &Buffer) {
        self.file.write_all(&buffer.b[0..buffer.size]).unwrap();
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn unsafe_test(){
    //     unsafe {
    //         let mut data = 10;
    //         let ref1 = &mut data;
    //         let ptr2 = ref1 as *mut _;
    //
    //         // ORDER SWAPPED!
    //         *ref1 += 1;
    //         *ptr2 += 2;
    //
    //         println!("{}", data);
    //     }
    // }

    #[test]
    fn write_all_ones() {
        let mut writer = Writer::new("test_file".to_string());
        for i in 0 .. 100{
            let buffer = [i; PAGE_SIZE];
            let mut buffer = Buffer{
                b: buffer,
                size: PAGE_SIZE,
            };
            writer.write(&buffer);
        }

        let mut reader = Reader::new("test_file");
        for i in 0 .. 100{
            let mut buffer = Buffer{
                b: [i; PAGE_SIZE],
                size: PAGE_SIZE,
            };
            reader.read_at(&mut buffer, i as u64 * PAGE_SIZE as u64);
            assert_eq!(buffer.b, [i; PAGE_SIZE]);
        }

        // let mut file = File::open("test_file").unwrap();
        // let mut read_buffer = [0u8; PAGE_SIZE];
        // file.read_exact(&mut read_buffer).unwrap();
        // assert_eq!(read_buffer, buffer.b);
    }
}