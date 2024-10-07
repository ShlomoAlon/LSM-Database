#![cfg_attr(target_os = "windows", feature(windows_file_ext))]
#![cfg_attr(target_os = "linux", feature(unix_file_ext))]

use positioned_io::{RandomAccessFile, ReadAt, Size};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

// use std::fs::OpenOptions;
// use std::os::windows::fs::{FileExt, OpenOptionsExt};
// use std::os::unix::fs::{FileExt, OpenOptionsExt};
use crate::buffer::{Buffer, PAGE_SIZE};

#[derive(Debug)]
pub struct Reader {
    pub(crate) file: RandomAccessFile,
    pub(crate) file_name: String,
}

impl Reader {
    pub(crate) fn new(file_name: &str) -> Self {
        #[cfg(target_os = "windows")]
        let file = OpenOptions::new().read(true).open(file_name).unwrap();

        #[cfg(target_os = "linux")]
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(file_name)
            .unwrap();
        let file = RandomAccessFile::try_new(file).unwrap();
        Self {
            file,
            file_name: file_name.to_string(),
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file.size().unwrap().unwrap()
    }

    pub fn read_page(&mut self, buffer: &mut Buffer, page_num: u64) {
        self.file
            .read_exact_at(page_num * PAGE_SIZE as u64, buffer)
            .unwrap();
    }
}

pub struct Writer {
    pub file: File,
    pub name: String,
}

impl Writer {
    pub(crate) fn new(file_name: String) -> Self {
        // if fs::metadata(&file_name).is_ok() {
        //     // If it exists, delete the file
        //     if let Err(err) = fs::remove_file(&file_name) {
        //         eprintln!("Error deleting file: {}", err);
        //     } else {
        //         println!("File deleted successfully!");
        //     }
        // }
        #[cfg(target_os = "windows")]
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .truncate(true)
            .open(file_name.to_string())
            .unwrap();

        #[cfg(target_os = "linux")]
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_DIRECT)
            .truncate(true)
            .open(file_name.to_string())
            .unwrap();

        // let file = File::create(file_name.to_string()).unwrap();
        Self {
            file,
            name: file_name,
        }
    }

    pub fn write_page(&mut self, buffer: &Buffer) {
        self.file.write_all(buffer).unwrap();
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

    #[cfg_attr(miri, ignore)]
    #[test]
    fn write_all_ones() {
        let _ = std::fs::remove_file("test");
        let mut writer = Writer::new("test".to_string());
        let mut buffer = Buffer::new();
        for i in 0..10 {
            buffer.as_mut_slice_i64().fill(i);
            writer.write_page(&buffer);
        }
        let mut reader = Reader::new("test");
        for i in 0..10 {
            reader.read_page(&mut buffer, i);
            assert_eq!(buffer.as_slice_i64(), &[i as i64; PAGE_SIZE / 8]);
        }
    }
}
