use crate::avl_tree::{NodeIter, ScanIter};
use crate::buffer::{Buffer, PAGE_SIZE_I64, TOMBSTONE};
use crate::write_and_read::Reader;

pub const PAGE_SIZE_AS_PAIR: u64 = (PAGE_SIZE_I64 / 2) as u64;
pub struct ReaderIterator {
    reader: Reader,
    buffer: Buffer,
    pub(crate) index: u64,
    upper_bound: i64,
    lower_bound: i64,
}

impl ReaderIterator {
    pub(crate) fn new(file_name: String) -> ReaderIterator {
        let reader = Reader::new(file_name.as_str());
        let buffer = Buffer::new();
        ReaderIterator {
            reader,
            buffer,
            index: 0,
            upper_bound: TOMBSTONE,
            lower_bound: i64::MIN,
        }
    }

    pub(crate) fn new_with_upper_bound(
        file_name: String,
        upper_bound: i64,
        lower_bound: i64,
        index: u64,
        buffer: Buffer,
    ) -> ReaderIterator {
        let reader = Reader::new(file_name.as_str());
        ReaderIterator {
            reader,
            buffer,
            index,
            upper_bound,
            lower_bound,
        }
    }
    fn next(&mut self) -> Option<(i64, i64)> {
        if self.index % (PAGE_SIZE_AS_PAIR) == 0 {
            if self.index == self.reader.file_size() / 16 as u64 {
                return None;
            }
            self.reader
                .read_page(&mut self.buffer, self.index / (PAGE_SIZE_AS_PAIR));
        }
        let pair = self.buffer.as_slice_pair()[self.index as usize % PAGE_SIZE_AS_PAIR as usize];
        if pair.0 == TOMBSTONE {
            debug_assert!(pair.1 == TOMBSTONE);
            return None;
        }
        if pair.0 > self.upper_bound {
            return None;
        }
        if pair.0 < self.lower_bound {
            self.index = self
                .buffer
                .as_slice_pair()
                .binary_search(&(self.lower_bound, 0))
                .unwrap_or_else(|x| x) as u64;
            return self.next();
        }
        let result = Some(pair);
        self.index += 1;
        result
    }
}

pub enum ScanIterator<'a> {
    Memtable(ScanIter<'a>),
    LevelN(ReaderIterator),
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = (i64, i64);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScanIterator::Memtable(iter) => iter.next(),
            ScanIterator::LevelN(iter) => iter.next(),
        }
    }
}

pub enum LevelIterator {
    Memtable(NodeIter),
    LevelN(ReaderIterator),
}

impl Iterator for LevelIterator {
    type Item = (i64, i64);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LevelIterator::Memtable(iter) => iter.next(),
            LevelIterator::LevelN(iter) => iter.next(),
        }
    }
}
