use crate::avl_tree::NodeIter;
use crate::buffer::{Buffer, PAGE_SIZE_I64};
use crate::write_and_read::Reader;

pub const PAGE_SIZE_AS_PAIR: u64 = (PAGE_SIZE_I64 / 2) as u64;
pub struct ReaderIterator{
    reader: Reader,
    buffer: Buffer,
    index: u64,
}

impl ReaderIterator{
    pub(crate) fn new(file_name: String) -> ReaderIterator{
        let reader = Reader::new(file_name.as_str());
        let buffer = Buffer::new();
        ReaderIterator{
            reader,
            buffer,
            index: 0,
        }
    }
    fn next(&mut self) -> Option<(i64, i64)>{
        if self.index % (PAGE_SIZE_AS_PAIR) == 0 {
            if self.index == self.reader.file_size() / PAGE_SIZE_AS_PAIR{
                return None;
            }
            self.reader.read_page(&mut self.buffer, self.index / (PAGE_SIZE_AS_PAIR));
        }
        let pair = self.buffer.as_slice_pair()[self.index as usize % PAGE_SIZE_AS_PAIR as usize];
        if pair.0 == i64::MAX{
            debug_assert!(pair.1 == i64::MAX);
            return None;
        }
        let result = Some(pair);
        self.index += 1;
        result
    }
}

pub enum LevelIterator{
    Memtable(NodeIter),
    LevelN(ReaderIterator),
}

impl Iterator for LevelIterator{
    type Item = (i64, i64);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LevelIterator::Memtable(iter) => iter.next(),
            LevelIterator::LevelN(iter) => iter.next(),
        }
    }
}