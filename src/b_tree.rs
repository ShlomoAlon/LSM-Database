use std::{fs, mem};
use arrayvec::ArrayVec;
use crate::write_and_read::Writer;
use crate::write_and_read::Reader;
use crate::buffer::{Buffer, PAGE_SIZE_I64};
use crate::buffer::PAGE_SIZE;
use crate::cache_trait::Cache;
use crate::compaction::{LevelIterator, ReaderIterator, ScanIterator, PAGE_SIZE_AS_PAIR};

/// used for writing the bottom level of the BTree
/// Because we don't put the bottom level in the cache, we don't need to reallocate the buffer
/// Hence why we have a different struct for the bottom level.
pub struct Level0Writer{
    writer: Writer,
    buffer: Buffer,
    size_in_pairs: u64,
}

impl Level0Writer{
    fn new(file_name: String) -> Level0Writer{
        let writer = Writer::new(file_name);
        let buffer = Buffer::new();
        Level0Writer{
            writer,
            buffer,
            size_in_pairs: 0,
        }
    }
    fn add_pair(&mut self, item: (i64, i64)) -> bool{
        self.buffer.as_mut_slice_pair()[self.size_in_pairs as usize]  = item;
        self.size_in_pairs += 1;
        if self.size_in_pairs == PAGE_SIZE_I64 as u64 / 2 {
            self.writer.write_page(&self.buffer);
            self.size_in_pairs = 0;
            true
        } else {
            false
        }
    }

    fn finish(&mut self){
        if self.size_in_pairs != 0 {
            let pair = (i64::MAX, i64::MAX);
            for i in self.size_in_pairs..PAGE_SIZE_I64 as u64 / 2{
                self.buffer.as_mut_slice_pair()[i as usize] = pair;
            }
            self.writer.write_page(&self.buffer);
        }
    }
}

/// used for writing the levels of the BTree that are not the bottom level.
/// This allows us to stream the data from disk without having to load the entire bottom level into memory.
pub struct LevelWriter{
    writer: Writer,
    size_in_i64: u64,
    buffer: Buffer,
}

impl LevelWriter{
    fn new(file_name: String) -> LevelWriter{
        let writer = Writer::new(file_name);
        LevelWriter{
            writer,
            size_in_i64: 0,
            buffer: Buffer::new(),
        }
    }
    fn add_i64<A: Cache>(&mut self, item: i64, cache: &mut A) -> bool{
        let index_in_buffer = self.size_in_i64 % PAGE_SIZE_I64 as u64;
        self.buffer.as_mut_slice_i64()[index_in_buffer as usize] = item;
        self.size_in_i64 += 1;
        if self.size_in_i64 % PAGE_SIZE_I64 as u64 == 0 {
            let mut old_buffer = Buffer::new();
            mem::swap(&mut self.buffer, &mut old_buffer);
            cache.write_page(&mut self.writer, self.size_in_i64 / PAGE_SIZE_I64 as u64 - 1, old_buffer);
            true
        } else {
            false
        }
    }

    fn finish<A: Cache>(&mut self, cache: &mut A){
        if self.size_in_i64 % PAGE_SIZE_I64 as u64 != 0 {
            let index = self.size_in_i64 % PAGE_SIZE_I64 as u64;
            for i in index..PAGE_SIZE_I64 as u64{
                self.buffer.as_mut_slice_i64()[i as usize] = i64::MAX;
            }
            let mut old_buffer = Buffer::new();
            mem::swap(&mut self.buffer, &mut old_buffer);
            cache.write_page(&mut self.writer, self.size_in_i64 / PAGE_SIZE_I64 as u64, old_buffer);
        }
    }
}
#[derive(Debug)]
pub struct BTreeReader{
    /// the readers for all levels of the BTree. The first reader is the bottom level and the last reader is the top level
    readers: ArrayVec<Reader, 10>,
}
//
impl BTreeReader{
    pub(crate) fn new(file_name_prefix: String, levels: usize) -> BTreeReader{
        let mut readers = ArrayVec::new();
        let file_name = format!("{}.items.btree", file_name_prefix);
        let reader = Reader::new(file_name.as_str());
        readers.push(reader);
        // let level = 0;
        for i in 0..levels{
            let file_name = format!("{}.level{}.btree", file_name_prefix, i);
            let reader = Reader::new(file_name.as_str());
            readers.push(reader);
        }
        BTreeReader{
            readers,
        }
    }
    /// gets the index of the buffer in the bottom level such that buffer[0] <= key <= buffer[last]
    /// in other words the index of the bottom level buffer that could potentially contain that key
    fn get_bottom_index<A: Cache>(&mut self, key: i64, cache: &mut A) -> usize{
        debug_assert!(self.readers.len() >= 1);
        let mut level = self.readers.len() - 1;
        let mut index: usize = 0;
        debug_assert!(self.readers[level].file_size() <= PAGE_SIZE as u64);
        // dbg!(&self.readers[level].file_name);
        // dbg!(self.readers[level].file_size());
        debug_assert!(self.readers[level].file_size() == PAGE_SIZE as u64);
        while level != 0{
            let buffer = cache.get_page(&mut self.readers[level], index as u64, true, true);
            let index_intermediate = buffer.as_slice_i64().binary_search(&key).unwrap_or_else(|i| i);
            index = index_intermediate + index * PAGE_SIZE_I64;
            level -= 1;
        }
        index
    }
    /// gets the item with the given key if it exists.
    pub(crate) fn get_item<A: Cache>(& mut self, key: i64, cache: & mut A) -> Option<i64>{
        let index = self.get_bottom_index(key, cache);
        let buffer = cache.get_page(&mut self.readers[0], index as u64, true, true);
        let item = buffer.as_slice_pair().binary_search_by_key(&key, |i| i.0).ok();
        item.map(|i| buffer.as_slice_pair()[i].1)
    }

    pub fn into_level_iter(self) -> LevelIterator{
        LevelIterator::LevelN(ReaderIterator::new(self.readers[0].file_name.clone()))
    }

    pub fn range<A: Cache>(&mut self, lower_bound: i64, upper_bound: i64, cache: &mut A) -> ScanIterator{
        let lower_index = self.get_bottom_index(lower_bound, cache) * PAGE_SIZE_AS_PAIR as usize;

        let mut iter = ReaderIterator::new_with_upper_bound(self.readers[0].file_name.clone(), upper_bound, lower_bound, lower_index as u64, Buffer::new());
        ScanIterator::LevelN(iter)
    }

    fn delete(&mut self){
        for i in 0..self.readers.len(){
            fs::remove_file(self.readers[i].file_name.as_str()).unwrap();
        }
    }
}

/// the writer for the Btree. The interface allows you to add a single item at a time.
/// The writer will only store as many buffers as their are levels. Even if you needed to merge
/// the entire LSM tree at once you would only be storing at most 10 buffers.
/// Though a lot more would be added to the cache if it could fit. (only the upper levels are cached)
pub struct BTreeWriter{
    file_name_prefix: String,
    /// the buffers for all upper levels of the BTree
    pub buffers: ArrayVec<LevelWriter, 10>,
    /// the buffers for the bottom level of the BTree
    /// They are a different type because we don't need to reallocate the buffer for the bottom level
    /// (since it's not writing to the cache.
    pub top_level: Level0Writer,
}
impl BTreeWriter {
    pub(crate) fn new(file_name_prefix: String) -> BTreeWriter {
        let file_name = format!("{}.items.btree", file_name_prefix);
        let s = BTreeWriter {
            buffers: ArrayVec::new(),
            file_name_prefix,
            top_level: Level0Writer::new(file_name),
        };
        s
    }
    fn add_item_level<A: Cache>(&mut self, item: i64, level: usize, cache: &mut A){
        if level == self.buffers.len() {
            let file_name = format!("{}.level{}.btree", self.file_name_prefix, level);
            self.buffers.push(LevelWriter::new(file_name));
        }
        if self.buffers[level].add_i64(item, cache){
            self.add_item_level(item, level + 1, cache);
        }
    }
    pub fn add_item<A: Cache>(&mut self, item: (i64, i64), cache: &mut A){
        if self.top_level.add_pair(item){
            self.add_item_level(item.0, 0, cache);
        }
    }
    pub fn finish<A: Cache>(&mut self, cache: &mut A) -> usize{
        self.top_level.finish();
        for i in 0..self.buffers.len(){
            self.buffers[i].finish(cache);
        }
        self.buffers.len()
    }
}
//
//
#[cfg(test)]
mod tests {
    use crate::cache_trait::NoCache;
    use super::*;

    #[test]
    fn test_writer_one_level(){
        let mut cache = NoCache;
        let mut writer = BTreeWriter::new("testing/test1".to_string());
        for i in 0..PAGE_SIZE/16{
            writer.add_item((i as i64, (i + 1) as i64), &mut cache);
        }
        assert_eq!(writer.finish(&mut cache), 1);
        // let mut file = Reader::new("test.level0.btree");
        // let mut buffer = Buffer::new();
        // file.read_page(&mut buffer, 0);
        // dbg!(buffer.as_slice_i64());
        let mut reader = BTreeReader::new("testing/test1".to_string(), 0);
        for i in 0..PAGE_SIZE/16{
            let item = reader.get_item(i as i64, &mut cache);
            assert_eq!(item, Some((i + 1) as i64));
        }
        reader.delete();
    }

    #[test]
    fn test_writer_level_2(){
        let mut cache = NoCache;
        let mut writer = BTreeWriter::new("testing/test2".to_string());
        for i in 0..PAGE_SIZE/16 * PAGE_SIZE/8 - 10{
            writer.add_item((i as i64, (i + 1) as i64), &mut cache);
        }
        assert_eq!(writer.finish(&mut cache), 1);
        let mut reader = BTreeReader::new("testing/test2".to_string(), 1);

        let item = reader.get_item(256, &mut cache);
        assert_eq!(item, Some(257));
        let item = reader.get_item(256 * 230, &mut cache);
        assert_eq!(item, Some(256 * 230 + 1));
        let item = reader.get_item(256 * 210 + 70, &mut cache);
        assert_eq!(item, Some(256 * 210 + 70 + 1));
        reader.delete();
    }

    #[test]
    fn test_level_3(){
        if fs::metadata("testing").is_ok() {
            fs::remove_dir_all("testing").unwrap();
        }
        fs::create_dir("testing").unwrap();
        let mut writer = BTreeWriter::new("testing/test3".to_string());
        let mut cache = NoCache;
        let length = PAGE_SIZE/16 * PAGE_SIZE/8 * 10;
        for i in 0..length{
            writer.add_item((i as i64, (i + 1) as i64), &mut cache);
        }
        // writer.add_item(((length - 1) as i64, length as i64), &mut cache);
        // assert_eq!(writer.top_level.size_in_pairs, length as u64 % (PAGE_SIZE_I64 as u64 / 2));
        assert_eq!(writer.finish(&mut cache), 2);
        let mut reader = BTreeReader::new("testing/test3".to_string(), 2);
        let iter: Vec<_> = reader.into_level_iter().collect();
        dbg!(iter.len());
        dbg!(iter[iter.len() - 1]);
        dbg!(length);
        for i in 0..length{
            assert_eq!(iter[i], (i as i64, (i + 1) as i64));
        }
        assert_eq!(iter.len(), PAGE_SIZE/16 * PAGE_SIZE/8 * 10);
        let mut reader = BTreeReader::new("testing/test3".to_string(), 2);
        assert_eq!(reader.get_item(length as i64 - 10, &mut cache), Some((length - 9) as i64));
        assert_eq!(reader.readers[0].file_size() /  16, length as u64);

        let item = reader.get_item(256 * 256 * 5 + 70 + 1, &mut cache);
        assert_eq!(item, Some(256 * 256 * 5 + 70 + 2));
        let item = reader.get_item(256, &mut cache);
        assert_eq!(item, Some(257));
        let item = reader.get_item(256 * 230, &mut cache);
        assert_eq!(item, Some(256 * 230 + 1));
        let item = reader.get_item(256 * 210 + 70, &mut cache);
        assert_eq!(item, Some(256 * 210 + 70 + 1));
        // reader.delete();
    }
}

#[cfg(test)]
mod tests2 {
    use super::*;
    use crate::cache_trait::NoCache;

    fn setup_btree(items: Vec<(i64, i64)>) -> (BTreeReader, NoCache) {
        let mut cache = NoCache;
        // remove the testing directory if it exists and create a new one
        if fs::metadata("testing").is_ok() {
            fs::remove_dir_all("testing").unwrap();
        }
        fs::create_dir("testing").unwrap();

        let mut writer = BTreeWriter::new("testing/range_test".to_string());
        for item in items {
            writer.add_item(item, &mut cache);
        }
        let levels = writer.finish(&mut cache);
        let reader = BTreeReader::new("testing/range_test".to_string(), levels);
        (reader, cache)
    }

    #[test]
    fn test_range_single_item() {
        let (mut reader, mut cache) = setup_btree(vec![(5, 50)]);
        let result: Vec<_> = reader.range(0, 10, &mut cache).collect();
        assert_eq!(result, vec![(5, 50)]);
    }

    #[test]
    fn test_range_multiple_items() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(2, 6, &mut cache).collect();
        assert_eq!(result, vec![(3, 30), (5, 50)]);
    }

    #[test]
    fn test_range_lower_bound() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(3, 10, &mut cache).collect();
        assert_eq!(result, vec![(3, 30), (5, 50), (7, 70)]);
    }

    #[test]
    fn test_range_upper_bound() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(0, 5, &mut cache).collect();
        assert_eq!(result, vec![(1, 10), (3, 30), (5, 50)]);
    }

    #[test]
    fn test_range_exact_bounds() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(1, 7, &mut cache).collect();
        assert_eq!(result, vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
    }

    #[test]
    fn test_range_out_of_bounds() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(10, 20, &mut cache).collect();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_range_large_dataset() {
        let items: Vec<_> = (0..10000).map(|i| (i, i * 10)).collect();
        let (mut reader, mut cache) = setup_btree(items);

        let mut item = reader.get_item(5000, &mut cache);
        assert_eq!(item, Some(50000));
        dbg!(item);
        let result: Vec<_> = reader.range(5000, 5010, &mut cache).collect();
        assert_eq!(result, vec![(5000, 50000), (5001, 50010), (5002, 50020), (5003, 50030), (5004, 50040), (5005, 50050), (5006, 50060), (5007, 50070), (5008, 50080), (5009, 50090), (5010, 50100)]);
    }

    #[test]
    fn test_range_reverse_bounds() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(7, 1, &mut cache).collect();
        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_range_same_bounds() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(5, 5, &mut cache).collect();
        assert_eq!(result, vec![(5, 50)]);
    }

    #[test]
    fn test_range_non_existent_bounds() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (5, 50), (7, 70)]);
        let result: Vec<_> = reader.range(2, 6, &mut cache).collect();
        assert_eq!(result, vec![(3, 30), (5, 50)]);
    }

    #[test]
    fn test_range_with_duplicates() {
        let (mut reader, mut cache) = setup_btree(vec![(1, 10), (3, 30), (3, 31), (5, 50), (5, 51), (7, 70)]);
        let result: Vec<_> = reader.range(2, 6, &mut cache).collect();
        assert_eq!(result, vec![(3, 30), (3, 31), (5, 50), (5, 51)]);
    }

    // Clean up after tests

}




