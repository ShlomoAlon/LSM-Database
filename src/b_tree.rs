// use arrayvec::ArrayVec;
// use crate::buffered_reader::BufferedReader;
// use crate::write_and_read::Writer;
// use crate::write_and_read::Reader;
// use crate::buffer::Buffer;
// use crate::buffer::PAGE_SIZE;
// struct BTreeReader{
//     /// the readers for all levels of the BTree. The first reader is the bottom level and the last reader is the top level
//     readers: ArrayVec<Reader, 10>,
// }
//
// impl BTreeReader{
//     /// gets the index of the buffer in the bottom level such that buffer[0] <= key <= buffer[last]
//     /// in other words the index of the bottom level buffer that could potentially contain that key
//     fn get_bottom_index(&mut self, key: i64, buffered_reader: &mut BufferedReader) -> u64{
//         debug_assert!(self.readers.len() >= 1);
//         let mut level = self.readers.len() - 1;
//         let mut index = 0;
//         debug_assert!(self.readers[level].file_size() <= PAGE_SIZE as u64);
//         debug_assert!(self.readers[level].file_size() % 8 == 0);
//         while level != 0{
//             buffered_reader.read_buffer(&mut self.readers[level], index);
//             let index_intermediate = buffered_reader.buffer().find_index_smaller_i64(key) as u64;
//             // dbg!(index_intermediate);
//             // dbg!(buffered_reader.buffer.index_i64(index_intermediate as usize));
//             index = index_intermediate + index * PAGE_SIZE as u64 / 8 + index;
//             level -= 1;
//             // dbg!(buffered_reader.buffer.i64_iter().collect::<Vec<_>>() );
//             // dbg!(index);
//             // dbg!(level);
//         }
//         index
//     }
//     /// gets the item with the given key if it exists.
//     fn get_item(& mut self, key: i64, buffered_reader: & mut BufferedReader) -> Option<i64>{
//         let index = self.get_bottom_index(key, buffered_reader);
//         buffered_reader.read_buffer(&mut self.readers[0], index);
//         // dbg!(buffered_reader.buffer.pair_iter().collect::<Vec<_>>() );
//         let item = buffered_reader.buffer().find_item(key);
//         item.map(|i| i.1)
//     }
// }
//
// struct BTreeWriter{
//     file_name_prefix: String,
//     buffers: ArrayVec<Buffer, 10>,
//     buffer_names: ArrayVec<Writer, 10>,
// }
//
// impl BTreeWriter {
//     fn new(file_name_prefix: String) -> BTreeWriter {
//         let buffer = Buffer::new();
//         let file_name = format!("{}.items.btree", file_name_prefix);
//         let writer = Writer::new(file_name);
//         let mut s = BTreeWriter {
//             buffers: ArrayVec::new(),
//             file_name_prefix,
//             buffer_names: ArrayVec::new()
//         };
//         s.buffers.push(buffer);
//         s.buffer_names.push(writer);
//         s
//     }
//
//     fn add_item_level(&mut self, item: i64, level: usize){
//         if level == self.buffers.len() {
//             let buffer = Buffer::new();
//             let file_name = format!("{}.level{}.btree", self.file_name_prefix, level);
//             let writer = Writer::new(file_name);
//             self.buffers.push(buffer);
//             self.buffer_names.push(writer);
//         }
//         if !self.buffers[level].add_i64(item){
//             self.buffer_names[level].write(&self.buffers[level]);
//             self.buffers[level].reset();
//             self.add_item_level(item, level + 1);
//         }
//     }
//     pub fn add_item(&mut self, item: (i64, i64)){
//         self.buffers[0].add(item);
//         if self.buffers[0].is_full(){
//             self.add_item_level(item.0, 1);
//             self.buffer_names[0].write(&self.buffers[0]);
//             self.buffers[0].reset();
//         }
//     }
//     pub fn finish(&mut self){
//         for i in 0..self.buffers.len(){
//             self.buffer_names[i].write(&self.buffers[i]);
//         }
//     }
// }
//
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_writer_one_level(){
//         let mut writer = BTreeWriter::new("test".to_string());
//         for i in 0..PAGE_SIZE/16{
//             writer.add_item((i as i64, (i + 1) as i64));
//         }
//         writer.finish();
//         let mut readers = ArrayVec::new();
//         assert_eq!(writer.buffer_names.len(), 2);
//         for i in writer.buffer_names{
//             let new_reader = Reader::new(i.name.as_str());
//             readers.push(new_reader);
//         }
//         let mut reader = BTreeReader{
//             readers: readers,
//         };
//         let mut buffered_reader = BufferedReader::new();
//         for i in 0..PAGE_SIZE/16{
//             let item = reader.get_item(i as i64, &mut buffered_reader);
//             assert_eq!(item, Some((i + 1) as i64));
//         }
//     }
//
//     #[test]
//     fn test_writer_level_2(){
//         let mut writer = BTreeWriter::new("test".to_string());
//         for i in 0..PAGE_SIZE/16 * PAGE_SIZE/8{
//             writer.add_item((i as i64, (i + 1) as i64));
//         }
//         writer.finish();
//         let mut readers = ArrayVec::new();
//         assert_eq!(writer.buffer_names.len(), 2);
//         for i in writer.buffer_names{
//             let new_reader = Reader::new(i.name.as_str());
//             readers.push(new_reader);
//         }
//         let mut reader = BTreeReader{
//             readers: readers,
//         };
//         let mut buffered_reader = BufferedReader::new();
//         let item = reader.get_item(256, &mut buffered_reader);
//         assert_eq!(item, Some(257));
//         let item = reader.get_item(256 * 230, &mut buffered_reader);
//         assert_eq!(item, Some(256 * 230 + 1));
//         let item = reader.get_item(256 * 210 + 70, &mut buffered_reader);
//         assert_eq!(item, Some(256 * 210 + 70 + 1));
//     }
//
//     #[test]
//     fn test_level_3(){
//         let mut writer = BTreeWriter::new("test".to_string());
//         for i in 0..PAGE_SIZE/16 * PAGE_SIZE/8 * 10{
//             writer.add_item((i as i64, (i + 1) as i64));
//         }
//         writer.finish();
//         let mut readers = ArrayVec::new();
//         assert_eq!(writer.buffer_names.len(), 3);
//         for i in writer.buffer_names{
//             let new_reader = Reader::new(i.name.as_str());
//             readers.push(new_reader);
//         }
//         let mut reader = BTreeReader{
//             readers: readers,
//         };
//         let mut buffered_reader = BufferedReader::new();
//         let item = reader.get_item(256 * 256 * 5 + 70 + 1, &mut buffered_reader);
//         assert_eq!(item, Some(256 * 256 * 5 + 70 + 2));
//         let item = reader.get_item(256, &mut buffered_reader);
//         assert_eq!(item, Some(257));
//         let item = reader.get_item(256 * 230, &mut buffered_reader);
//         assert_eq!(item, Some(256 * 230 + 1));
//         let item = reader.get_item(256 * 210 + 70, &mut buffered_reader);
//         assert_eq!(item, Some(256 * 210 + 70 + 1));
//
//
//     }
// }
//
//
