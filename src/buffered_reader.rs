// use crate::write_and_read::Reader;
// use crate::buffer::Buffer;
// pub struct BufferedReader{
//     buffer: Buffer,
// }
//
// impl BufferedReader {
//     pub fn read_buffer(&mut self, reader: & mut Reader, page: u64) -> &Buffer{
//         reader.read_at_page(&mut self.buffer, page);
//         &self.buffer
//     }
//     pub fn buffer(&mut self) -> &Buffer{
//         &self.buffer
//     }
//     pub fn new() -> BufferedReader{
//         BufferedReader{
//             buffer: Buffer::new(),
//         }
//     }
// }