use crate::buffer::{Buffer, PAGE_SIZE};
use crate::cache_trait::Cache;
use crate::write_and_read::{Reader, Writer};
use bitset_core::BitSet;
use siphasher::sip128::SipHasher13;

pub const CACHE_LINE_SIZE_BYTES: u8 = 64;
pub const NUM_CACHE_LINES: usize = PAGE_SIZE / CACHE_LINE_SIZE_BYTES as usize;
const_assert!(NUM_CACHE_LINES == 64);
const CACHE_LINE_SIZE_BITS: u64 = CACHE_LINE_SIZE_BYTES as u64 * 8;

const BITS_PER_PAGE: usize = 4096 * 8;
struct BloomFilterWriter {
    bloom_filter: Vec<Buffer>,
}

impl BloomFilterWriter {
    fn new(num_items: u64) -> BloomFilterWriter {
        let pages = num_items * 6 / BITS_PER_PAGE as u64 + 1;
        // dbg!(pages);
        // dbg!(num_items);
        let mut bloom_filter = Vec::with_capacity(pages as usize);
        for _ in 0..pages {
            bloom_filter.push(Buffer::new_0());
        }
        BloomFilterWriter { bloom_filter }
    }
    fn add_key(&mut self, key: i64) {
        let hashes = Hashes::new(self.bloom_filter.len() as u64, key);
        let disk_sector = &mut self.bloom_filter[hashes.disk_sector as usize];
        let cache_line = &mut disk_sector.as_mut_cache_lines()[hashes.cache_line as usize];
        cache_line.bit_set(hashes.cache_line_offset1.into());
        cache_line.bit_set(hashes.cache_line_offset2.into());
        cache_line.bit_set(hashes.cache_line_offset3.into());
        cache_line.bit_set(hashes.cache_line_offset4.into());
    }

    fn write_to_disk<A: Cache>(self, file_name: String, cache: &mut A) {
        let mut writer = Writer::new(file_name);
        for (i, buffer) in self.bloom_filter.into_iter().enumerate() {
            cache.write_page(&mut writer, i as u64, buffer)
        }
    }
}

struct BloomFilterReader {
    file_reader: Reader,
    num_pages: u64,
}

impl BloomFilterReader {
    fn new(file_name: String) -> BloomFilterReader {
        let file_reader = Reader::new(file_name.as_str());
        let num_pages = file_reader.file_size() / PAGE_SIZE as u64;
        BloomFilterReader {
            file_reader,
            num_pages,
        }
    }
    fn check_item<A: Cache>(&mut self, key: i64, cache: &mut A) -> bool {
        let hashes = Hashes::new(self.num_pages, key);
        let disk_sector = cache.get_page(&mut self.file_reader, hashes.disk_sector, true, false);
        let cache_line = &disk_sector.as_cache_lines()[hashes.cache_line as usize];
        cache_line.bit_test(hashes.cache_line_offset1.into())
            && cache_line.bit_test(hashes.cache_line_offset2.into())
            && cache_line.bit_test(hashes.cache_line_offset3.into())
            && cache_line.bit_test(hashes.cache_line_offset4.into())
    }
}

struct Hashes {
    disk_sector: u64,
    cache_line: u8,
    cache_line_offset1: u16,
    cache_line_offset2: u16,
    cache_line_offset3: u16,
    cache_line_offset4: u16,
}
impl Hashes {
    fn new(num_disk_sectors: u64, key: i64) -> Hashes {
        let hasher = SipHasher13::new();
        let hash1 = hasher.hash(&key.to_ne_bytes());
        let disk_sector = hash1.h1 % num_disk_sectors;
        // use the last 8 bits of the second hash to determine which cache line to use
        let cache_line = (hash1.h2 & CACHE_LINE_SIZE_BYTES as u64 - 1) as u8;
        // use next 11
        // dbg!(CACHE_LINE_SIZE_BITS);
        // dbg!(2_u64.pow(9) as u64);
        debug_assert!(CACHE_LINE_SIZE_BITS == 2_u64.pow(9) as u64);
        let cache_line_offset1 = (hash1.h2 >> 6) & (CACHE_LINE_SIZE_BITS - 1);
        let cache_line_offset2 = (hash1.h2 >> 15) & (CACHE_LINE_SIZE_BITS - 1);
        let cache_line_offset3 = (hash1.h2 >> 24) & (CACHE_LINE_SIZE_BITS - 1);
        let cache_line_offset4 = (hash1.h2 >> 33) & (CACHE_LINE_SIZE_BITS - 1);
        Hashes {
            disk_sector: disk_sector,
            cache_line: cache_line as u8,
            cache_line_offset1: cache_line_offset1 as u16,
            cache_line_offset2: cache_line_offset2 as u16,
            cache_line_offset3: cache_line_offset3 as u16,
            cache_line_offset4: cache_line_offset4 as u16,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache_trait::NoCache;
    use std::fs;
    #[test]
    fn test_bloom_filter_one_page() {
        if fs::metadata("bloom_filter1").is_ok() {
            fs::remove_file("bloom_filter1").unwrap();
        }
        let mut cache = NoCache;
        let mut bloom_filter_writer = BloomFilterWriter::new(100);
        for i in 0..100 {
            bloom_filter_writer.add_key(i);
        }
        bloom_filter_writer.write_to_disk("bloom_filter1".to_string(), &mut cache);
        let mut bloom_filter_reader = BloomFilterReader::new("bloom_filter1".to_string());
        assert_eq!(bloom_filter_reader.num_pages, 1);
        for i in 0..100 {
            assert!(bloom_filter_reader.check_item(i, &mut cache));
        }
        for i in 100..200 {
            assert!(!bloom_filter_reader.check_item(i, &mut cache));
        }
    }

    #[test]
    fn test_bloom_filter_two_pages() {
        if fs::metadata("bloom_filter2").is_ok() {
            fs::remove_file("bloom_filter2").unwrap();
        }
        let mut cache = NoCache;
        let mut bloom_filter_writer = BloomFilterWriter::new((PAGE_SIZE * 2) as u64);
        for i in 0..1000 {
            bloom_filter_writer.add_key(i);
        }
        bloom_filter_writer.write_to_disk("bloom_filter2".to_string(), &mut cache);
        let mut bloom_filter_reader = BloomFilterReader::new("bloom_filter2".to_string());
        assert_eq!(bloom_filter_reader.num_pages, 2);
        for i in 0..1000 {
            assert!(bloom_filter_reader.check_item(i, &mut cache));
        }
        for i in 1000..2000 {
            assert!(!bloom_filter_reader.check_item(i, &mut cache));
        }
    }
}
