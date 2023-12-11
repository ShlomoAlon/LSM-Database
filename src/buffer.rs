use std::cmp::Ordering;

pub const PAGE_SIZE: usize = 4096;

/// searches range [start, finish) for the first index where f returns Ordering::Equal
/// if it doesn't find it, it returns the index where it would be inserted
/// or where [result - 1].item < key <= [result]
fn binary_search<F>(f: F, start: usize, finish: usize) -> Result<usize, usize>
where F: Fn(usize) -> Ordering
{
    let mut left = start;
    let mut right = finish;
    while left < right {
        let mid = left + (right - left)/ 2;
        let cmp = f(mid);
        if cmp == Ordering::Less {
            left = mid + 1;
        } else if cmp == Ordering::Greater {
            right = mid;
        } else {
            return Ok(mid);
        }
    }
    Err(left)
}

#[repr(C, align(4096))]
#[derive(Debug, Clone)]
pub struct Buffer{
    pub(crate) b: [u8; PAGE_SIZE],
    pub(crate) size: usize,
}

impl Buffer{
    pub fn new() -> Self{
        Self{
            b: [0u8; PAGE_SIZE],
            size: 0,
        }
    }
    pub fn is_full(&self) -> bool{
        self.size == PAGE_SIZE
    }
    pub fn add(&mut self, item: (i64, i64)) -> bool{
        debug_assert!(self.size % 16 == 0);
        if self.size == PAGE_SIZE {
            return false
        }
        let value1 = item.0;
        let value2 = item.1;
        let bytes1 = value1.to_be_bytes();
        let bytes2 = value2.to_be_bytes();
        self.b[self.size..self.size + 8].copy_from_slice(&bytes1);
        self.b[self.size + 8..self.size + 16].copy_from_slice(&bytes2);
        self.size += 16;
        true
    }
    pub fn add_i64(&mut self, item: i64) -> bool{
        debug_assert!(self.size % 8 == 0);
        if self.size == PAGE_SIZE {
            return false
        }
        let bytes = item.to_be_bytes();
        self.b[self.size..self.size + 8].copy_from_slice(&bytes);
        self.size += 8;
        true
    }

    pub fn index_pair(&self, index: usize) -> (i64, i64){
        debug_assert!(self.size % 16 == 0);
        debug_assert!(index < self.size / 16);
        let bytes1 = &self.b[index * 16..index * 16 + 8];
        let bytes2 = &self.b[index * 16 + 8..index * 16 + 16];
        let value1 = i64::from_be_bytes(bytes1.try_into().unwrap());
        let value2 = i64::from_be_bytes(bytes2.try_into().unwrap());
        (value1, value2)
    }

    pub fn index_i64(&self, index: usize) -> i64{
        debug_assert!(self.size % 8 == 0);
        debug_assert!(index < self.size / 8);
        let bytes = &self.b[index * 8..index * 8 + 8];
        let value = i64::from_be_bytes(bytes.try_into().unwrap());
        value
    }

    pub fn pair_iter(& self) -> impl Iterator<Item = (i64, i64)> + '_{
        (0..self.size/16).map(|i| self.index_pair(i))
    }

    pub fn i64_iter(& self) -> impl Iterator<Item = i64> + '_{
        (0..self.size/8).map(|i| self.index_i64(i))
    }

    pub fn reset(&mut self){
        self.size = 0;
    }

    pub fn find_item(&self, key: i64) -> Option<(i64, i64)>{
        let result = binary_search(
            |index| {
                self.index_pair(index).0.cmp(&key)
            },
            0,
            self.size / 16,
        );
        match result {
            Ok(index) => {
                Some(self.index_pair(index))
            }
            Err(_) => {
                None
            }
        }
    }

    pub fn find_index_smaller_i64(&self, key: i64) -> usize{
        let result = binary_search(
            |index| {
                self.index_i64(index).cmp(&key)
            },
            0,
            self.size / 8,
        );
        match result {
            Ok(index) => index,
            Err(index) => index,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_binary_search(){
        let list = vec![1, 2, 3, 4, 6, 6, 7, 8, 9];
        let result = binary_search(
            |index| {
                list[index].cmp(&5)
            },
            0,
            list.len(),
        );
        assert_eq!(result, Err(4));
        let result = binary_search(
            |index| {
                list[index].cmp(&7)
            },
            0,
            list.len(),
        );
        assert_eq!(result, Ok(6));
    }

    #[test]
    fn test_binary_search_on_buffer(){
        let mut buffer = Buffer::new();
        for i in 0..PAGE_SIZE/16{
            buffer.add((i as i64, i as i64));
        }
        for i in 0..PAGE_SIZE/16{
            assert_eq!(buffer.find_item(i as i64), Some((i as i64, i as i64)));
            assert_eq!(buffer.index_pair(i), (i as i64, i as i64));
        }
        let mut buffer = Buffer::new();
        for i in 0..PAGE_SIZE/8{
            buffer.add_i64((i * 2) as i64);
        }
        // dbg!(buffer.pair_iter().collect::<Vec<_>>());
        for i in 0..PAGE_SIZE/8{
            assert_eq!(buffer.index_i64(i), (i * 2) as i64);
            let index = i * 2;
            assert_eq!(buffer.find_index_smaller_i64(index as i64), i);
            let index = (i * 2) as i64 - 1;
            assert_eq!(buffer.find_index_smaller_i64(index as i64), i);
        }
    }
}
