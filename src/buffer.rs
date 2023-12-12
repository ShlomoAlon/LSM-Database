use std::cell::{RefCell, UnsafeCell};
use std::cmp::Ordering;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::slice::{from_raw_parts, from_raw_parts_mut};

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_I64: usize = 4096 / 8;

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
struct InnerBuffer{
    buffer: [i64; PAGE_SIZE_I64],
}

#[derive(Debug, Clone)]
pub struct Buffer{
    pub(crate) inner_buffer: Rc<UnsafeCell<InnerBuffer>>,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            inner_buffer: Rc::new(UnsafeCell::new(InnerBuffer {
                buffer: [0; PAGE_SIZE_I64],
            })),
        }
    }
    pub fn as_slice_i64(&self) -> &[i64] {
        unsafe {
            let inner_buffer = self.inner_buffer.get();
            from_raw_parts((*inner_buffer).buffer.as_ptr(), PAGE_SIZE_I64)
        }
    }
    pub fn as_mut_slice_i64(&mut self) -> &mut [i64] {
        unsafe {
            let inner_buffer = self.inner_buffer.get();
            from_raw_parts_mut((*inner_buffer).buffer.as_mut_ptr(), PAGE_SIZE_I64)
        }
    }
    pub fn as_slice_pair(&self) -> &[(i64, i64)] {
        unsafe {
            let inner_buffer = self.inner_buffer.get();
            from_raw_parts((*inner_buffer).buffer.as_ptr() as *const (i64, i64), PAGE_SIZE_I64 / 2)
        }
    }
    pub fn as_mut_slice_pair(&mut self) -> &mut [(i64, i64)] {
        unsafe {
            let inner_buffer = self.inner_buffer.get();
            from_raw_parts_mut((*inner_buffer).buffer.as_mut_ptr() as *mut (i64, i64), PAGE_SIZE_I64 / 2)
        }
    }
    pub fn as_mut_slice_u8(&mut self) -> &mut [u8] {
        unsafe {
            let inner_buffer = self.inner_buffer.get();
            from_raw_parts_mut((*inner_buffer).buffer.as_mut_ptr() as *mut u8, PAGE_SIZE)
        }
    }
    pub fn as_slice_u8(&self) -> &[u8] {
        unsafe {
            let inner_buffer = self.inner_buffer.get();
            from_raw_parts((*inner_buffer).buffer.as_ptr() as *const u8, PAGE_SIZE)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_buffer(){
        // assert!(false);
        let mut buffer = Buffer::new();
        for i in 0..PAGE_SIZE_I64{
            buffer.as_mut_slice_i64()[i] = i as i64;
        }
        for i in 0..PAGE_SIZE_I64{
            assert_eq!(buffer.as_slice_i64()[i], i as i64);
        }
        for i in 0..PAGE_SIZE_I64/2{
            assert_eq!(buffer.as_slice_pair()[i], (i as i64 * 2 as i64, i as i64 * 2 + 1 as i64));
        }
        for i in 0..PAGE_SIZE_I64/2{
            buffer.as_mut_slice_pair()[i] = (i as i64, i as i64);
        }
        for i in 0..PAGE_SIZE_I64/2{
            assert_eq!(buffer.as_slice_pair()[i], (i as i64, i as i64));
            assert_eq!(buffer.as_slice_i64()[i * 2], i as i64);
        }
    }
    #[test]
    fn check_empty_buffer(){
        let mut buffer = Buffer::new();
        assert_eq!(buffer.as_mut_slice_u8(), [0; PAGE_SIZE]);
    }
    // use super::*;
    // #[test]
    // fn test_binary_search(){
    //     let list = vec![1, 2, 3, 4, 6, 6, 7, 8, 9];
    //     let result = binary_search(
    //         |index| {
    //             list[index].cmp(&5)
    //         },
    //         0,
    //         list.len(),
    //     );
    //     assert_eq!(result, Err(4));
    //     let result = binary_search(
    //         |index| {
    //             list[index].cmp(&7)
    //         },
    //         0,
    //         list.len(),
    //     );
    //     assert_eq!(result, Ok(6));
    // }
    //
    // #[test]
    // fn test_binary_search_on_buffer(){
    //     let mut buffer = Buffer::new();
    //     for i in 0..PAGE_SIZE/16{
    //         buffer.add((i as i64, i as i64));
    //     }
    //     for i in 0..PAGE_SIZE/16{
    //         assert_eq!(buffer.find_item(i as i64), Some((i as i64, i as i64)));
    //         assert_eq!(buffer.index_pair(i), (i as i64, i as i64));
    //     }
    //     let mut buffer = Buffer::new();
    //     for i in 0..PAGE_SIZE/8{
    //         buffer.add_i64((i * 2) as i64);
    //     }
    //     // dbg!(buffer.pair_iter().collect::<Vec<_>>());
    //     for i in 0..PAGE_SIZE/8{
    //         assert_eq!(buffer.index_i64(i), (i * 2) as i64);
    //         let index = i * 2;
    //         assert_eq!(buffer.find_index_smaller_i64(index as i64), i);
    //         let index = (i * 2) as i64 - 1;
    //         assert_eq!(buffer.find_index_smaller_i64(index as i64), i);
    //     }
    // }
}
