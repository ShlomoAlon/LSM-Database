use crate::bloom_filter::{CACHE_LINE_SIZE_BYTES, NUM_CACHE_LINES};
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::slice::{from_raw_parts, from_raw_parts_mut};

/// represents a cache line
type CacheLine = [u8; CACHE_LINE_SIZE_BYTES as usize];
const_assert_eq!(
    std::mem::size_of::<CacheLine>(),
    CACHE_LINE_SIZE_BYTES as usize
);
const_assert!(PAGE_SIZE % NUM_CACHE_LINES == 0);

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_SIZE_I64: usize = 4096 / 8;

pub const TOMBSTONE_U8: u8 = u8::MAX;
pub const TOMBSTONE: i64 = i64::MAX;

// Note that we shouldn't need to use repr(C, align(4096)) because the buffer is 4096 bytes and
// it should always be aligned to 4096 bytes. However, I'm not sure if rust guarantees this. So
// I'm going to be safe.
#[repr(C, align(4096))]
#[derive(Debug, Clone)]
pub struct InnerBuffer {
    buffer: [u8; PAGE_SIZE],
}

impl InnerBuffer {
    /// implemented the function here so that I can use the const attribute.
    /// While the jury is out on whethere this improves performance. It's possible that in the
    /// future it will.
    const fn as_mut_slice<A>(&mut self) -> &mut [A] {
        debug_assert!(PAGE_SIZE % std::mem::size_of::<A>() == 0);
        debug_assert!(std::mem::align_of::<A>() <= 4096);
        // This is safe because the buffer is 4096 bytes (and is one unique allocation)
        // and the alignment of A is less than 4096
        // bytes. and 4096 is a multiple of the size of A.
        unsafe {
            from_raw_parts_mut(
                self.buffer.as_mut_ptr() as *mut A,
                PAGE_SIZE / std::mem::size_of::<A>(),
            )
        }
    }
    /// implemented the function here so that I can use the const attribute.
    /// While the jury is out on whethere this improves performance. It's possible that in the
    /// future it will.
    const fn as_slice<A>(&self) -> &[A] {
        debug_assert!(PAGE_SIZE % std::mem::size_of::<A>() == 0);
        debug_assert!(std::mem::align_of::<A>() <= 4096);
        // This is safe because the buffer is 4096 bytes (and is one unique allocation)
        // and the alignment of A is less than 4096
        // bytes. and 4096 is a multiple of the size of A.
        unsafe {
            from_raw_parts(
                self.buffer.as_ptr() as *const A,
                PAGE_SIZE / std::mem::size_of::<A>(),
            )
        }
    }
}

/// This is a reference counted aligned (to 4096 bytes) buffer. When you read and write from disk, you do it directly
/// to this buffer. You can then add it directly to the cache without copying any data.
/// Due to it being reference counted we can return it from the cache without worrying about it
/// being dropped (or copying it).
///
/// It also has a bunch of helper methods for transmuting the buffer to different types.
/// This is very unsafe to read data from disk that has been written on a different endian machine.
/// So don't do that.
#[derive(Debug, Clone)]
pub struct Buffer {
    pub(crate) inner_buffer: Rc<InnerBuffer>,
}

impl Buffer {
    pub fn new() -> Self {
        let mut result = Self {
            inner_buffer: Rc::new(InnerBuffer {
                buffer: [0; PAGE_SIZE],
            }),
        };
        result.as_mut_slice_i64().fill(TOMBSTONE);
        result
    }

    pub fn new_0() -> Self {
        let mut result = Self {
            inner_buffer: Rc::new(InnerBuffer {
                buffer: [0; PAGE_SIZE],
            }),
        };
        result
    }
    pub fn as_mut_slice<A>(&mut self) -> &mut [A] {
        debug_assert!(Rc::strong_count(&self.inner_buffer) == 1);
        // Safety: this function should only ever be called in the context where we haven't added it to the
        // cache yet. And thus never called clone so the strong count is 1.
        unsafe { Rc::get_mut_unchecked(&mut self.inner_buffer).as_mut_slice() }
    }
    pub fn as_slice<A>(&self) -> &[A] {
        self.inner_buffer.as_slice()
    }
    pub fn as_slice_i64(&self) -> &[i64] {
        // this works because rust type coerces as_slice<A>() too as_slice<i64>().
        self.as_slice()
    }
    pub fn as_mut_slice_i64(&mut self) -> &mut [i64] {
        self.as_mut_slice()
    }
    pub fn as_slice_pair(&self) -> &[(i64, i64)] {
        self.as_slice()
    }
    pub fn as_mut_slice_pair(&mut self) -> &mut [(i64, i64)] {
        self.as_mut_slice()
    }
    pub fn as_mut_slice_u8(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
    pub fn as_slice_u8(&self) -> &[u8] {
        self.as_slice()
    }
    pub fn as_mut_cache_lines(&mut self) -> &mut [CacheLine] {
        self.as_mut_slice()
    }
    pub fn as_cache_lines(&self) -> &[CacheLine] {
        self.as_slice()
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_buffer() {
        // assert!(false);
        let mut buffer = Buffer::new();
        for i in 0..PAGE_SIZE_I64 {
            buffer.as_mut_slice_i64()[i] = i as i64;
        }
        for i in 0..PAGE_SIZE_I64 {
            assert_eq!(buffer.as_slice_i64()[i], i as i64);
        }
        for i in 0..PAGE_SIZE_I64 / 2 {
            assert_eq!(
                buffer.as_slice_pair()[i],
                (i as i64 * 2 as i64, i as i64 * 2 + 1 as i64)
            );
        }
        for i in 0..PAGE_SIZE_I64 / 2 {
            buffer.as_mut_slice_pair()[i] = (i as i64, i as i64);
        }
        for i in 0..PAGE_SIZE_I64 / 2 {
            assert_eq!(buffer.as_slice_pair()[i], (i as i64, i as i64));
            assert_eq!(buffer.as_slice_i64()[i * 2], i as i64);
        }
    }
    #[test]
    fn check_empty_buffer() {
        let mut buffer = Buffer::new();
        assert_eq!(buffer.as_slice_i64(), [TOMBSTONE; PAGE_SIZE_I64]);
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
