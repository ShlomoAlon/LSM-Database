use std::cell::RefCell;
use std::ptr;
use std::rc::Rc;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;

mod linked_list;

use linked_list::{LinkedList, LinkListNode};


struct CacheItem<T>{
    item: T,
    node: *mut LinkListNode<LRUItem<T>>,
    head_of_node: *mut LinkedList<CacheItem<T>>,
}


#[derive(Debug)]
struct ItemTester{
    hash: u64,
    value: u64,
}

impl ItemTester {
    fn new(hash: u64, value: u64) -> Self{
        ItemTester{
            hash,
            value,
        }
    }
}

trait KeyHash {
    fn hash(&self) -> u64;
}

impl KeyHash for ItemTester {
    fn hash(&self) -> u64 {
        self.hash
    }
}

impl PartialEq<Self> for ItemTester {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Eq for ItemTester {

}

impl<T: Debug> Debug for CacheItem<T>{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.item.fmt(f)
    }
}

struct LRUItem<T>{
    node: *mut LinkListNode<CacheItem<T>>,
}

impl<T> LRUItem<T> {
    fn pop_in_cache(&mut self) -> Box<LinkListNode<CacheItem<T>>> {
        unsafe{
            (*(*self.node).data.head_of_node).pop(self.node)
        }
    }
}

impl<T: Debug> Debug for LRUItem<T>{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe { (*self.node).data.fmt(f) }
    }
}


type LRU<T> = LinkedList<LRUItem<T>>;
type RCcacheItem<T> = Rc<RefCell<LinkedList<CacheItem<T>>>>;
#[derive(Debug)]
struct Cache<T: KeyHash>{
    directory: Vec<RCcacheItem<T>>,
    exponent: usize,
    max_bucket_size: usize,
    max_length: usize,
    lru: LRU<T>,
}



impl<T: KeyHash + Eq> Cache<T> {
    fn new(exponent: usize, max_bucket_size: usize) -> Self{
        Cache{
            directory: (0..2^exponent).map(|_| Rc::new(RefCell::new(LinkedList::new()))).collect(),
            exponent,
            max_bucket_size,
            max_length: 100,
            lru: LinkedList::new(),
        }
    }

    fn create_new_cache_item(&mut self, item: T) -> Box<LinkListNode<CacheItem<T>>>{
        let mut cache_item = Box::new(LinkListNode::new(
                CacheItem{
                    item: item,
                    node: ptr::null_mut(),
                    head_of_node: ptr::null_mut(),
                }
        ));

        let mut lru_node = Box::new(LinkListNode::new(
            LRUItem{
                node: &mut * cache_item,
            }));

        cache_item.data.node = &mut *lru_node;
        self.lru.push_node_front(lru_node);
        cache_item
    }

    fn add_to_index(&mut self, index: usize, mut node: Box<LinkListNode<CacheItem<T>>>){
        let mut bucket = self.directory[index].deref().borrow_mut();
        if bucket.len > self.max_bucket_size && Rc::strong_count(&self.directory[index]) > 1 {
            bucket.valid = false;
        }
        if !bucket.valid{
            let copy = self.directory[index].clone();
            drop(bucket);
            self.directory[index] = Rc::new(RefCell::new(LinkedList::new()));
            let mut bucket = copy.deref().borrow_mut();
            while let Some(item) = bucket.pop_front_node() {
                self.add_node(item);
            }
            self.add_to_index(index, node);
        } else{
            node.data.head_of_node = self.directory[index].as_ptr();
            bucket.push_node_front(node);
        }
    }
    fn add_node(&mut self, node: Box<LinkListNode<CacheItem<T>>>){
        let index = node.data.item.hash() as usize & ((2^self.exponent) - 1);
        self.add_to_index(index, node);
    }
    fn add_item(&mut self, item: T){
        let node = self.create_new_cache_item(item);
        self.add_node(node);
        if self.lru.len >= self.max_length {
            let mut lru_node = self.lru.pop_back();
            lru_node.data.pop_in_cache();
            debug_assert!(self.lru.len == self.max_length - 1);
        }


    }

}



#[cfg(test)]
mod tests2 {
    // use super::*;

    #[test]
    fn test() {
        // let mut cache = Cache::new(10, 10);
        // cache.add_to_index(0, ItemTester::new(1, 2));
        // cache.add_to_index(0, ItemTester::new(2, 5));
        // dbg!(cache);

    }
}
