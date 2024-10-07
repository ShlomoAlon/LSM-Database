use std::fmt::{Debug, Formatter};
use std::ptr;

pub struct LinkListNode<T> {
    next: *mut LinkListNode<T>,
    prev: *mut LinkListNode<T>,
    pub(crate) data: T,
}

pub(crate) struct LinkedList<T> {
    head: *mut LinkListNode<T>,
    tail: *mut LinkListNode<T>,
    pub(crate) valid: bool,
    pub(crate) len: usize,
}

impl<T> LinkListNode<T> {
    pub(crate) fn new(data: T) -> Self {
        LinkListNode {
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
            data,
        }
    }
}

impl<T: Debug> Debug for LinkedList<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.iter().collect::<Vec<_>>().fmt(f)
    }
}
pub struct NodeIter<T> {
    list: LinkedList<T>,
}

pub struct IntoIter<T> {
    list: LinkedList<T>,
}

impl<T> LinkedList<T> {
    pub fn into_iter(self) -> IntoIter<T> {
        IntoIter { list: self }
    }
    pub fn into_node_iter(self) -> NodeIter<T> {
        NodeIter { list: self }
    }
}

impl<T> IntoIterator for LinkedList<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        self.into_iter()
    }
}

impl<T> Iterator for IntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.list.pop_front()
    }
}

impl<T> Iterator for NodeIter<T> {
    type Item = Box<LinkListNode<T>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.list.pop_front_node()
    }
}

pub struct Iter<'a, T> {
    next: *mut LinkListNode<T>,
    _boo: std::marker::PhantomData<&'a T>,
}

impl<'a, T> IntoIterator for &'a LinkedList<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            next: self.head,
            _boo: std::marker::PhantomData,
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next.is_null() {
            None
        } else {
            let result = unsafe { &(*self.next).data };
            self.next = unsafe { (*self.next).next };
            Some(result)
        }
    }
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        LinkedList {
            head: ptr::null_mut(),
            tail: ptr::null_mut(),
            valid: true,
            len: 0,
        }
    }

    pub(crate) fn pop_back(&mut self) -> Box<LinkListNode<T>> {
        self.pop(self.tail)
    }

    pub(crate) fn push_node_front(&mut self, mut node: Box<LinkListNode<T>>) {
        node.next = self.head;
        let node = Box::into_raw(node);
        if self.head.is_null() {
            debug_assert!(self.len == 0);
            self.head = node;
            self.tail = node;
        } else {
            unsafe {
                (*self.head).prev = node;
            }
            self.head = node;
        }
        self.len += 1;
    }

    fn push_front(&mut self, data: T) -> *mut LinkListNode<T> {
        let node = Box::new(LinkListNode::new(data));
        let result = Box::into_raw(node);
        unsafe {
            self.push_node_front(Box::from_raw(result));
        }
        result
    }

    pub(crate) fn pop(&mut self, node: *mut LinkListNode<T>) -> Box<LinkListNode<T>> {
        debug_assert!(self.len > 0);
        debug_assert!(node != ptr::null_mut());
        let mut node = unsafe { Box::from_raw(node) };
        if node.prev.is_null() {
            // node is head
            debug_assert!(self.head == &mut *node);
            self.head = node.next;
        } else {
            unsafe {
                (*node.prev).next = node.next;
            }
        }
        if node.next.is_null() {
            debug_assert!(self.tail == &mut *node);
            debug_assert!(self.len >= 1);
            self.tail = node.prev;
        } else {
            unsafe {
                (*node.next).prev = node.prev;
            }
        }
        self.len -= 1;
        node.next = ptr::null_mut();
        node.prev = ptr::null_mut();
        node
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        self.pop_front_node().map(|item| item.data)
    }
    pub(crate) fn pop_front_node(&mut self) -> Option<Box<LinkListNode<T>>> {
        if self.head.is_null() {
            debug_assert!(self.len == 0);
            None
        } else {
            Some(self.pop(self.head))
        }
    }

    fn pop_and_push_front(&mut self, node: *mut LinkListNode<T>) {
        let item = self.pop(node);
        self.push_node_front(item);
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }
    fn iter(&self) -> Iter<T> {
        Iter {
            next: self.head,
            _boo: std::marker::PhantomData,
        }
    }
}

impl<T> Drop for LinkedList<T> {
    fn drop(&mut self) {
        while let Some(_) = self.pop_front() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::rev;

    #[test]
    fn test_numbers() {
        let mut linked_list_head = LinkedList::new();
        assert_eq!(linked_list_head.len(), 0);
        assert!(linked_list_head.is_empty());
        let first = linked_list_head.push_front(1);
        let second = linked_list_head.push_front(2);
        linked_list_head.push_front(3);
        assert_eq!(linked_list_head.len(), 3);
        assert_eq!(
            linked_list_head.iter().collect::<Vec<_>>(),
            vec![&3, &2, &1]
        );
        linked_list_head.pop(second);
        assert_eq!(linked_list_head.iter().collect::<Vec<_>>(), vec![&3, &1]);
        linked_list_head.pop_front();
        assert_eq!(linked_list_head.iter().collect::<Vec<_>>(), vec![&1]);
        assert_eq!(linked_list_head.len, 1);
        linked_list_head.pop(first);
        assert!(linked_list_head.iter().collect::<Vec<_>>() == Vec::<&i32>::new());
        assert!(linked_list_head.len == 0);
        for i in rev(0..1000) {
            linked_list_head.push_front(i);
        }
        assert_eq!(linked_list_head.len(), 1000);
        assert_eq!(
            linked_list_head
                .iter()
                .map(|item| *item)
                .collect::<Vec<_>>(),
            (0..1000).collect::<Vec<_>>()
        );
        assert_eq!(linked_list_head.len(), 1000);

        for _i in 0..999 {
            linked_list_head.pop_back();
        }
        assert_eq!(linked_list_head.iter().find(|item| **item == 0), Some(&0));
        assert_eq!(linked_list_head.len(), 1);
        assert_eq!(linked_list_head.iter().collect::<Vec<_>>(), vec![&0]);
    }
    // #[test]
    // fn test_item_tester(){
    //     let mut linked_list_head = LinkedList::new();
    //     let first = linked_list_head.push_front(ItemTester::new(1, 2));
    //     let second = linked_list_head.push_front(ItemTester::new(2, 5));
    //     assert_eq!(linked_list_head.iter().find(|item| item.hash() == 1), Some(&ItemTester::new(1, 2)));
    // }
}
