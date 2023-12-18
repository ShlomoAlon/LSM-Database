use std::cmp::Ordering::{Equal, Greater, Less};
use crate::compaction::LevelIterator;

type Child = Option<Box<Node>>;
/// iterates over every element in the range [lower_bound, upper_bound]
/// Does not consume the tree
/// by default lower_bound is i64::MIN and upper_bound is i64::MAX
pub struct ScanIter<'a>{
    lower_bound: i64,
    upper_bound: i64,
    parents: Vec<&'a Node>,
    cur: &'a Child,
}

impl<'a> ScanIter<'a>{
    fn new(root: &'a Child) -> Self{
        ScanIter {
            lower_bound: i64::MIN,
            upper_bound: i64::MAX,
            parents: vec![],
            cur: &root,
        }
    }
    fn new_with_bounds(root: &'a Child, lower_bound: i64, upper_bound: i64) -> Self {
        debug_assert!(lower_bound <= upper_bound);
        ScanIter {
            lower_bound,
            upper_bound,
            parents: vec![],
            cur: &root,
        }
    }
}

impl <'a> Iterator for ScanIter<'a>{
    type Item = (i64, i64);
    fn next(&mut self) -> Option<Self::Item> {

        if let Some(node) = self.cur.as_ref(){
            if node.key < self.lower_bound{
                self.cur = &node.right;
                return self.next();
            } else if node.key > self.upper_bound{
                self.cur = &node.left;
                return self.next();
            }
            self.parents.push(node);
            self.cur = &node.left;
            self.next()
        } else {
            if self.parents.len() == 0{
                None
            } else {
                let node = self.parents.pop().unwrap();
                let result = Some((node.key, node.value));
                self.cur = &node.right;
                result
            }
        }
    }
}
/// consuming iterator over every element in the tree
pub struct NodeIter{
    parents: Vec<Child>,
    cur: Child,
}

impl NodeIter {
    fn new(root: Node) -> Self {
        NodeIter {
            parents: vec![],
            cur: Some(Box::new(root)),
        }
    }
}
impl Iterator for NodeIter{
    type Item = (i64, i64);
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut node) = self.cur.take() {
            let new_cur = node.left.take();
            self.parents.push(Some(node));
            self.cur = new_cur;
            self.next()
        } else {
            if self.parents.len() == 0 {
                None
            } else {
                let mut node = self.parents.pop().unwrap().unwrap();
                let result = Some((node.key, node.value));
                let new_cur = node.right.take();
                self.cur = new_cur;
                result
            }
        }
    }
}
#[derive(Debug)]
struct Node {
    key: i64,
    value: i64,
    height: i64,
    left: Child,
    right: Child,
}


impl Node {
    fn new(key: i64, value: i64) -> Box<Self> {
        Box::new(Node {
            key: key,
            value: value,
            height: 1,
            left: None,
            right: None,
        })
    }

    fn into_iter(self) -> NodeIter{
        NodeIter::new(self)
    }

    fn left_height(&self) -> i64 {
        self.left.as_ref().map_or(0, |node| node.height)
    }

    fn right_height(&self) -> i64 {
        self.right.as_ref().map_or(0, |node| node.height)
    }

    fn fix_height(&mut self) {
        self.height = 1 + std::cmp::max(self.left_height(), self.right_height());
    }

    fn insert(node: Child, key: i64, value: i64) -> Box<Self> {
        match node {
            None => Node::new(key, value),
            Some(mut inner_node) => {
                if key < inner_node.key {
                    inner_node.left = Some(Node::insert(inner_node.left, key, value));
                } else if key == inner_node.key {
                    inner_node.value = value;
                } else {
                    inner_node.right = Some(Node::insert(inner_node.right, key, value));
                }
                self::Node::balance(inner_node)
            }
        }
    }

    fn balance(mut node: Box<Node>) -> Box<Node> {
        node.fix_height();
        let balance = node.right_height() - node.left_height();
        if balance > 1 {
            if node.right.as_ref().unwrap().right_height()
                < node.right.as_ref().unwrap().left_height()
            {
                node.right = Some(Node::rotate_right(node.right.unwrap()));
            }
            node = Node::rotate_left(node);
        } else if balance < -1 {
            if node.left.as_ref().unwrap().left_height()
                < node.left.as_ref().unwrap().right_height()
            {
                node.left = Some(Node::rotate_left(node.left.unwrap()));
            }
            node = Node::rotate_right(node);
        }
        node
    }

    fn rotate_left(mut node: Box<Node>) -> Box<Node> {
        let mut top = node.right.take().unwrap();
        node.right = top.left.take();
        node.fix_height();
        top.left = Some(node);
        top.fix_height();
        top
    }
    fn rotate_right(mut node: Box<Node>) -> Box<Node> {
        let mut top = node.left.take().unwrap();
        node.left = top.right.take();
        node.fix_height();
        top.right = Some(node);
        top.fix_height();
        top
    }

    fn print_tree(&self, level: usize) {
        if let Some(right) = &self.right {
            right.print_tree(level + 1);
        }
        println!("{}{}: {}", " ".repeat(level * 4), self.key, self.value);
        if let Some(left) = &self.left {
            left.print_tree(level + 1);
        }
    }

    fn get(&self, key: i64) -> Option<i64> {
        match self.key.cmp(&key) {
            Equal => Some(self.value),
            Greater => self.left.as_ref().and_then(|node| node.get(key)),
            Less => self.right.as_ref().and_then(|node| node.get(key)),
        }
    }

    fn scan(node: &Child, lower_bound: i64, upper_bound: i64) -> ScanIter {
        ScanIter::new_with_bounds(node, lower_bound, upper_bound)
    }
    fn get_all(&self, vec: &mut Vec<u8>) {
        if let Some(left) = &self.left {
            left.get_all(vec);
        }
        vec.extend_from_slice(&mut self.key.to_ne_bytes());
        vec.extend_from_slice(&mut self.value.to_ne_bytes());
        if let Some(right) = &self.right {
            right.get_all(vec);
        }
    }
}
pub struct MemoryTable {
    mem_table_size: usize,
    cur_size: usize,
    root: Child,
}

impl MemoryTable {
    pub(crate) fn new(mem_table_size: usize) -> Self {
        MemoryTable {
            mem_table_size: mem_table_size,
            cur_size: 0,
            root: None,
        }
    }

    pub fn iter(&self) -> ScanIter {
        ScanIter::new(&self.root)
    }

    pub fn into_level_iter(mut self) -> LevelIterator{
        LevelIterator::Memtable(self.get_iter().unwrap())
    }

    fn get_iter(&mut self) -> Option<NodeIter>{
        if self.root.is_none(){
            None
        } else {
            self.cur_size = 0;
            Some(self.root.take().unwrap().into_iter())
        }
    }

    fn insert(&mut self, key: i64, value: i64) -> bool {
        if self.cur_size >= self.mem_table_size {
            return false;
        }
        self.root = Some(Node::insert(self.root.take(), key, value));
        self.cur_size += 1;
        true
    }

    fn scan(&self, key1: i64, key2: i64) -> ScanIter {
        Node::scan(&self.root, key1, key2)
    }

    fn get(&self, key: i64) -> Option<i64> {
        self.root.as_ref().and_then(|node| node.get(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::assert_equal;

    #[test]
    fn test_insert() {
        let mut mem_table = MemoryTable::new(100);
        for i in 0..100 {
            assert!(mem_table.insert(i, i));
        }
        assert!(!mem_table.insert(100, 100));
        for i in 0..100 {
            assert_eq!(mem_table.get(i), Some(i));
        }
    }

    #[test]
    pub fn into_iter2(){
        let num = 2;
        let mut root = Node::new(0, 0);
        let mut mem_table = MemoryTable::new(100);
        for i in 0..num{
            mem_table.insert(i, i);
        }
        let mut root2 = Node::new(0, 0);
        for i in 1..num{
            root = Node::insert(Some(root), i, i);
            root2 = Node::insert(Some(root2), i, i);
        }
        let mut iter = mem_table.iter();
        let mut iter2 = root2.into_iter();
        for i in 0 .. num{
            let next = iter2.next();
            // dbg!(next);
            // dbg!(iter2.next());
            assert_eq!(next, Some((i, i)));
            let next = iter.next();
            // dbg!(next);
            // dbg!(iter.next());
            assert_eq!(next, Some((i, i)));
        }
        assert_equal(mem_table.iter(), (0..num).map(|x| (x, x)));
        assert_equal(root.into_iter(), (0..num).map(|x| (x, x)));
    }
    #[test]
    pub fn into_iter(){
        let mut root = Node::new(0, 0);
        let mut root2 = Node::new(0, 0);
        for i in 1..100{
            root = Node::insert(Some(root), i, i);
            root2 = Node::insert(Some(root2), i, i);

        }
        let mut iter2 = root2.into_iter();
        for i in 0 .. 100{
            let next = iter2.next();
            // dbg!(next);
            // dbg!(iter2.next());
            assert_eq!(next, Some((i, i)));
            // dbg!(next);
            // dbg!(iter.next());
        }
        for i in 0 .. 100{
        }
        assert_equal(root.into_iter(), (0..100).map(|x| (x, x)));
    }

    #[test]
    fn test_scan() {
        let mut mem_table = MemoryTable::new(1000);
        for i in 500..1000 {
            assert!(mem_table.insert(i, i));
        }
        assert_equal(mem_table.scan(0, 1000), (500..1000).map(|x| (x, x)));
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn test_scan_on_insert_backwards(){
        let mut mem_table = MemoryTable::new(10000);

        for i in (500..10000).rev(){
            assert!(mem_table.insert(i, i));
        }
        assert_equal(mem_table.scan(0, 10000), (500..10000).map(|x| (x, x)));
    }


}
