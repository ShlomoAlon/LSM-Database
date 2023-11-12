use std::cmp::Ordering::{Equal, Greater, Less};
use std::future::Future;
use genawaiter::stack::{Co, Gen, let_gen};
type Child = Option<Box<Node>>;

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
                } else if (key == inner_node.key) {
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

    fn scan(node: &Child, lower_bound: i64, upper_bound: i64) -> Vec<(i64, i64)> {
        match node {
            None => {
                vec![]
            }
            Some(inner) => {
                if inner.key < lower_bound {
                    Self::scan(&inner.right, lower_bound, upper_bound)
                } else if inner.key > upper_bound {
                    Self::scan(&inner.left, lower_bound, upper_bound)
                } else {
                    let mut result = Self::scan(&inner.left, lower_bound, upper_bound);
                    result.push((inner.key, inner.value));
                    result.extend(Self::scan(&inner.right, lower_bound, upper_bound));
                    result
                }
            }
        }
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

    fn get_all_gen(&self) -> impl Iterator<Item = (i64, i64)>{
        gen {
            for i in self.get_all(self){
                yield i;
            }
        }
    }
}
struct MemoryTable {
    mem_table_size: usize,
    cur_size: usize,
    root: Child,
}

impl MemoryTable {
    fn new(mem_table_size: usize) -> Self {
        MemoryTable {
            mem_table_size: mem_table_size,
            cur_size: 0,
            root: None,
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

    fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)> {
        Node::scan(&self.root, key1, key2)
    }

    fn get(&self, key: i64) -> Option<i64> {
        self.root.as_ref().and_then(|node| node.get(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
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
    fn test_scan() {
        let mut mem_table = MemoryTable::new(1000);
        for i in 500..1000 {
            assert!(mem_table.insert(i, i));
        }
        assert_equal(mem_table.scan(0, 1000), (500..1000).map(|x| (x, x)));
    }

    #[test]
    fn test_scan_on_insert_backwards(){
        let mut mem_table = MemoryTable::new(10000);

        for i in (500..10000).rev(){
            assert!(mem_table.insert(i, i));
        }
        assert_equal(mem_table.scan(0, 10000), (500..10000).map(|x| (x, x)));
    }
}
