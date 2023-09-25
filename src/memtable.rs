use std::cmp::Ordering::{Equal, Greater, Less};
use std::fmt::format;
use std::io::Write;
use std::path::Path;
use positioned_io::ReadAt;
use positioned_io::RandomAccessFile;
use rayon::prelude::*;
use itertools::Itertools;
use std::io::BufWriter;

const PAGE_SIZE: usize = 1000 * 4;
type Child = Option<Box<Node>>;

struct Node{
    key: i64,
    value: i64,
    height: i64,
    left: Child,
    right: Child
}

impl Node{

    fn new(key: i64, value: i64) -> Box<Self>{
        Box::new(Node{
            key: key,
            value: value,
            height: 1,
            left: None,
            right: None
        })
    }

    fn left_height(&self) -> i64 {
        self.left.as_ref().map_or(0, |node| node.height)
    }

    fn right_height(&self) -> i64 {
        self.right.as_ref().map_or(0, |node| node.height)
    }

    fn fix_height(&mut self) {
        self.height = 1 + std::cmp::max(
            self.left_height(),
            self.right_height()
        );
    }

    fn insert(node: Child, key: i64, value: i64) -> Box<Self> {
        match node {
            None => {
                Node::new(key, value)
            }
            Some(mut inner_node) => {
                if key < inner_node.key {
                    inner_node.left = Some(Node::insert(inner_node.left, key, value));
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
            if node.right.as_ref().unwrap().right_height() < node.right.as_ref().unwrap().left_height() {
                node.right = Some(Node::rotate_right(node.right.unwrap()));
            }
            node = Node::rotate_left(node);
        } else if balance < -1 {
            if node.left.as_ref().unwrap().left_height() < node.left.as_ref().unwrap().right_height() {
                node.left = Some(Node::rotate_left(node.left.unwrap()));
            }
            node = Node::rotate_right(node);
        }
        node
    }

    fn rotate_left(mut node: Box<Node>) -> Box<Node>{
        let mut top = node.right.take().unwrap();
        node.right = top.left.take();
        node.fix_height();
        top.left = Some(node);
        top.fix_height();
        top
    }
    fn rotate_right(mut node: Box<Node>) -> Box<Node>{
        let mut top = node.left.take().unwrap();
        node.left = top.right.take();
        node.fix_height();
        top.right = Some(node);
        top.fix_height();
        top
    }

    // fn delete(mut node: Option<Box<Node>>, key: i64) -> Option<Box<Node>>{
    //     panic!("Not implemented")
    // }

    fn print_tree(&self, level: usize) {
        if let Some(right) = &self.right {
            right.print_tree(level + 1);
        }
        println!("{}{}: {}", " ".repeat(level * 4), self.key, self.value);
        if let Some(left) = &self.left {
            left.print_tree(level + 1);
        }
    }

    fn get(&self, key: i64) -> Option<i64>{
        match self.key.cmp(&key) {
            Equal => Some(self.value),
            Greater => self.left.as_ref().and_then(|node| node.get(key)),
            Less => self.right.as_ref().and_then(|node| node.get(key))
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
}

struct MemoryTable{
    mem_table_size: usize,
    cur_size: usize,
    root: Child
}

impl MemoryTable{
    fn new(mem_table_size: usize) -> Self {
        MemoryTable{
            mem_table_size: mem_table_size,
            cur_size: 0,
            root: None
        }
    }

    fn insert(&mut self, key: i64, value: i64) -> bool{
        if self.cur_size >= self.mem_table_size {
            return false;
        }
        self.root = Some(Node::insert(self.root.take(), key, value));
        self.cur_size += 1;
        true
    }

    fn to_vec(&self) -> Vec<u8>{
        let tuples = Node::scan(&self.root, i64::MIN, i64::MAX);
        tuples_to_bytes(tuples)
    }

    fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)>{
        Node::scan(&self.root, key1, key2)
    }

    fn get(&self, key: i64) -> Option<i64>{
        self.root.as_ref().and_then(|node| node.get(key))
    }

    fn create_stable(&self, file_name: String) -> SSTable{
        let bytes = self.to_vec();
        let file_size = bytes.len();
        let mut file = std::fs::File::create(&file_name).unwrap();
        file.write_all(&bytes).unwrap();
        SSTable{
            file_name: file_name,
            file_size: file_size
        }
    }
}

fn tuples_to_bytes(tuples: Vec<(i64, i64)>) -> Vec<u8>{
    tuples.par_iter().flat_map(
        |(key, value)| {
            let mut key_bytes = key.to_be_bytes().to_vec();
            let mut value_bytes = value.to_be_bytes().to_vec();
            key_bytes.append(&mut value_bytes);
            key_bytes
        }
    ).collect()
}

fn bytes_to_tuples(bytes: Vec<u8>) -> Vec<(i64, i64)>{
    let mut result = vec![];
    let mut i = 0;
    while i < bytes.len() {
        let key = i64::from_be_bytes(bytes[i..i+8].try_into().unwrap());
        let value = i64::from_be_bytes(bytes[i+8..i+16].try_into().unwrap());
        result.push((key, value));
        i += 16;
    }
    result
}
#[derive(Debug)]
struct SSTable{
    file_name: String,
    file_size: usize,
}

impl SSTable{
    fn get_page(&self, key: u64) -> Vec<(i64, i64)>{
        let file = RandomAccessFile::open(&self.file_name).unwrap();
        let mut bytes: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let bytes_read = file.read_at(key * PAGE_SIZE as u64, &mut bytes).unwrap();
        bytes_to_tuples(bytes[..bytes_read].to_vec())
    }
    fn get_value(&self, key: i64) -> Option<i64> {
        let pages_to_search: Vec<_> = (0..(self.file_size + (PAGE_SIZE - 1)) / PAGE_SIZE).collect();
        let mut result = None;
        _ = pages_to_search.binary_search_by(|&page_num| {
            let page = self.get_page(page_num as u64);
            let first_key = page.first().unwrap().0;
            let last_key = page.last().unwrap().0;

            if key > last_key {
                return Less
            };
            if first_key > key {
                Greater
            } else if last_key >= key && key >= first_key {
                let result_index = page.binary_search_by(|&(k, _)| k.cmp(&key));
                if result_index.is_err() {
                    return Equal;
                }
                result = Some(page[result_index.unwrap()].1);
                Equal
            } else {
                panic!("Should not happen")
            }
        });
        result
    }
    fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)>{
        let pages_to_search: Vec<_> = (0..(self.file_size + (PAGE_SIZE - 1)) / PAGE_SIZE).collect();
        let mut first_page = Vec::new();
        let mut cur_page_index = pages_to_search.len();
        _ = pages_to_search.binary_search_by(|&page_num| {
            let page = self.get_page(page_num as u64);
            let first_key = page.first().unwrap().0;
            let last_key = page.last().unwrap().0;

            if key1 > last_key {
                return Less
            };
            first_page = page;
            cur_page_index = page_num;
            if first_key > key1 {
                Greater
            } else if last_key >= key1 && key1 >= first_key {
                Equal
            } else {
                panic!("Should not happen")
            }
        });
        if first_page.is_empty() {
            return vec![];
        }
        let first_page_starting_element_index = first_page.partition_point(|&(key, _)| key < key1);
        let mut most_recent_page = first_page[first_page_starting_element_index..].to_vec();
        let mut result = Vec::new();
        loop {
            // if most_recent_page.last().is_none() {
            //     dbg!(first_page.clone());
            //     dbg!(self.clone());
            // }
            if most_recent_page.last().unwrap().0 > key2 {
                let page_partition_point = most_recent_page.partition_point(|&(key, _)| key <= key2);
                result.extend(most_recent_page[..page_partition_point].to_vec());
                break;
            }
            result.extend(most_recent_page);
            if cur_page_index == pages_to_search.len() {
                break;
            }
            most_recent_page = self.get_page(pages_to_search[cur_page_index] as u64);
            cur_page_index += 1;
        }
        result
    }
}






struct Database{
    table_size: u64,
    path: String,
    mem_table: MemoryTable,
    ss_tables: Vec<SSTable>
}

impl Database {

    fn new(path: String, table_size: u64) -> Self {
        let path = Path::new(&path);
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        std::fs::create_dir(path).unwrap();
        Self::from_disk(path.to_str().unwrap().to_string(), table_size)

    }
    fn from_disk(path: String, table_size: u64) -> Self {
        let items = std::fs::read_dir(path.clone()).unwrap();
        let mut ss_table_names = Vec::new();
        for item in items {
            let item = item.unwrap();
            let file_name = item.file_name().into_string().unwrap();
            if file_name.ends_with(".sst") {
                ss_table_names.push(SSTable{
                    file_name: file_name,
                    file_size: item.metadata().unwrap().len() as usize
                });
            }
        }
        ss_table_names.sort_by(|a, b| {
            let a_file_number = a.file_name.split(".").next().unwrap().parse::<u64>().unwrap();
            let b_file_number = b.file_name.split(".").next().unwrap().parse::<u64>().unwrap();
            a_file_number.cmp(&b_file_number)
        }
        );
        Database{
            table_size,
            path: path,
            mem_table: MemoryTable::new(10000),
            ss_tables: ss_table_names
        }
    }
    fn insert(&mut self, key: i64, value: i64) {
        if self.mem_table.insert(key, value){
        } else {
            let new_stable = self.mem_table.create_stable(format!("{}/{}.sst", self.path, self.ss_tables.len()));
            self.ss_tables.push(new_stable);
            self.mem_table = MemoryTable::new(self.table_size as usize);
            self.mem_table.insert(key, value);
        }
    }

    fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)>{
        let mut scanned_tables = self.ss_tables.par_iter().map(
            |ss_table| {
                ss_table.scan(key1, key2)
            }).collect::<Vec<_>>();
        scanned_tables.push(Node::scan(&self.mem_table.root, key1, key2));
        scanned_tables.into_iter().kmerge_by(|a, b| a.0 < b.0).collect()
    }

    fn get(&self, key: i64) -> Option<i64>{
        let mut result = self.mem_table.get(key);
        if result.is_some() {
            return result;
        }
        self.ss_tables.par_iter().find_map_any(
            |ss_table| {
                ss_table.get_value(key)
            }
        )
    }
    fn close(&mut self){
        self.ss_tables.push(self.mem_table.create_stable(format!("{}.sst", self.ss_tables.len())));
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use super::*;

    #[test]
    fn test_insert() {
        let mut node = Node::new(1, 1);
        for i in 2..30{
            node = Node::insert(Some(node), i, i);
        }
        node.print_tree(0);
        println!("{:?}", Node::scan(&Some(node), 10, 20));
    }

    #[test]
    fn test_ss_table() {
        let mut mem_table = MemoryTable::new(10000);
        for i in 0..100000 {
            mem_table.insert(i, i);
        }
        let ss_table = mem_table.create_stable("test".to_string());
        println!("{:?}", ss_table.scan(1001, 99900));
        // println!("{:?}", ss_table.scan(10, 100));
    }

    #[test]
    fn test_database(){
        let mut db = Database::from_disk("Database".to_string(), 10000);
        // for i in (0..10000000).map(|x| x * 2) {
        //     db.insert(i, i);
        // }
        // for i in (0..5000000).map(|x| x * 2 + 1) {
        //     db.insert(i, i);
        // }
        dbg!("reached");
        println!("{:?}", db.scan(10010, 29900));
        println!("{:?}", db.get(1001));
        println!("{:?}", db.get(9999900));

        db.close();
    }
}

