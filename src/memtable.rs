use arrayvec::ArrayVec;
use futures::io::AsyncWriteExt;
use glommio::io::{DmaStreamWriter, ImmutableFile, ImmutableFileBuilder, ReadResult};
use itertools::Itertools;
use positioned_io::RandomAccessFile;
use positioned_io::ReadAt;
use rayon::prelude::*;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::fmt::format;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use futures::executor::block_on;
// use futures_lite::io::AsyncWriteExt;
use glommio::{
    io::{DmaFile, DmaStreamWriterBuilder},
    LocalExecutor,
};

const PAGE_SIZE: usize = 1024 * 4;
const VECTOR_SIZE: usize = PAGE_SIZE / 16;
type Child = Option<Box<Node>>;

struct Node {
    key: i64,
    value: i64,
    height: i64,
    left: Child,
    right: Child,
}

// struct Node_Iterator{
//     node: Child,
//     left_iterator: Op
// }
//
// impl Iterator for Node_Iterator{
//     type Item = (i64, i64);
//
//     fn next(&mut self) -> Option<Self::Item> {
//
//     }
// }

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
    fn write_all(&self, mut buffer: &mut BufWriter<File>) {
        if let Some(left) = &self.left {
            left.write_all(buffer);
        }
        buffer
            .write_all(&self.key.to_ne_bytes())
            .expect("did not write");
        buffer
            .write_all(&self.value.to_ne_bytes())
            .expect("did not write");
        if let Some(right) = &self.right {
            right.write_all(buffer);
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

    fn to_vec_2(&self) -> Vec<i64> {
        let tuples = Node::scan(&self.root, i64::MIN, i64::MAX);
        tuples
            .iter()
            .flat_map(|(key, value)| vec![*key, *value])
            .collect()
    }

    fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)> {
        Node::scan(&self.root, key1, key2)
    }

    fn get(&self, key: i64) -> Option<i64> {
        self.root.as_ref().and_then(|node| node.get(key))
    }

    fn create_stable(&self, file_name: String) -> SSTable {
        // println!("started writing");

        // let bytes = self.to_vec();
        // let bytes = self.scan(i64::MIN, i64::MAX).into_iter().flat_map(|(key, value)| {
        //     let mut result = [0; 16];
        //     result[..8].copy_from_slice(&key.to_ne_bytes());
        //     result[8..].copy_from_slice(&value.to_ne_bytes());
        //     result
        // }).collect::<Vec<_>>();
        let file_size = self.cur_size * 16;
        // let mut file = std::fs::File::create(&file_name).unwrap();
        // let mut file = BufWriter::new(file);
        match &self.root {
            None => {}
            Some(i) => {
                let mut bytes = Vec::with_capacity(file_size * 16);
                i.get_all(&mut bytes);
                let executor = LocalExecutor::default();
                executor.run(async {
                    let mut file = DmaFile::create(&file_name).await.unwrap();
                    let mut writer = DmaStreamWriterBuilder::new(file).build();
                    writer.write_all(&bytes).await.unwrap();
                    writer.sync().await.unwrap();
                    writer.close().await.unwrap();
                });
            }
        }
        // for (key, value) in items {
        //     file.write_all(&key.to_ne_bytes()).expect("did not write");
        //     file.write_all(&value.to_ne_bytes()).expect("did not write");
        // }
        // println!("ended writing");
        SSTable {
            file_name: file_name,
            file_size: file_size,
        }
    }
}

#[derive(Debug)]
struct SSTable {
    file_name: String,
    file_size: usize,
}

impl SSTable {
    fn get_all(&self) -> Vec<(i64, i64)> {
        let file = RandomAccessFile::open(&self.file_name).unwrap();
        let pages_to_search: Vec<_> = (0..(self.file_size + (PAGE_SIZE - 1)) / PAGE_SIZE).collect();
        let mut result = Vec::new();
        for page_num in pages_to_search {
            let page = self.get_page(page_num as u64, &file);
            result.extend(page);
        }
        result
    }
    fn get_page_2(&self, key: u64, file: &DmaFile) -> ArrayVec<(i64, i64), VECTOR_SIZE> {
        let executor = LocalExecutor::default();
        let mut vec = ArrayVec::new();
        let s = file.read_at_aligned(key * PAGE_SIZE as u64, PAGE_SIZE);
        let s = executor.run(async{

            println!("reading");
            let file = file
                .read_at(key * PAGE_SIZE as u64, PAGE_SIZE)
                .await
                .unwrap();
            println!("read");
            file
        });

        for i in (0..s.len()).step_by(16) {
            let key = i64::from_ne_bytes(s[i..i + 8].try_into().unwrap());
            let value = i64::from_ne_bytes(s[i + 8..i + 16].try_into().unwrap());
            vec.push((key, value));
        }
        vec
    }
    fn get_page(&self, key: u64, file: &RandomAccessFile) -> ArrayVec<(i64, i64), VECTOR_SIZE> {
        let mut bytes: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let bytes_read = file.read_at(key * PAGE_SIZE as u64, &mut bytes).unwrap();
        let mut vec = ArrayVec::new();
        for i in (0..bytes_read).step_by(16) {
            let key = i64::from_ne_bytes(bytes[i..i + 8].try_into().unwrap());
            let value = i64::from_ne_bytes(bytes[i + 8..i + 16].try_into().unwrap());
            vec.push((key, value));
        }
        vec
    }
    fn get_value(&self, key: i64) -> Option<i64> {
        let file = RandomAccessFile::open(&self.file_name).unwrap();
        let pages_to_search: Vec<_> = (0..(self.file_size + (PAGE_SIZE - 1)) / PAGE_SIZE).collect();
        let mut result = None;
        _ = pages_to_search.binary_search_by(|&page_num| {
            println!("getting page");
            let page = self.get_page(page_num as u64, &file);
            println!("got page");
            let first_key = page.first().unwrap().0;
            let last_key = page.last().unwrap().0;

            if key > last_key {
                return Less;
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
    fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)> {
        let executor = LocalExecutor::default();
        let file = executor.run(async {
            DmaFile::open(&self.file_name).await.unwrap()
        });
        let pages_to_search: Vec<_> = (0..(self.file_size + (PAGE_SIZE - 1)) / PAGE_SIZE).collect();
        let mut first_page = ArrayVec::new();
        let mut cur_page_index = pages_to_search.len();
        _ = pages_to_search.binary_search_by(|&page_num| {
            println!("getting page");
            let page = self.get_page_2(page_num as u64, &file);
            println!("got page");
            let first_key = page.first().unwrap().0;
            let last_key = page.last().unwrap().0;

            if key1 > last_key {
                return Less;
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
        let mut most_recent_page: ArrayVec<(i64, i64), VECTOR_SIZE> = first_page
            [first_page_starting_element_index..]
            .try_into()
            .unwrap();
        let mut result = Vec::new();
        loop {
            // if most_recent_page.last().is_none() {
            //     dbg!(first_page.clone());
            //     dbg!(self.clone());
            // }
            if most_recent_page.last().unwrap().0 > key2 {
                let page_partition_point =
                    most_recent_page.partition_point(|&(key, _)| key <= key2);
                result.extend(most_recent_page[..page_partition_point].to_vec());
                break;
            }
            result.extend(most_recent_page);
            cur_page_index += 1;
            if cur_page_index == pages_to_search.len() {
                break;
            }
            most_recent_page = self.get_page_2(pages_to_search[cur_page_index] as u64, &file);
        }
        result
    }
}

pub struct Database {
    table_size: u64,
    path: String,
    mem_table: MemoryTable,
    ss_tables: Vec<SSTable>,
}

impl Database {
    pub fn new(path: String, table_size: u64) -> Self {
        let path = Path::new(&path);
        if path.exists() {
            std::fs::remove_dir_all(path).unwrap();
        }
        std::fs::create_dir(path).unwrap();
        Self::from_disk(path.to_str().unwrap().to_string(), table_size)
    }
    pub fn from_disk(path: String, table_size: u64) -> Self {
        let items = std::fs::read_dir(path.clone()).unwrap();
        let mut ss_table_names = Vec::new();
        for item in items {
            let item = item.unwrap();
            let file_name = item.file_name().into_string().unwrap();
            if file_name.ends_with(".sst") {
                ss_table_names.push(SSTable {
                    file_name: file_name,
                    file_size: item.metadata().unwrap().len() as usize,
                });
            }
        }
        ss_table_names.sort_by(|a, b| {
            let a_file_number = a
                .file_name
                .split(".")
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            let b_file_number = b
                .file_name
                .split(".")
                .next()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            a_file_number.cmp(&b_file_number)
        });
        Database {
            table_size,
            path: path.clone(),
            mem_table: MemoryTable::new(10000),
            ss_tables: ss_table_names
                .into_iter()
                .map(|name| SSTable {
                    file_name: format!("{}/{}", path.clone(), name.file_name),
                    file_size: name.file_size,
                })
                .collect(),
        }
    }
    pub fn insert(&mut self, key: i64, value: i64) {
        if self.mem_table.insert(key, value) {
        } else {
            let new_stable =
                self.mem_table
                    .create_stable(format!("{}/{}.sst", self.path, self.ss_tables.len()));
            self.ss_tables.push(new_stable);
            self.mem_table = MemoryTable::new(self.table_size as usize);
            self.mem_table.insert(key, value);
        }
    }

    pub fn scan(&self, key1: i64, key2: i64) -> Vec<(i64, i64)> {
        let mut scanned_tables = self
            .ss_tables
            .iter()
            .map(|ss_table| ss_table.scan(key1, key2))
            .collect::<Vec<_>>();
        let value = Node::scan(&self.mem_table.root, key1, key2);
        scanned_tables.push(value);
        scanned_tables
            .into_iter()
            .kmerge_by(|a, b| a.0 < b.0)
            .collect()
    }

    pub fn get(&self, key: i64) -> Option<i64> {
        let mut result = self.mem_table.get(key);
        if result.is_some() {
            return result;
        }
        self.ss_tables
            .par_iter()
            .find_map_any(|ss_table| ss_table.get_value(key))
    }
    pub fn close(&mut self) {
        self.ss_tables.push(self.mem_table.create_stable(format!(
            "{}/{}.sst",
            self.path,
            self.ss_tables.len()
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_insert() {
        let mut node = Node::new(1, 1);
        for i in -10000..10000 {
            node = Node::insert(Some(node), i, i);
        }
        // dbg!(Node::scan(&Some(node), -10, 10000));
        let mut writer = BufWriter::new(File::create("test.txt").unwrap());
        node.write_all(&mut writer);
        let ss_table = SSTable {
            file_name: "test.txt".to_string(),
            file_size: 10000 * 16 * 2,
        };
        // dbg!(ss_table.get_all());
        dbg!(ss_table.scan(-10, 10000));
    }

    #[test]
    fn test_ss_table() {
        let mut mem_table = MemoryTable::new(10000);
        for i in 0..100000 {
            mem_table.insert(i, i);
        }
        let ss_table = mem_table.create_stable("test".to_string());
        println!("{:?}", ss_table.scan(0, 99900));
        // println!("{:?}", ss_table.scan(10, 100));
    }

    #[test]
    fn test_database() {
        let mut db = Database::new("Database".to_string(), 10000);
        for i in (0..10000000).map(|x| x * 2) {
            db.insert(i, i);
        }
        for i in (0..5000000).map(|x| x * 2 + 1) {
            db.insert(i, i);
        }
        dbg!("reached");
        println!("{:?}", db.scan(10010, 29900));
        println!("{:?}", db.get(1001));
        println!("{:?}", db.get(9999900));

        db.close();
    }
}
