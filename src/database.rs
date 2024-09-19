use std::fs;
use std::fs::File;
use std::ops::Index;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use crate::avl_tree::MemoryTable;
use crate::b_tree::{BTreeReader, BTreeWriter};
use crate::cache_trait::Cache;
use crate::compaction::{LevelIterator, ScanIterator};

#[derive(Debug, Serialize, Deserialize)]
struct DatabaseMetadata{
    mem_table_file_name: Option<String>,
    b_trees_file_names_and_levels: Vec<Option<(String, usize)>>,
    max_mem_table_size: u64,
}

struct Database<A: Cache>{
    mem_table: MemoryTable,
    /// second parameter is the number of levels
    b_trees: Vec<Option<BTreeReader>>,
    max_mem_table_size: usize,
    path: String,
    cache: A,
}

impl<A: Cache> Database<A>{
    fn create(path: String, max_mem_table_size: usize) -> Self{
        let mem_table = MemoryTable::new(max_mem_table_size as usize);
        let b_trees = Vec::new();
        Database{
            mem_table,
            b_trees,
            max_mem_table_size,
            path,
            cache: A::default(),
        }
    }
    fn open(path: String) -> Self{
        let file = File::open(path.clone() + "/metadata.json").unwrap();
        let metadata: DatabaseMetadata = serde_json::from_reader(file).unwrap();
        let mut b_trees = Vec::new();
        for i in metadata.b_trees_file_names_and_levels{
            if let Some((file_name, level)) = i {
                b_trees.push(Some(BTreeReader::new(file_name, level)));
            } else {
                b_trees.push(None);
            }
        }
        let mem_table = MemoryTable::new(metadata.max_mem_table_size as usize);
        Database{
            mem_table,
            b_trees,
            max_mem_table_size: metadata.max_mem_table_size as usize,
            path,
            cache: A::default(),
        }
    }

    fn get(&mut self, key: i64) -> Option<i64>{
        if let Some(value) = self.mem_table.get(key){
            return Some(value);
        }
        for b_tree in self.b_trees.iter_mut(){
            if let Some(b_tree) = b_tree{
                if let Some(value) = b_tree.get_item(key, &mut self.cache){
                    return Some(value);
                }
            }
        }
        None
    }


    fn insert_iter_at_level(&mut self, level: usize, mut iter: Vec<LevelIterator>) -> usize{
        if level >= self.b_trees.len(){
            debug_assert!(level == self.b_trees.len());
            self.b_trees.push(None);
        }
        if let Some(b_tree) = self.b_trees[level].take(){
            iter.push(b_tree.into_level_iter());
            self.insert_iter_at_level(level + 1, iter)
        } else {
            let file_name = self.path.clone() + "/b_tree_" + level.to_string().as_str();
            let mut b_tree_writer = BTreeWriter::new(file_name.clone());
            let mut iter = iter.into_iter().kmerge().dedup_by(|item1, item2| item1.0 == item2.0);
            for item in iter{
                b_tree_writer.add_item(item, &mut self.cache);
            }
            let btree_level = b_tree_writer.finish(&mut self.cache);
            self.b_trees[level] = Some(BTreeReader::new(file_name, btree_level));
            level
        }
    }
    fn insert(&mut self, key: i64, value: i64){
        if !self.mem_table.insert(key, value){
            dbg!(key);
            dbg!(value);
            let old_meme_table = std::mem::replace(&mut self.mem_table, MemoryTable::new(self.max_mem_table_size));
            let last_level = self.insert_iter_at_level(0, vec![old_meme_table.into_level_iter()]);
            for file in fs::read_dir(self.path.clone()).unwrap(){
                let file = file.unwrap();
                if get_level_number(file.file_name().to_str().unwrap()) < last_level{
                    fs::remove_file(file.path()).unwrap();
                }
            }
            self.insert(key, value);
        }
    }

    fn range(&mut self, lower_bound: i64, upper_bound: i64) -> impl Iterator<Item = (i64, i64)> + use<'_, A>{
        let mut iterators = Vec::new();
        for b_tree in self.b_trees.iter_mut(){
            if let Some(b_tree) = b_tree{
                iterators.push(b_tree.range(lower_bound, upper_bound, &mut self.cache));
            }
        }
        iterators.push(ScanIterator::Memtable(self.mem_table.scan(lower_bound, upper_bound)));
        iterators.into_iter().kmerge().dedup_by(|item1, item2| item1.0 == item2.0)
    }

    // fn range(&mut self, lower_bound: i64, upper_bound: i64) -> impl Iterator<Item = (i64, i64)>{
    //     let mut iterators = Vec::new();
    //     for b_tree in self.b_trees.iter_mut(){
    //         if let Some(b_tree) = b_tree{
    //             iterators.push(b_tree.scan(lower_bound, upper_bound));
    //         }
    //     }
    //     iterators.into_iter().flatten()
    // }

    // fn insert(&mut self, key: i64, value: i64){
    //     if self.mem_table.cur_size < self.max_mem_table_size{
    //         self.mem_table.insert(key, value);
    //     } else {
    //         let mut b_tree = BTreeReader::new(self.path.clone() + "/b_tree_" + self.b_trees.len().to_string().as_str(), 0);
    //         for (k, v) in self.mem_table.iter(){
    //             b_tree.insert(k, v);
    //         }
    //         self.b_trees.push(Some(b_tree));
    //         self.mem_table = MemoryTable::new(self.max_mem_table_size);
    //         self.mem_table.insert(key, value);
    //     }
    // }
}


fn get_level_number(file_name: &str) -> usize{
    let mut level = 0;
    match file_name.strip_prefix("b_tree_") {
        None => usize::MAX,
        Some(name) => {
            let first_non_digit = name.chars().position(|c| !c.is_digit(10)).unwrap_or(name.len());
            name[..first_non_digit].parse().unwrap_or(usize::MAX)
        }
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    use std::fs;
    use crate::cache_trait::NoCache;

    #[test]
    fn level_number(){
        assert_eq!(get_level_number("b_tree_10_hello_world.world"), 10);
    }

    #[test]
    fn test_database_small() {
        let path = "test_database".to_string();
        fs::remove_dir_all(path.clone());
        fs::create_dir_all(path.clone()).unwrap();
        let mut database: Database<NoCache> = Database::create(path.clone(), 1000);
        for i in 0..2000{
            database.insert(i, i);
        }
        println!("{:?}", database.b_trees);
        for i in 0..2000{
            assert_eq!(database.get(i), Some(i));
        }
    }
    #[test]
    fn test_database_larger() {
        let path = "test_database".to_string();
        fs::remove_dir_all(path.clone());
        fs::create_dir_all(path.clone()).unwrap();
        let mut database: Database<NoCache> = Database::create(path.clone(), 1000);
        for i in 0..8000{
            database.insert(i, i);
        }
        println!("{:?}", database.b_trees);
        for i in 0..8000{
            assert_eq!(database.get(i), Some(i));
        }
    }
    #[test]
    fn test_database_larger2() {
        let path = "test_database".to_string();
        fs::remove_dir_all(path.clone());
        fs::create_dir_all(path.clone()).unwrap();
        let mut database: Database<NoCache> = Database::create(path.clone(), 1000);
        for i in 0..8001{
            database.insert(i, i);
        }
        println!("{:?}", database.b_trees);
        for i in 0..8001{
            assert_eq!(database.get(i), Some(i));
        }
    }
}