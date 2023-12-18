use std::fs::File;
use serde::{Deserialize, Serialize};
use crate::avl_tree::MemoryTable;
use crate::b_tree::BTreeReader;
use crate::cache_trait::Cache;

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
}