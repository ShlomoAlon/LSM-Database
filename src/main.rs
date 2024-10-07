#![allow(dead_code)]
#![feature(test)]
#![feature(get_mut_unchecked)]
#![feature(const_mut_refs)]
#![feature(const_slice_from_raw_parts_mut)]
#[allow(dead_code)]
#[macro_use]
extern crate static_assertions;

// mod memtable;

// use crate::memtable::Database;
extern crate rand;

// pub mod memtablev2;
pub mod avl_tree;
pub mod b_tree;
pub mod bloom_filter;
pub mod buffer;
pub mod cache;
pub mod cache_trait;
pub mod compaction;
pub mod database;
pub mod write_and_read;

fn main() {}
