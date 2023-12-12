mod memtable;

// use crate::memtable::Database;
use std::time::Instant;
use rand::seq::SliceRandom;

extern crate rand;


mod api;
mod memtablev2;
mod avl_tree;
mod write_and_read;
mod cache;
mod b_tree;
mod buffered_reader;
mod buffer;
mod read_write;

fn main() {

    // let mut range = (-100000..100000).collect::<Vec<i64>>();
    // let mut rng = rand::thread_rng();
    // range.shuffle(&mut rng);
    // // dbg!(range.clone());
    // println!("done\n");
    // let start = std::time::Instant::now();
    // let mut db = Database::new("Database".to_string(), 100000);
    // for i in range{
    //     db.insert(i, i);
    // }
    // // for i in (0..10000000).map(|x| x * 2) {
    // //     db.insert(i, i);
    // // }
    // // for i in (0..5000000).map(|x| x * 2 + 1) {
    // //     db.insert(i, i);
    // // }
    // dbg!("reached\n");
    // let first = std::time::Instant::now();
    // // println!("insertion time: {:?}\n", first.duration_since(start));
    // for _i in 0..10000{
    //     db.scan(0, 1000);
    // }
    // let one = db.scan(99990, 100000);
    // let _two = db.scan(-100000, 1000);
    // let _three = db.scan(-100000, 1000);
    // let _four = db.scan(-1000, 100000);
    // let _five = db.scan(-100000, 1000);
    // // let one = db.scan(0, 1000000000);
    // // let one = db.scan(0, 1000000000);
    // // let one = db.scan(0, 1000000000);
    // // db.scan(10010, 29900);
    // // db.scan(10010, 29900);
    // // db.scan(10010, 29900);
    // // db.get(1001);
    // // db.get(9999900);
    // let second = Instant::now();
    // // println!("{:?}", one);
    // // println!("{:?}", two);
    // // println!("{:?}", three);
    // println!("insertion: {:?}", first.duration_since(start));
    // println!("search time: {:?}\n", second.duration_since(first));
    // // println!("{:?}", db.get(1001));
    // // println!("{:?}", db.get(9999900));
    // println!("{:?}", one);
    // // println!("{:?}", two);
    // // println!("{:?}", three);
    // // println!("{:?}", four);
    // // println!("{:?}", five);
    //
    // db.close();
}
