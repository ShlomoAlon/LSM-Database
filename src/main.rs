mod memtable;
use crate::memtable::Database;
mod api;

fn main() {

    let mut db = Database::new("Database".to_string(), 100000);
    for i in (0..10000000).map(|x| x * 2) {
        db.insert(i, i);
    }
    dbg!("reached");
    for i in (0..5000000).map(|x| x * 2 + 1) {
        db.insert(i, i);
    }
    dbg!("reached2");
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
