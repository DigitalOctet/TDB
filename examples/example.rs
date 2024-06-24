use rand::{self, Rng};

use tdb::{Opts, TDB};

fn main() {
    let mut tdb = generate_random_db_instance();

    tdb.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
    let res = tdb.get(&vec![1, 2, 3]).unwrap();
    assert_eq!(res, Some(vec![4, 5, 6]));
    println!("First  get: {:?}", res);

    tdb.put(&vec![7], &vec![8]).unwrap();
    let keys = tdb.list_keys();
    tdb.sync().unwrap();
    println!("Second get: {:?}", keys);

    tdb.delete(&vec![7]).unwrap();
    tdb.merge().unwrap();
    let res = tdb.get(&vec![7]).unwrap();
    assert_eq!(res, None);
    println!("Third  get: {:?}", res);
}

fn generate_random_db_instance() -> TDB {
    let file_name = generate_random_name();
    let data_dir = format!("./data/{}", file_name);
    let opts = Opts::new(true, true);
    TDB::open_with_opts(data_dir, opts).unwrap()
}

fn generate_random_name() -> String {
    let rng = rand::thread_rng();
    let rand_string: String = rng
        .sample_iter(rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    rand_string
}
