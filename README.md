# TDB - Toy Database
Welcome to TDB. This is a tiny but full-fledged database based on [Bitcask](https://en.wikipedia.org/wiki/Bitcask).

## Getting Started

### Examples

See `examples/example.rs` for a simple example. Simply run: 
```bash
cargo run --example example
```
You'll see output similar to this:
```bash
First  get: Some([4, 5, 6])
Second get: [[1, 2, 3], [7]]
Third  get: None
```

### API Descriptions

| API                                                          | Descriptions                                                     |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| pub fn open_with_opts<T: Into<PathBuf>>(*data_dir*: T, *opts*: Opts) -> Result<Self, DBError> | Open a new or existing Bitcask datastore with additional options. Valid options include read write (if this process is going to be a writer and not just a reader) and sync on put (if this writer would prefer to sync the write file after every write operation). |
| pub fn open<T: Into<PathBuf>>(*data_dir*: T) -> Result<Self, DBError> | Open a new or existing Bitcask datastore for read-only access.        |
| pub fn get(&self, *key*: &Key) -> Result<Option<Value>, DBError> | Retrieve a value by key from a Bitcask datastore.                                           |
| pub fn put(&mut self, *key*: &Key, *value*: &Value) -> Result<(), DBError> | Store a key and value in a Bitcask datastore.                                             |
| pub fn delete(&mut self, *key*: &Key) -> Result<(), DBError> | Delete a key from a Bitcask datastore.                                             |
| pub fn list_keys(&self) -> Vec<Key>                          | List all keys in a Bitcask datastore.                                       |
| pub fn fold<F: Fn(Key, Value, Acc) -> Acc, Acc>(&self, *fun*: F, *acc0*: Acc) -> Result<Acc, DBError> | Fold over all K/V pairs in a Bitcask datastore. Fun is expected to be of the form: F(K,V,Acc0) → Acc. |
| pub fn merge(&mut self) -> Result<(), DBError>               | Merge several data files within a Bitcask datastore into a more compact form. |
| pub fn sync(&mut self) -> Result<(), DBError>                | Force any writes to sync to disk.                       |
| pub fn close(&mut self) -> Result<(), DBError>               | Close a Bitcask data store and flush all pending writes (if any) to disk.                                 |
