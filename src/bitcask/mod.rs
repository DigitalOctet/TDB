//! A tiny but full-fledged database engine based on bitcask.

use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};

use super::error::DBError;
pub(crate) use opts::Opts;
use storage::Storage;

mod keydir;
mod log;
pub mod opts;
mod storage;

type FileId = usize;
type SizeType = u64;
type Key = Vec<u8>;
type Value = Vec<u8>;

/// Type that manages the database. It encapsulates [`Storage`] which is the 
/// underlying type of the database. This type is thread-safe by using a
/// [`RwLock`].
pub struct BitCask {
    /// `Storage` is the underlying type of the database.
    storage: Arc<RwLock<Storage>>,
    /// whether mutable or not.
    mutable: bool,
    /// whether to sync on put.
    sync_on_put: bool,
}

impl BitCask {
    pub fn open_with_opts<T: Into<PathBuf>>(data_dir: T, opts: Opts) -> Result<Self, DBError> {
        let s = Storage::new(data_dir)?;

        Ok(Self {
            storage: Arc::new(RwLock::new(s)),
            mutable: opts.is_mutable(),
            sync_on_put: opts.do_sync_on_put(),
        })
    }

    pub fn open<T: Into<PathBuf>>(data_dir: T) -> Result<Self, DBError> {
        let s = Storage::new(data_dir)?;

        Ok(Self {
            storage: Arc::new(RwLock::new(s)),
            mutable: false,
            sync_on_put: false,
        })
    }

    pub fn get(&self, key: &Key) -> Result<Option<Value>, DBError> {
        self.storage.read().unwrap().get(key)
    }

    pub fn put(&mut self, key: &Key, value: &Value) -> Result<(), DBError> {
        if self.mutable {
            self.storage
                .write()
                .unwrap()
                .put(key, value, self.sync_on_put)
        } else {
            Err(DBError::OptionError(
                "tried to write in read-only access".to_string(),
            ))
        }
    }

    pub fn delete(&mut self, key: &Key) -> Result<(), DBError> {
        if self.mutable {
            self.storage.write().unwrap().delete(key, self.sync_on_put)
        } else {
            Err(DBError::OptionError(
                "tried to delete in read-only access".to_string(),
            ))
        }
    }

    pub fn list_keys(&self) -> Vec<Key> {
        self.storage.read().unwrap().list_keys()
    }

    pub fn fold<F, Acc>(&self, fun: F, acc0: Acc) -> Result<Acc, DBError>
    where
        F: Fn(Key, Value, Acc) -> Acc,
    {
        self.storage.read().unwrap().fold(fun, acc0)
    }

    pub fn merge(&mut self) -> Result<(), DBError> {
        self.storage.write().unwrap().merge(self.sync_on_put)
    }

    pub fn sync(&mut self) -> Result<(), DBError> {
        self.storage.write().unwrap().sync()
    }

    pub fn close(&mut self) -> Result<(), DBError> {
        self.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{opts::Opts, BitCask};
    use rand::{self, Rng};

    #[test]
    fn basics_test() {
        let mut tdb = generate_random_bitcask_instance();

        tdb.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
        let res = tdb.get(&vec![1, 2, 3]).unwrap();
        assert_eq!(res, Some(vec![4, 5, 6]));

        tdb.put(&vec![7], &vec![8]).unwrap();
        let _keys = tdb.list_keys();
        tdb.sync().unwrap();

        tdb.delete(&vec![7]).unwrap();
        tdb.merge().unwrap();
        let res = tdb.get(&vec![7]).unwrap();
        assert_eq!(res, None);
    }

    fn generate_random_bitcask_instance() -> BitCask {
        let file_name = generate_random_name();
        let data_dir = format!("./data/{}", file_name);
        let opts = Opts::new(true, true);
        BitCask::open_with_opts(data_dir, opts).unwrap()
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
}
