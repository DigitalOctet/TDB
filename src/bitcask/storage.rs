use std::{fs, path::PathBuf};

use crate::error::DBError;

use super::{keydir::KeyDir, log::Log, Key, Value};

pub(super) struct Storage {
    log: Log,
    keydir: KeyDir,
}

impl Storage {
    pub(super) fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self, DBError> {
        let data_dir = data_dir.into();
        fs::create_dir_all(&data_dir)?;
        let mut keydir = KeyDir::new();
        let log = Log::from_disk(&data_dir, &mut keydir)?;

        Ok(Self { log, keydir })
    }

    pub(super) fn get(&self, key: &Key) -> Result<Option<Value>, DBError> {
        let keydir_entry = self.keydir.get(key);
        match keydir_entry {
            Some(entry) => {
                let value = self.log.get(&entry)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub(super) fn put(
        &mut self,
        key: &Key,
        value: &Value,
        sync_on_put: bool,
    ) -> Result<(), DBError> {
        let keydir_entry = self.log.put(key, value, sync_on_put)?;
        self.keydir.put(key.clone(), keydir_entry);

        Ok(())
    }

    pub(super) fn delete(&mut self, key: &Key, sync_on_put: bool) -> Result<(), DBError> {
        self.log.delete(key, sync_on_put)?;
        self.keydir.delete(key);

        Ok(())
    }

    pub(super) fn list_keys(&self) -> Vec<Key> {
        self.keydir.list_keys()
    }

    pub(super) fn fold<F, Acc>(&self, fun: F, acc0: Acc) -> Result<Acc, DBError>
    where
        F: Fn(Key, Value, Acc) -> Acc,
    {
        let mut acc = acc0;
        for k in self.keydir.list_keys() {
            let value = self.get(&k)?.unwrap();
            acc = fun(k, value, acc);
        }

        Ok(acc)
    }

    pub(super) fn merge(&mut self, sync_on_put: bool) -> Result<(), DBError> {
        for k in self.keydir.list_keys() {
            let value = self.get(&k)?.unwrap();
            self.put_on_merge(k, value, sync_on_put)?;
        }
        self.log.finish_merge()?;

        Ok(())
    }

    pub(super) fn sync(&mut self) -> Result<(), DBError> {
        self.log.sync()
    }

    fn put_on_merge(&mut self, key: Key, value: Value, sync_on_put: bool) -> Result<(), DBError> {
        let keydir_entry = self.log.put_on_merge(&key, value, sync_on_put)?;
        self.keydir.put(key, keydir_entry);
        Ok(())
    }
}
