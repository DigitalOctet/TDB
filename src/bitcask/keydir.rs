use std::collections::BTreeMap;

use super::{FileId, Key, SizeType};

pub(super) struct KeyDirEntry {
    pub(super) file_id: FileId,
    pub(super) value_sz: SizeType,
    pub(super) value_pos: SizeType,
}

impl KeyDirEntry {
    pub(super) fn new(file_id: FileId, value_sz: SizeType, value_pos: SizeType) -> Self {
        Self {
            file_id,
            value_sz,
            value_pos,
        }
    }
}

pub(super) struct KeyDir {
    keydir: BTreeMap<Key, KeyDirEntry>,
}

impl KeyDir {
    pub(super) fn new() -> Self {
        Self {
            keydir: BTreeMap::new(),
        }
    }

    pub(super) fn get(&self, key: &Key) -> Option<&KeyDirEntry> {
        self.keydir.get(key)
    }

    pub(super) fn put(&mut self, key: Key, entry: KeyDirEntry) -> Option<KeyDirEntry> {
        self.keydir.insert(key, entry)
    }

    pub(super) fn delete(&mut self, key: &Key) -> Option<KeyDirEntry> {
        self.keydir.remove(key)
    }

    pub(super) fn list_keys(&self) -> Vec<Key> {
        self.keydir.keys().cloned().collect()
    }
}
