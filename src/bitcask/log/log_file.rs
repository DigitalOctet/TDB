use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::{
    bitcask::{
        keydir::{KeyDir, KeyDirEntry},
        FileId, SizeType,
    },
    error::DBError,
};

use super::log_entry::{Deserialize, LogEntry, Serialize};

#[derive(Debug)]
pub(super) struct LogFile {
    file_id: FileId,
    path: PathBuf,
    file: File,
}

impl LogFile {
    pub(super) const EXTENSION: &'static str = "tdb";
    pub(super) const MERGE_EXTENSION: &'static str = "merge";
    pub(super) const MAX_FILE_SIZE: SizeType = 1_000_000; // 1MB

    pub(super) fn new<T: Into<PathBuf>>(
        data_dir: T,
        file_id: FileId,
        extension: &'static str,
    ) -> Result<Self, DBError> {
        let mut path: PathBuf = data_dir.into();
        path.push(file_id.to_string());
        path.set_extension(extension);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            file_id,
            path,
            file,
        })
    }

    pub(super) fn open(
        file_id: FileId,
        path: PathBuf,
        keydir: &mut KeyDir,
    ) -> Result<Self, DBError> {
        let file = fs::OpenOptions::new().read(true).append(true).open(&path)?;
        let file = Self {
            file_id,
            path,
            file,
        };
        file.populate_keydir(keydir)?;

        Ok(file)
    }

    pub(super) fn append_entry(
        &mut self,
        entry: LogEntry,
        sync_on_put: bool,
    ) -> Result<SizeType, DBError> {
        let value_pos = self.file.seek(SeekFrom::End(0))? + entry.get_value_offset();
        entry.serialize(&mut self.file)?;
        if sync_on_put {
            self.file.flush()?;
        }

        Ok(value_pos)
    }

    pub(super) fn change_extension(&mut self) -> Result<(), DBError> {
        let old_path = self.path.clone();
        self.path.set_extension(Self::EXTENSION);
        fs::rename(old_path, self.path.clone())?;
        Ok(())
    }

    #[inline]
    pub(super) fn get_file_id(&self) -> FileId {
        self.file_id
    }

    #[inline]
    pub(super) fn get_file(&self) -> &File {
        &self.file
    }

    #[inline]
    pub(super) fn get_file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    fn populate_keydir(&self, keydir: &mut KeyDir) -> Result<(), DBError> {
        let file_sz = self.file.metadata()?.len();
        let mut buf_reader = BufReader::new(&self.file);
        let mut cursor = 0_u64;
        buf_reader.seek(SeekFrom::Start(cursor))?;
        loop {
            if cursor >= file_sz {
                break;
            }
            let log_entry = LogEntry::deserialize(&mut buf_reader)?;
            let log_entry_size = log_entry.total_size();
            if log_entry.is_tombstone() {
                keydir.delete(log_entry.get_key_ref());
            } else {
                let keydir_entry = KeyDirEntry::new(
                    self.file_id,
                    log_entry.value_size(),
                    cursor + log_entry.get_value_offset(),
                );
                keydir.put(log_entry.get_key(), keydir_entry);
            }
            cursor += log_entry_size;
        }

        Ok(())
    }
}
