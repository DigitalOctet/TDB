use std::{
    cell::Cell,
    ffi::OsStr,
    fs,
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    vec,
};

use log_entry::LogEntry;

use crate::error::DBError;

use self::log_file::LogFile;
use super::{
    keydir::{KeyDir, KeyDirEntry},
    FileId, Key, SizeType, Value,
};

mod log_entry;
mod log_file;

pub(super) struct Log {
    files: Vec<LogFile>,
    data_dir: PathBuf,
    cur_file_sz: SizeType,
    merged_files: Cell<Vec<LogFile>>,
    cur_merged_file_sz: SizeType,
}

impl Log {
    pub(super) fn from_disk<T: Into<PathBuf>>(
        data_dir: T,
        keydir: &mut KeyDir,
    ) -> Result<Self, DBError> {
        let data_dir = data_dir.into();

        let files = fs::read_dir(&data_dir)?
            .filter_map(|path| {
                path.ok().map(|path| path.path()).filter(|path| {
                    path.is_file() && path.extension() == Some(OsStr::new(LogFile::EXTENSION))
                })
            })
            .collect();
        let mut files = Self::to_log_files(files, keydir)?;

        let next_file_id = if files.len() == 0 {
            0
        } else {
            files.last().unwrap().get_file_id() + 1
        };
        let cur_file = LogFile::new(data_dir.clone(), next_file_id, LogFile::EXTENSION)?;
        files.push(cur_file);

        Ok(Self {
            files,
            data_dir,
            cur_file_sz: 0,
            merged_files: Cell::new(vec![]),
            cur_merged_file_sz: 0,
        })
    }

    pub(super) fn get(&self, keydir_entry: &KeyDirEntry) -> Result<Value, DBError> {
        let KeyDirEntry {
            file_id,
            value_sz,
            value_pos,
        } = keydir_entry;
        let log_file = self.get_file(*file_id);
        let mut buf_reader = BufReader::with_capacity(*value_sz as usize, log_file.get_file());
        buf_reader.seek(SeekFrom::Start(*value_pos))?;
        let mut buf = vec![0; *value_sz as usize];
        buf_reader.read_exact(&mut buf)?;

        Ok(buf)
    }

    pub(super) fn put(
        &mut self,
        key: &Key,
        value: &Value,
        sync_on_put: bool,
    ) -> Result<KeyDirEntry, DBError> {
        self.append(
            LogEntry::new_live_entry(key.clone(), value.clone()),
            sync_on_put,
        )
    }

    pub(super) fn delete(&mut self, key: &Key, sync_on_put: bool) -> Result<KeyDirEntry, DBError> {
        self.append(LogEntry::new_tombstone_entry(key.clone()), sync_on_put)
    }

    pub(super) fn put_on_merge(
        &mut self,
        key: &Key,
        value: Value,
        sync_on_put: bool,
    ) -> Result<KeyDirEntry, DBError> {
        let entry = LogEntry::new_live_entry(key.clone(), value);
        let entry_sz = entry.total_size();
        if self.cur_merged_file_sz + entry_sz > LogFile::MAX_FILE_SIZE
            || self.cur_merged_file_sz == 0
        {
            self.create_new_merge_file()?;
        }
        self.cur_merged_file_sz += entry_sz;
        let log_file = self.get_current_merged_file();
        let value_pos = log_file.append_entry(entry.clone(), sync_on_put)?;

        Ok(KeyDirEntry::new(
            log_file.get_file_id(),
            entry.value_size(),
            value_pos,
        ))
    }

    pub(super) fn finish_merge(&mut self) -> Result<(), DBError> {
        self.cur_file_sz = self.cur_merged_file_sz;
        self.cur_merged_file_sz = 0;
        self.files = self.merged_files.take();
        self.merged_files = Cell::new(vec![]);

        for log_file in &mut self.files {
            log_file.change_extension()?;
        }
        Ok(())
    }

    pub(super) fn sync(&mut self) -> Result<(), DBError> {
        for file in &mut self.files {
            file.get_file_mut().flush()?;
        }
        Ok(())
    }

    fn to_log_files(files: Vec<PathBuf>, keydir: &mut KeyDir) -> Result<Vec<LogFile>, DBError> {
        let mut files = files
            .into_iter()
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|file_stem| file_stem.to_str())
                    .and_then(|file_stem| file_stem.parse::<FileId>().ok())
                    .map(|file_id| (file_id, path))
            })
            .map(|(file_id, path)| LogFile::open(file_id, path, keydir).map(|file| (file_id, file)))
            .collect::<Result<Vec<(FileId, LogFile)>, DBError>>()?;

        files.sort_by_key(|(file_id, _)| *file_id);
        Ok(files.into_iter().map(|(_, file)| file).collect())
    }

    fn get_file(&self, file_id: FileId) -> &LogFile {
        self.files.get(file_id as usize).unwrap()
    }

    fn get_current_file(&mut self) -> &mut LogFile {
        self.files.last_mut().unwrap()
    }

    fn get_current_merged_file(&mut self) -> &mut LogFile {
        self.merged_files.get_mut().last_mut().unwrap()
    }

    fn append(&mut self, entry: LogEntry, sync_on_put: bool) -> Result<KeyDirEntry, DBError> {
        let entry_sz = entry.total_size();
        if self.cur_file_sz + entry_sz > LogFile::MAX_FILE_SIZE {
            self.create_new_file()?;
        }
        self.cur_file_sz += entry_sz;
        let log_file = self.get_current_file();
        let value_pos = log_file.append_entry(entry.clone(), sync_on_put)?;

        Ok(KeyDirEntry::new(
            log_file.get_file_id(),
            entry.value_size(),
            value_pos,
        ))
    }

    fn create_new_file(&mut self) -> Result<(), DBError> {
        let next_file_id = self.files.last().unwrap().get_file_id() + 1;
        let log_file = LogFile::new(&self.data_dir, next_file_id, LogFile::EXTENSION)?;
        self.files.push(log_file);
        self.cur_file_sz = 0;

        Ok(())
    }

    fn create_new_merge_file(&mut self) -> Result<(), DBError> {
        let next_file_id = if let Some(file) = self.merged_files.get_mut().last() {
            file.get_file_id() + 1
        } else {
            0
        };
        let log_file = LogFile::new(&self.data_dir, next_file_id, LogFile::MERGE_EXTENSION)?;
        self.merged_files.get_mut().push(log_file);
        self.cur_merged_file_sz = 0;

        Ok(())
    }
}
