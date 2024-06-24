//! Options to tdb.

/// Options give when opening a database by calling `Bitcask::open_with_opts`.
pub struct Opts {
    /// whether writable or not
    read_write: bool,
    /// whether to sync on put
    sync_on_put: bool,
}

impl Opts {
    #[inline]
    pub fn new(read_write: bool, sync_on_put: bool) -> Opts {
        Opts {
            read_write,
            sync_on_put,
        }
    }

    #[inline]
    pub fn read_write(&mut self, read_write: bool) {
        self.read_write = read_write;
    }

    #[inline]
    pub fn sync_on_put(&mut self, sync_on_put: bool) {
        self.sync_on_put = sync_on_put;
    }

    #[inline]
    pub(crate) fn is_mutable(&self) -> bool {
        self.read_write
    }

    #[inline]
    pub(crate) fn do_sync_on_put(&self) -> bool {
        self.sync_on_put
    }
}
