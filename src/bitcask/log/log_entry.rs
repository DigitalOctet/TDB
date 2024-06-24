use std::io::{Read, Write};

use crc::{Crc, CRC_32_CKSUM};

use crate::{
    bitcask::{Key, SizeType, Value},
    error::DBError,
};

/// Any object that is writable can be serialized to
pub(super) trait Serialize {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<(), DBError>;
}

/// Any object that is readable can be deserialized
pub(super) trait Deserialize {
    fn deserialize<T: Read>(buf: &mut T) -> Result<Self, DBError>
    where
        Self: Sized;
}

#[derive(Clone)]
pub(super) struct LogEntry {
    checksum: u32,
    key: Key,
    value: Option<Value>,
}

impl LogEntry {
    const CHECKSUM_SIZE: SizeType = 4;
    const SIZE_SIZE: SizeType = SizeType::BITS as SizeType / 8;
    const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);

    pub(super) fn new_live_entry(key: Key, value: Value) -> Self {
        let mut entry = LogEntry {
            checksum: 0,
            key,
            value: Some(value),
        };
        entry.checksum = entry.calculate_checksum();

        entry
    }

    pub(super) fn new_tombstone_entry(key: Key) -> LogEntry {
        let mut entry = LogEntry {
            checksum: 0,
            key,
            value: None,
        };
        entry.checksum = entry.calculate_checksum();

        entry
    }

    #[inline]
    fn key_size(&self) -> SizeType {
        self.key.len() as u64
    }

    #[inline]
    pub(super) fn value_size(&self) -> SizeType {
        self.value.as_ref().map(|v| v.len() as u64).unwrap_or(0)
    }

    #[inline]
    pub(super) fn total_size(&self) -> SizeType {
        Self::CHECKSUM_SIZE + Self::SIZE_SIZE * 2 + self.key_size() + self.value_size()
    }

    #[inline]
    pub(super) fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    #[inline]
    pub(super) fn get_key_ref(&self) -> &Key {
        &self.key
    }

    #[inline]
    pub(super) fn get_key(self) -> Key {
        self.key
    }

    #[inline]
    pub(super) fn get_value_offset(&self) -> SizeType {
        Self::CHECKSUM_SIZE + Self::SIZE_SIZE * 2 + self.key_size()
    }

    fn calculate_checksum(&self) -> u32 {
        let mut digest = Self::CRC32.digest();
        digest.update(&self.key_size().to_be_bytes());
        digest.update(&self.value_size().to_be_bytes());
        digest.update(&self.key);
        if let Some(value) = &self.value {
            digest.update(value);
        }
        digest.finalize()
    }

    fn is_valid(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

impl Serialize for LogEntry {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<(), DBError> {
        let Self {
            checksum,
            key,
            value,
        } = self;
        buf.write_all(&checksum.to_be_bytes())?;
        buf.write_all(&self.key_size().to_be_bytes())?;
        buf.write_all(&self.value_size().to_be_bytes())?;
        buf.write_all(&key)?;
        if let Some(value) = value {
            buf.write_all(value)?;
        }

        Ok(())
    }
}

impl Deserialize for LogEntry {
    fn deserialize<T: Read>(buf: &mut T) -> Result<Self, DBError>
    where
        Self: Sized,
    {
        let mut checksum_buf = [0_u8; Self::CHECKSUM_SIZE as usize];
        buf.read_exact(&mut checksum_buf)?;
        let checksum = u32::from_be_bytes(checksum_buf);
        let mut size_buf = [0_u8; Self::SIZE_SIZE as usize];
        buf.read_exact(&mut size_buf)?;
        let key_size = SizeType::from_be_bytes(size_buf);
        buf.read_exact(&mut size_buf)?;
        let value_size = SizeType::from_be_bytes(size_buf);
        let mut key_buf = vec![0_u8; key_size as usize];
        buf.read_exact(&mut key_buf)?;
        let value = if value_size > 0 {
            let mut value_buf = vec![0_u8; value_size as usize];
            buf.read_exact(&mut value_buf)?;
            Some(value_buf)
        } else {
            None
        };

        let entry = Self {
            checksum,
            key: key_buf,
            value,
        };
        if entry.is_valid() {
            Ok(entry)
        } else {
            Err(DBError::DataError("invalid checksum".to_string()))
        }
    }
}
