use std::ops::Deref;

use crate::error::DataLogError;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt {
    // This is overkill but due to byte packing a usize isn't bigger than a u8
    size: u8,
    value: u64,
}

impl Deref for UInt {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl UInt {
    #[inline]
    pub const fn new(size: u8, value: u64) -> Self {
        Self { size, value }.shrink()
    }

    const fn new_unchecked(size: u8, value: u64) -> Self {
        Self { size, value }
    }

    #[inline]
    pub const fn shrink(&self) -> Self {
        let universal_int = self.value;
        if universal_int <= 0xFF {
            Self::new_unchecked(1, universal_int)
        } else if universal_int <= 0xFFFF {
            Self::new_unchecked(2, universal_int)
        } else if universal_int <= 0x00FF_FFFF {
            Self::new_unchecked(3, universal_int)
        } else if universal_int <= 0xFFFF_FFFF {
            Self::new_unchecked(4, universal_int)
        } else if universal_int <= 0x00FF_FFFF_FFFF {
            Self::new_unchecked(5, universal_int)
        } else if universal_int <= 0xFFFF_FFFF_FFFF {
            Self::new_unchecked(6, universal_int)
        } else if universal_int <= 0x00FF_FFFF_FFFF_FFFF {
            Self::new_unchecked(7, universal_int)
        } else {
            Self::new_unchecked(8, universal_int)
        }
    }

    pub const fn get_byte_count(&self) -> u8 {
        self.size
    }

    #[inline]
    #[allow(trivial_casts)]
    pub const fn as_binary(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                std::ptr::from_ref::<u64>(&self.value).cast::<u8>(),
                self.size as usize
            )
        }
    }

    #[inline]
    pub fn from_binary(le_bytes: &[u8]) -> Option<Self> {
        let size = le_bytes.len();
        if let Ok(size_u8) = u8::try_from(size) {
            let mut bytes = [0u8; 8];
            bytes[..size].copy_from_slice(le_bytes);
            Some(Self::new(size_u8, u64::from_le_bytes(bytes)))
        } else {
            None
        }
    }
}

impl From<u8> for UInt {
    #[inline]
    fn from(int: u8) -> Self {
        Self::new(1, int.into()).shrink()
    }
}

impl From<u16> for UInt {
    #[inline]
    fn from(int: u16) -> Self {
        Self::new(2, int.into()).shrink()
    }
}

impl From<u32> for UInt {
    #[inline]
    fn from(int: u32) -> Self {
        Self::new(4, int.into()).shrink()
    }
}

impl From<u64> for UInt {
    #[inline]
    fn from(int: u64) -> Self {
        Self::new(8, int).shrink()
    }
}

#[derive(Debug)]
pub struct RecordByteReader<'buf> {
    bytes: &'buf [u8]
}
impl<'buf> RecordByteReader<'buf> {
    #[inline]
    pub const fn new(bytes: &'buf [u8]) -> Self {
        Self { bytes }
    }

    pub fn byte(&mut self) -> Result<u8, DataLogError> {
        if self.bytes.is_empty() {
            return Err(DataLogError::RecordReaderOutOfBounds("u8"));
        }
        let mut bytes = [0u8; 1];
        let (int_bytes, rest) = self.bytes.split_at(1);
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(u8::from_le_bytes(bytes))
    }

    pub const fn inspect_byte(&self) -> Result<u8, DataLogError> {
        if self.bytes.is_empty() {
            return Err(DataLogError::RecordReaderOutOfBounds("u8"));
        }
        Ok(self.bytes[0])
    }

    pub fn bytes(&mut self, len: usize) -> Result<&'buf [u8], DataLogError> {
        if self.bytes.len() < len {
            return Err(DataLogError::RecordReaderOutOfBounds("bytes"));
        }
        if self.bytes.len() == len {
            return Ok(std::mem::take(&mut self.bytes));
        }
        let (int_bytes, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        Ok(int_bytes)
    }

    pub fn string(&mut self, len: usize) -> Result<&'buf str, DataLogError> {
        if self.bytes.len() < len {
            return Err(DataLogError::RecordReaderOutOfBounds("string"));
        }
        let (int_bytes, rest) = self.bytes.split_at(len);
        self.bytes = rest;
        std::str::from_utf8(int_bytes).map_err(|_| DataLogError::RecordReaderOutOfBounds("string"))
    }

    pub fn inspect_bytes(&self, len: usize) -> Result<&'buf [u8], DataLogError> {
        if self.bytes.len() < len {
            return Err(DataLogError::RecordReaderOutOfBounds("bytes"));
        }
        Ok(&self.bytes[..len])
    }

    #[allow(unused)]
    pub fn i32(&mut self) -> Result<i32, DataLogError> {
        if self.bytes.len() < 4 {
            return Err(DataLogError::RecordReaderOutOfBounds("i32"));
        }
        let mut bytes = [0u8; 4];
        let (int_bytes, rest) = self.bytes.split_at(bytes.len());
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(i32::from_le_bytes(bytes))
    }

    pub fn i64(&mut self) -> Result<i64, DataLogError> {
        if self.bytes.len() < 8 {
            return Err(DataLogError::RecordReaderOutOfBounds("i64"));
        }
        let mut bytes = [0u8; 8];
        let (int_bytes, rest) = self.bytes.split_at(bytes.len());
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(i64::from_le_bytes(bytes))
    }

    pub fn u32(&mut self) -> Result<u32, DataLogError> {
        if self.bytes.len() < 4 {
            return Err(DataLogError::RecordReaderOutOfBounds("u32"));
        }
        let mut bytes = [0u8; 4];
        let (int_bytes, rest) = self.bytes.split_at(bytes.len());
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn bool(&mut self) -> Result<bool, DataLogError> {
        if self.bytes.is_empty() {
            return Err(DataLogError::RecordReaderOutOfBounds("bool"));
        }
        let mut bytes = [0u8; 1];
        let (int_bytes, rest) = self.bytes.split_at(1);
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(u8::from_le_bytes(bytes) != 0)
    }

    pub fn f32(&mut self) -> Result<f32, DataLogError> {
        if self.bytes.len() < 4 {
            return Err(DataLogError::RecordReaderOutOfBounds("f32"));
        }
        let mut bytes = [0u8; 4];
        let (int_bytes, rest) = self.bytes.split_at(4);
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(f32::from_le_bytes(bytes))
    }

    pub fn f64(&mut self) -> Result<f64, DataLogError> {
        if self.bytes.len() < 8 {
            return Err(DataLogError::RecordReaderOutOfBounds("f64"));
        }
        let mut bytes = [0u8; 8];
        let (int_bytes, rest) = self.bytes.split_at(8);
        bytes.copy_from_slice(int_bytes);
        self.bytes = rest;
        Ok(f64::from_le_bytes(bytes))
    }

    pub fn uint(&mut self, size: usize) -> Result<UInt, DataLogError> {
        if self.bytes.len() < size {
            return Err(DataLogError::RecordReaderOutOfBounds("uint"));
        }
        let (int_bytes, rest) = self.bytes.split_at(size);
        self.bytes = rest;
        UInt::from_binary(int_bytes).ok_or(DataLogError::RecordReaderOutOfBounds("uint"))
    }

    #[inline]
    pub fn skip(&mut self, len: usize) -> Result<(), DataLogError> {
        if self.bytes.len() < len {
            return Err(DataLogError::RecordReaderOutOfBounds("skip"));
        }
        self.bytes = &self.bytes[len..];
        Ok(())
    }

    #[inline]
    pub const fn the_rest(self) -> &'buf [u8] {
        self.bytes
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[inline]
    pub const fn bytes_left(&self) -> usize {
        self.bytes.len()
    }
}
