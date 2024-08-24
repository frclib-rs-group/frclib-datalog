use thiserror::Error;

/// Errors that can occur when interacting with a `DataLog`
#[allow(missing_docs)]
#[derive(Error, Debug)]
pub enum DataLogError {
    #[error("DataLog io error: {0:?}")]
    Io(#[from] std::io::Error),
    #[error("DataLog Int Cast error: {0:?}")]
    IntCast(#[from] std::num::TryFromIntError),
    #[error("DataLog Utf8 error: {0:?}")]
    Utf8(#[from] std::string::FromUtf8Error),
    #[error("Record serialization error: {0:?}")]
    RecordSerialize(&'static str),
    #[error("Record deserialization error: {0:?}")]
    RecordDeserialize(&'static str),
    #[error("Record type error: {0:?}")]
    RecordType(&'static str),
    #[error("Record byte reader was short: {0}")]
    RecordReaderOutOfBounds(&'static str),
    #[error("Attempted to modify a read only data log")]
    DataLogReadOnly,
    #[error("DataLog entry does not exist")]
    NoSuchEntry,
    #[error("Outside entry lifetime")]
    OutsideEntryLifetime,
    #[error("DataLog entry already exists")]
    EntryAlreadyExists,
    #[error("DataLog entry type mismatch")]
    EntryTypeMismatch,
    #[error("Dile not a valid DataLog")]
    InvalidDataLog,
    #[error("File doesn't exist")]
    FileDoesNotExist,
    #[error("File already exists")]
    FileAlreadyExists,
    #[error("Retro entry data")]
    RetroEntryData,
    #[error("DataLog version mismatch")]
    VersionMismatch,
    #[error("DataLog magic mismatch")]
    MagicMismatch,
    #[error("Record too large")]
    RecordTooLarge,
    #[error("Metadata too large")]
    MetadataTooLarge,
}
