use std::{collections::HashMap, fmt::Debug, io::Read, mem::swap, path::PathBuf};

use crate::{proto::{entries::EntryLifeStatus, records::{parse_records, ControlRecord, Record}}, DataLogError, EntryId, EntryTypeMap, TimestampedValue};
use byteorder::ReadBytesExt;
use frclib_core::{structure::FrcStructureBytes, value::{FrcTimestamp, FrcTimestampedValue, FrcValue, IntoFrcValue}};

#[derive(Debug, Clone)]
struct EntryData {
    values: Vec<FrcTimestampedValue>,
    metadata: Vec<TimestampedValue<String>>,
    type_str: Vec<TimestampedValue<String>>
}

/// Configuration for the [`DataLogReader`]
#[derive(Debug, Clone, Copy)]
pub struct DataLogReaderConfig {
    /// Require the magic bytes at the start of the file to be `WPILOG`
    pub require_magic: bool,
    /// Require a specific version of the file format
    pub required_version: Option<(u8, u8)>,
}
impl Default for DataLogReaderConfig {
    fn default() -> Self {
        Self {
            require_magic: true,
            required_version: Some((1, 0))
        }
    }
}

type StringPredicate = Box<dyn Fn(&str) -> bool>;

/// A reader that can filter entries based on certain criteria
pub struct EntryFilterReader<'a> {
    data: &'a EntryData,
    before: Option<u64>,
    after: Option<u64>,
    required_metadata_predicate: Option<StringPredicate>,
    required_type_predicate: Option<StringPredicate>
}
impl Debug for EntryFilterReader<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntryFilterReader")
            .field("before", &self.before)
            .field("after", &self.after)
            .field("required_metadata_predicate", &self.required_metadata_predicate.is_some())
            .field("required_type_predicate", &self.required_type_predicate.is_some())
            .finish()
    }
}

/// An all in one reader for a datalog file
#[derive(Debug)]
pub struct DataLogReader {
    path: PathBuf,
    format_version: (u8, u8),
    header_metadata: String,
    config: DataLogReaderConfig,
    keys: HashMap<String, u32>,
    data: HashMap<u32, EntryData>
}

impl DataLogReader {
    /// Will create a new reader that reads the file at the given path
    /// 
    /// # Example
    /// ```rust
    /// use std::path::PathBuf;
    /// use frclib_datalog::reader::DataLogReader;
    /// use frclib_core::value::FrcValue;
    /// 
    /// let path = PathBuf::from("path/to/file");
    /// let reader = DataLogReader::try_new(path, Default::default())
    ///         .expect("Failed to create reader");
    /// 
    /// reader.read_entry("entry_name").into_iter().for_each(|value| {
    ///     match value.value {
    ///         FrcValue::Int(i) => println!("Int: {}", i),
    ///         _ => println!("Not an int")
    ///     }
    /// });
    /// 
    /// ```
    /// 
    /// # Errors
    /// - [`DataLogError::MagicMismatch`] if the magic bytes at the start of the file do not match `WPILOG` and [`DataLogReaderConfig::require_magic`] is `true`
    /// - [`DataLogError::VersionMismatch`] if the version of the file does not match the required version in [`DataLogReaderConfig::required_version`]
    /// - [`DataLogError::Io`] if there is an error reading the file
    /// - [`DataLogError::Utf8`] if there is an error reading the metadata or any string entries in the file
    /// - [`DataLogError::IntCast`] if there is an error reading records
    /// - [`DataLogError::RecordDeserialize`] if there is an error reading records
    /// - [`DataLogError::RecordType`] if there is an error reading records
    /// - [`DataLogError::RecordReaderOutOfBounds`] if there is an error reading records
    pub fn try_new(path: PathBuf, config: DataLogReaderConfig) -> Result<Self, DataLogError> {
        let file = std::fs::File::open(&path)?;
        let mut reader = Self {
            path,
            format_version: (0, 0),
            header_metadata: String::new(),
            config,
            keys: HashMap::new(),
            data: HashMap::new()
        };
        reader.read(file)?;
        reader.sort_data();
        Ok(reader)
    }

    #[allow(clippy::map_entry)]
    fn get_entry_data(&mut self, id: EntryId) -> Result<&mut EntryData, DataLogError> {
        if !self.data.contains_key(&id) {
            let _ = self.data.insert(id, EntryData {
                values: Vec::new(),
                metadata: Vec::new(),
                type_str: Vec::new()
            });
        }
        self.data.get_mut(&id).ok_or(DataLogError::NoSuchEntry)
    }

    #[allow(unused_results)]
    fn read(&mut self, mut file: std::fs::File) -> Result<(), DataLogError> {

        // Validate Magic
        let mut magic = [0u8; 6];
        file.read_exact(&mut magic)?;
        if  self.config.require_magic && magic != *b"WPILOG" {
            return Err(DataLogError::MagicMismatch);
        }

        // Validate Version
        let mut version = [0u8; 2];
        file.read_exact(&mut version)?;
        self.format_version = (version[1], version[0]);
        if let Some(version) = self.config.required_version {
            if version != self.format_version{
                return Err(DataLogError::VersionMismatch);
            }
        }

        // Read Metadata
        let metadata_len = file.read_u32::<byteorder::LittleEndian>()?;
        let mut metadata = vec![0u8; metadata_len as usize];
        file.read_exact(&mut metadata)?;
        self.header_metadata = String::from_utf8(metadata)
            .unwrap_or_default();

        // Read Records
        let mut file_buffer = Vec::new();
        file.read_to_end(&mut file_buffer)?;
        let mut entry_types: EntryTypeMap = HashMap::new();
        let mut entry_status: HashMap<EntryId, EntryLifeStatus> = HashMap::new();
        let all_records = parse_records(&file_buffer, &mut entry_types)?;
        for record in all_records {
            match record {
                Record::Control(inner, timestamp, id) => {
                    match inner {
                        ControlRecord::Start(name, type_str, metadata) => {
                            if let Some(EntryLifeStatus::Alive { .. }) = entry_status.get(&id) {
                                // Got a start record for an already started entry
                                continue;
                            }
                            entry_status.insert(id, EntryLifeStatus::Alive { start: timestamp });
                            entry_types.insert(id, type_str.clone());
                            self.keys.insert(name, id);
                            let data = self.get_entry_data(id)?;
                            data.type_str.push(TimestampedValue::new(timestamp, type_str));
                            data.metadata.push(TimestampedValue::new(timestamp, metadata));
                        }
                        ControlRecord::Finish => {
                            if let Some(status) = entry_status.get_mut(&id) {
                                if let EntryLifeStatus::Alive { start } = status {
                                    *status = EntryLifeStatus::Dead { start: *start, end: timestamp };
                                }
                            }
                        }
                        ControlRecord::Metadata(metadata) => {
                            if let Some(EntryLifeStatus::Alive { .. }) = entry_status.get(&id) {
                                self.get_entry_data(id)?.metadata.push(TimestampedValue::new(timestamp, metadata));
                            }
                        }
                    }
                },
                Record::Data(value, timestamp, id) => {
                    if let Some(EntryLifeStatus::Alive { .. }) = entry_status.get(&id) {
                        let type_str = entry_types.get(&id)
                            .ok_or(DataLogError::NoSuchEntry)?;
                        if value.matches_type(type_str) {
                            continue;
                        }

                        let value = FrcTimestampedValue::new(timestamp, value.into_frc_value());
                        self.get_entry_data(id)?.values.push(value);
                    }
                }
            }
        }
        Ok(())
    }

    fn sort_data(&mut self) {
        for data in self.data.values_mut() {
            data.values.sort_by_key(|value| value.timestamp);
            data.metadata.sort_by_key(|timestamped_value| timestamped_value.timestamp);
            data.type_str.sort_by_key(|timestamped_value| timestamped_value.timestamp);
        }
    }

    /// Returns the path of the file being read
    #[must_use]
    pub const fn get_path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns the format version of the file being read
    #[must_use]
    pub const fn get_format_version(&self) -> (u8, u8) {
        self.format_version
    }

    /// Returns the header metadata of the file being read
    #[must_use]
    pub fn get_header_metadata(&self) -> &str {
        &self.header_metadata
    }

    /// Returns the values from the entry with the given key,
    /// if no entry with the given key exists an empty `Vec` is returned
    #[must_use]
    pub fn read_entry(&self, entry_key: &str) -> Vec<&FrcTimestampedValue> {
        if let Some(id) = self.keys.get(entry_key) {
            if let Some(data) = self.data.get(id) {
                return data.values.iter().collect();
            }
        }
        Vec::new()
    }

    /// Returns the values from the entry with the given key that are after the given timestamp,
    /// if no entry with the given key exists an empty `Vec` is returned
    #[must_use]
    pub fn read_entry_after(&self, entry_key: &str, timestamp: u64) -> Vec<&FrcTimestampedValue> {
        if let Some(id) = self.keys.get(entry_key) {
            if let Some(data) = self.data.get(id) {
                return data.values.iter()
                    .filter(|value| value.timestamp > timestamp)
                    .collect();
            }
        }
        Vec::new()
    }

    /// Returns the values from the entry with the given key that are before the given timestamp,
    /// if no entry with the given key exists an empty `Vec` is returned
    #[must_use]
    pub fn read_entry_before(&self, entry_key: &str, timestamp: u64) -> Vec<&FrcTimestampedValue> {
        if let Some(id) = self.keys.get(entry_key) {
            if let Some(data) = self.data.get(id) {
                return data.values.iter()
                    .filter(|value| value.timestamp < timestamp)
                    .collect();
            }
        }
        Vec::new()
    }

    /// Returns the values from the entry with the given key that are between the given timestamps,
    /// if no entry with the given key exists an empty `Vec` is returned
    #[must_use]
    pub fn read_entry_between(&self, entry_key: &str, start: u64, end: u64) -> Vec<&FrcTimestampedValue> {
        if let Some(id) = self.keys.get(entry_key) {
            if let Some(data) = self.data.get(id) {
                return data.values.iter()
                    .filter(|value| value.timestamp >= start && value.timestamp <= end)
                    .collect();
            }
        }
        Vec::new()
    }

    /// Returns the values from the entry with the given key that are between the given timestamps,
    /// if no entry with the given key exists an empty `Vec` is returned
    #[must_use]
    pub fn read_entry_metadata(&self, entry_key: &str) -> Vec<&TimestampedValue<String>> {
        if let Some(id) = self.keys.get(entry_key) {
            if let Some(data) = self.data.get(id) {
                return data.metadata.iter().collect();
            }
        }
        Vec::new()
    }

    /// Returns the values from the entry with the given key that are between the given timestamps,
    /// if no entry with the given key exists an empty `Vec` is returned
    #[must_use]
    pub fn read_entry_type_str(&self, entry_key: &str) -> Vec<&TimestampedValue<String>> {
        if let Some(id) = self.keys.get(entry_key) {
            if let Some(data) = self.data.get(id) {
                return data.type_str.iter().collect();
            }
        }
        Vec::new()
    }

    /// Get all the keys for the entries in the `DataLog`
    /// 
    /// # Memory
    /// This function will allocate a new `Vec` on each call,
    /// if this is frequently accessed consider caching the result.
    /// 
    /// # Returns
    /// A `Vec` of all keys in the `DataLog`
    #[must_use]
    pub fn get_all_entry_keys(&self) -> Vec<&String> {
        self.keys.keys().collect()
    }

    /// Creates a filter for the entry with the given key
    #[must_use]
    pub fn create_entry_filter<'log>(&'log self, entry_key: &str) -> Option<EntryFilterReader<'log>>  {
        Some(EntryFilterReader::new(self.data.get(self.keys.get(entry_key)?)?))
    }

    /// Converts any [`FrcValue::Raw`] entries that have a type string
    /// matching something in the [`frclib_core::structure::FrcStructDescDB`]
    /// into [`FrcValue::Struct`] or [`FrcValue::StructArray`]
    /// 
    /// This can be an expensive call which is why its no implicitly run on read
    pub fn structify_all_data(&mut self) {
        fn update_type_str_for_timestamp(
            timestamp: FrcTimestamp,
            type_history: &[TimestampedValue<String>],
            type_str: &mut String,
            expiration_timestamp: &mut FrcTimestamp
        ) {
            if timestamp >= *expiration_timestamp {
                for value in type_history.iter().rev() {
                    if value.timestamp <= timestamp {
                        type_str.clone_from(&value.value);
                        break;
                    }
                    *expiration_timestamp = value.timestamp;
                }
            }
        }

        for data in self.data.values_mut() {
            let type_history = data.type_str.clone();
            let mut type_str = String::new();
            let mut expiration_timestamp = 0u64;
            for value in &mut data.values {
                if let FrcValue::Raw(raw_bytes) = &mut value.value {
                    update_type_str_for_timestamp(value.timestamp, &type_history, &mut type_str, &mut expiration_timestamp);
                    if let Some(struct_desc) = frclib_core::structure::FrcStructDescDB::get(&type_str) {
                        let mut new_struct_inner = FrcStructureBytes {
                            desc: struct_desc,
                            count: 1,
                            data: Box::default()
                        };
                        swap(raw_bytes, &mut new_struct_inner.data);
                        let mut new_struct = FrcValue::Struct(
                            Box::new(
                                new_struct_inner
                            )
                        );
                        swap(&mut value.value, &mut new_struct);
                    }
                }
            }
        }
    }
}

impl <'a> EntryFilterReader<'a> {
    fn new(data: &'a EntryData) -> Self {
        EntryFilterReader {
            data,
            before: None,
            after: None,
            required_metadata_predicate: None,
            required_type_predicate: None
        }
    }

    /// Filters the values to only include those before the given timestamp
    /// 
    /// This method is chainable and mutates the original filter
    pub fn before(&mut self, timestamp: u64) -> &mut Self {
        self.before = Some(timestamp);
        self
    }

    /// Filters the values to only include those after the given timestamp
    /// 
    /// This method is chainable and mutates the original filter
    pub fn after(&mut self, timestamp: u64) -> &mut Self {
        self.after = Some(timestamp);
        self
    }

    /// Filters the values to only include those that comply with the predicate
    /// 
    /// This method is chainable and mutates the original filter
    pub fn required_metadata_predicate(&mut self, predicate: Box<dyn Fn(&str) -> bool>) -> &mut Self {
        self.required_metadata_predicate = Some(predicate);
        self
    }


    /// Filters the values to only include those that comply with the predicate
    /// 
    /// This method is chainable and mutates the original filter
    pub fn required_type_predicate(&mut self, predicate: Box<dyn Fn(&str) -> bool>) -> &mut Self {
        self.required_type_predicate = Some(predicate);
        self
    }

    /// Filters the values to only include those that are of the given type
    /// 
    /// This method is chainable and mutates the original filter
    pub fn required_type(&mut self, type_str: String) -> &mut Self {
        self.required_type_predicate = Some(Box::new(move |value_type| value_type == type_str));
        self
    }

    fn get_metadata_at_timestamp(&self, timestamp: u64) -> Option<&str> {
        self.data.metadata.iter()
            .rev()
            .find(|value| value.timestamp <= timestamp)
            .map(|value| value.value.as_str())
    }

    fn get_type_at_timestamp(&self, timestamp: u64) -> Option<&str> {
        self.data.type_str.iter()
            .rev()
            .find(|value| value.timestamp <= timestamp)
            .map(|value| value.value.as_str())
    }

    /// Collects all the values that match the filter criteria
    #[must_use]
    pub fn collect(&self) -> Vec<&FrcTimestampedValue> {
        self.data.values.iter()
            .filter(|value| {
                if let Some(before) = self.before {
                    if value.timestamp > before {
                        return false;
                    }
                }
                if let Some(after) = self.after {
                    if value.timestamp < after {
                        return false;
                    }
                }
                if let Some(predicate) = &self.required_metadata_predicate {
                    if let Some(metadata) = self.get_metadata_at_timestamp(value.timestamp) {
                        if !predicate(metadata) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                if let Some(predicate) = &self.required_type_predicate {
                    if let Some(value_type) = self.get_type_at_timestamp(value.timestamp) {
                        if !predicate(value_type) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            })
            .collect()
    }
}