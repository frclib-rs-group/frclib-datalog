use std::{collections::HashMap, io::Write, num::NonZeroU32, sync::atomic::{AtomicU32, Ordering}};

use byteorder::WriteBytesExt;
use frclib_core::value::{FrcTimestamp, FrcTimestampedValue, FrcType, FrcValue, IntoFrcValue, StaticallyFrcTyped};

use crate::{now, proto::{entries::{get_data_type, get_data_type_serial, EntryLifeStatus}, records::{ControlRecord, DataRecord}}, DataLogError};

static DATALOG_INCREMENTER: AtomicU32 = AtomicU32::new(1);

const WPILOG_MAGIC: [u8; 6] = *b"WPILOG";
const WPILOG_VERSION: [u8; 2] = [0, 1];

/// A unique identifier for a data entry in a specific datalog
#[derive(Debug, Clone, Copy)]
pub struct EntryId {
    datalog_id: u32,
    entry_id: u32
}

/// A unique identifier for a data entry in a specific datalog
#[derive(Debug, Clone, Copy)]
pub struct TypedEntryId<T: IntoFrcValue> {
    datalog_id: u32,
    entry_id: u32,
    _phantom: std::marker::PhantomData<T>
}

impl <T: IntoFrcValue> From<TypedEntryId<T>> for EntryId {
    fn from(value: TypedEntryId<T>) -> Self {
        Self::new(value.datalog_id, value.entry_id)
    }
}

impl <T: IntoFrcValue> TypedEntryId<T> {
    const fn new(datalog_id: u32, entry_id: u32) -> Self {
        Self {
            datalog_id,
            entry_id,
            _phantom: std::marker::PhantomData
        }
    }
}

impl EntryId {
    const fn new(datalog_id: u32, entry_id: u32) -> Self {
        Self {
            datalog_id,
            entry_id
        }
    }

    const fn typed<T: IntoFrcValue>(self) -> TypedEntryId<T> {
        TypedEntryId::new(self.datalog_id, self.entry_id)
    }
}

#[derive(Debug)]
struct EntryData {
    key: String,
    entry_type: String,
    prehashed_type: NonZeroU32,
    lifestatus: EntryLifeStatus,
    packing_buffer: Vec<u8>
}

/// A datalog writer
#[derive(Debug)]
pub struct DataLogWriter {
    /// The writer
    writer: std::io::BufWriter<std::fs::File>,
    /// The entry type map
    entry_data: Vec<EntryData>,
    /// The map of keys to entry ids
    entry_id_map: HashMap<String, u32>,
    /// Highest entry id
    highest_entry_id: u32,
    /// The datalog id
    datalog_id: u32
}

impl DataLogWriter {
    /// Creates a new datalog writer
    /// 
    /// # Errors
    ///  - [`DataLogError::MetadataTooLarge`] if the metadata is too large
    ///  - [`DataLogError::Io`] if an IO error occurs
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(file: std::fs::File, metadata: impl ToString) -> Result<Self, DataLogError> {
        let mut w = Self {
            writer: std::io::BufWriter::new(file),
            entry_data: Vec::new(),
            entry_id_map: HashMap::new(),
            highest_entry_id: 0,
            datalog_id: DATALOG_INCREMENTER.fetch_add(1, Ordering::SeqCst)
        };

        let metadata = metadata.to_string();

        w.writer.write_all(&WPILOG_MAGIC)?;
        w.writer.write_all(&WPILOG_VERSION)?;
        if let Ok(len) = metadata.len().try_into() {
            w.writer.write_u32::<byteorder::LittleEndian>(len)?;
            w.writer.write_all(metadata.as_bytes())?;
        } else {
            return Err(DataLogError::MetadataTooLarge);
        }


        Ok(w)
    }

    fn get_entry_data(&self, id: u32) -> Result<&EntryData, DataLogError> {
        self.entry_data.get(id as usize).ok_or(DataLogError::NoSuchEntry)
    }

    fn get_entry_data_mut(&mut self, id: u32) -> Result<&mut EntryData, DataLogError> {
        self.entry_data.get_mut(id as usize).ok_or(DataLogError::NoSuchEntry)
    }

    fn inner_write(&mut self, id: EntryId, tv: FrcTimestampedValue, check_type: bool) -> Result<(), DataLogError> {
        if tv.value == FrcValue::Void {
            return Ok(());
        }

        if id.datalog_id != self.datalog_id {
            return Err(DataLogError::InvalidDataLog);
        }

        let data = self.get_entry_data(id.entry_id)?;

        if let EntryLifeStatus::Dead { .. } = data.lifestatus {
            return Err(DataLogError::OutsideEntryLifetime);
        }

        if check_type && data.prehashed_type != get_data_type_serial(&tv.value.get_type()) {
            return Err(DataLogError::EntryTypeMismatch);
        }

        let timestamp = tv.timestamp;
        let data_record = DataRecord::from(tv.value);

        data_record.write_to(timestamp, id.entry_id, &mut self.writer)
    }

    /// Writes a value to the datalog.
    /// 
    /// If the value is casted to [`FrcValue::Void`], like what can happen with [`Option::None`], this function will no-op.
    /// 
    /// # Errors
    /// - [`DataLogError::OutsideEntryLifetime`] if the entry is closed
    /// - [`DataLogError::InvalidDataLog`] if the entry is not in this datalog
    /// - [`DataLogError::Io`] if an IO error occurs
    #[inline]
    pub fn write<T: IntoFrcValue>(&mut self, id: TypedEntryId<T>, value: T) -> Result<(), DataLogError> {
        self.inner_write(id.into(), value.into_frc_value().to_timestamped(now()), false)
    }

    /// Writes a value to the datalog with a specific timestamp.
    /// 
    /// If the value is casted to [`FrcValue::Void`], like what can happen with [`Option::None`], this function will no-op.
    /// 
    /// # Errors
    /// - [`DataLogError::OutsideEntryLifetime`] if the entry is closed
    /// - [`DataLogError::InvalidDataLog`] if the entry is not in this datalog
    /// - [`DataLogError::Io`] if an IO error occurs
    #[inline]
    pub fn write_timestamped<T: IntoFrcValue>(&mut self, id: TypedEntryId<T>, value: T, timestamp: FrcTimestamp) -> Result<(), DataLogError> {
        self.inner_write(id.into(), value.into_frc_value().to_timestamped(timestamp), false)
    }

    /// Writes a value to the datalog.
    /// 
    /// If the value is [`FrcValue::Void`],  this function will no-op.
    /// 
    /// # Errors
    /// - [`DataLogError::OutsideEntryLifetime`] if the entry is closed
    /// - [`DataLogError::InvalidDataLog`] if the entry is not in this datalog
    /// - [`DataLogError::Io`] if an IO error occurs
    /// - [`DataLogError::EntryTypeMismatch`] if the value type doesn't match the entry type
    pub fn write_dynamic(&mut self, id: EntryId, value: FrcTimestampedValue) -> Result<(), DataLogError> {
        self.inner_write(id, value, true)
    }

    /// Gets the entry id for a key, creating it if it doesn't exist
    /// 
    /// # Errors
    /// - [`DataLogError::RecordType`] if the entry type is [`FrcType::Void`]
    /// - [`DataLogError::EntryTypeMismatch`] if the entry type doesn't match the existing entry type
    /// - [`DataLogError::OutsideEntryLifetime`] if the entry is closed
    /// - [`DataLogError::Io`] if an IO error occurs
    /// - [`DataLogError::MetadataTooLarge`] if the metadata is too large
    #[allow(unused_results, clippy::needless_pass_by_value)]
    #[inline(never)]
    pub fn get_entry_dynamic(&mut self, key: impl ToString, entry_type: FrcType, metadata: Option<String>) -> Result<EntryId, DataLogError> {
        if FrcType::Void == entry_type {
            return Err(
                DataLogError::RecordType(
                    "Cannot create a void entry".to_string()
                )
            )
        }

        let key = key.to_string();

        if let Some(id) = self.entry_id_map.get(&key) {
            let data = self.get_entry_data(*id)?;
            if data.prehashed_type != get_data_type_serial(&entry_type) {
                return Err(DataLogError::EntryTypeMismatch);
            }
            if let EntryLifeStatus::Dead{ .. } = data.lifestatus {
                return Err(DataLogError::OutsideEntryLifetime);
            }
            return Ok(EntryId {
                datalog_id: self.datalog_id,
                entry_id: *id
            })
        }

        let id = self.highest_entry_id;
        self.entry_id_map.insert(key.clone(), id);
        self.entry_data.push(EntryData {
            key: key.clone(),
            entry_type: entry_type.to_string(),
            prehashed_type: get_data_type_serial(&entry_type),
            lifestatus: EntryLifeStatus::Alive{ start: crate::now() },
            packing_buffer: match entry_type {
                FrcType::Struct(desc) | FrcType::StructArray(desc) => {
                    Vec::with_capacity(desc.size)
                },
                _ => Vec::new()
            }
        });

        self.highest_entry_id += 1;

        let metadata = if let Some(metadata) = metadata {
            if metadata.len() > u32::MAX as usize {
                return Err(DataLogError::MetadataTooLarge);
            }
            metadata
        } else {
            String::new()
        };

        let control_record = ControlRecord::Start(
            key,
            get_data_type(&entry_type)
                .ok_or_else( ||
                    DataLogError::RecordType(
                        "Cannot create a void entry".to_string()
                    )
                )?
                .to_string(),
            metadata
        );

        control_record.write_to(crate::now(), id, &mut self.writer)?;

        Ok(EntryId {
            datalog_id: self.datalog_id,
            entry_id: id
        })
    }

    /// Gets the entry id for a key, creating it if it doesn't exist
    /// 
    /// # Errors
    /// - [`DataLogError::EntryTypeMismatch`] if the entry type doesn't match the existing entry type
    /// - [`DataLogError::OutsideEntryLifetime`] if the entry is closed
    /// - [`DataLogError::Io`] if an IO error occurs
    /// - [`DataLogError::MetadataTooLarge`] if the metadata is too large
    #[inline]
    pub fn get_entry<T: StaticallyFrcTyped>(&mut self, key: impl ToString, metadata: Option<String>) -> Result<TypedEntryId<T>, DataLogError> {
        self.get_entry_dynamic(
            key,
            T::TYPE,
            metadata
        ).map(EntryId::typed::<T>)
    }

    /// Closes an entry, this will invalidate the entry id and any clones of it.
    /// 
    /// # Errors
    /// - [`DataLogError::InvalidDataLog`] if the entry is not in this datalog
    /// - [`DataLogError::OutsideEntryLifetime`] if the entry is closed
    /// - [`DataLogError::Io`] if an IO error occurs
    /// - [`DataLogError::NoSuchEntry`] if the entry doesn't exist
    pub fn close_entry(&mut self, id: EntryId) -> Result<(), DataLogError> {
        if id.datalog_id != self.datalog_id {
            return Err(DataLogError::InvalidDataLog);
        }

        let data = self.get_entry_data_mut(id.entry_id)?;

        if let EntryLifeStatus::Dead { .. } = data.lifestatus {
            return Err(DataLogError::OutsideEntryLifetime);
        } else if let EntryLifeStatus::Alive { start } = data.lifestatus {
            data.lifestatus = EntryLifeStatus::Dead {
                start,
                end: crate::now()
            };
        }

        // try and reclaim some memory
        data.key = String::new();
        data.entry_type = String::new();
        data.packing_buffer = Vec::new();

        ControlRecord::Finish.write_to(crate::now(), id.entry_id, &mut self.writer)?;

        Ok(())
    }

    /// Flushes to the file
    /// 
    /// # Errors
    /// - [`DataLogError::Io`] if an IO error occurs
    pub fn flush(&mut self) -> Result<(), DataLogError> {
        self.writer.flush()?;
        Ok(())
    }
}