#![allow(dead_code)]

use std::{collections::HashMap, hash::BuildHasher, io::Write};
use byteorder::{LittleEndian, WriteBytesExt};

use frclib_core::value::{FrcValue, IntoFrcValue};

use crate::{
    error::DataLogError,
    proto::util::{RecordByteReader, UInt},
    EntryId, EntryMetadata, EntryName, EntryType, FrcTimestamp,
};

#[allow(clippy::wildcard_imports)]
use super::entries::*;

fn chunk_by_record(bytes: &[u8]) -> Result<Vec<&[u8]>, DataLogError> {
    let mut chunks = Vec::new();
    let mut reader = RecordByteReader::new(bytes);
    while !reader.is_empty() {
        let bit_field = RecordElementBitfield::from_bits_truncate(
            reader.inspect_byte()?,
        );

        if reader.bytes_left() < bit_field.total_length() {
            break;
        }

        // 1-byte header length bitfield
        // 1 to 4-byte (32-bit) entry ID
        // 1 to 4-byte (32-bit) payload size (in bytes)
        // 1 to 8-byte (64-bit) timestamp (in integer microseconds)
        // payload data (arbitrary length)

        let header = reader.inspect_bytes(1 + bit_field.total_length())?;

        let payload_size = *UInt::from_binary(
            &header
                [1 + bit_field.id_length()..1 + bit_field.id_length() + bit_field.payload_length()],
        ).ok_or(DataLogError::RecordDeserialize("Failed to read payload size"))?;

        let total_size = 1 + bit_field.total_length() + usize::try_from(payload_size)?;

        let chunk = reader.bytes(total_size)?;

        chunks.push(chunk);
    }
    Ok(chunks)
}

pub fn parse_records<H: BuildHasher>(bytes: &[u8], type_map: &mut HashMap<u32, u32, H>) -> Result<Vec<Record>, DataLogError> {
    let chunks = chunk_by_record(bytes)?;
    let mut records = Vec::new();
    for chunk in chunks {
        if let Ok(record) = Record::from_binary(chunk, type_map) {
            if let Record::Control(control, _, _) = &record {
                if let Some(entry_type) = control.get_entry_type() {
                    #[allow(unused_results)]
                    {
                        type_map.insert(record.get_id(), get_str_type_serial(entry_type));
                    }
                }
            }
            records.push(record);
        }
    }
    Ok(records)
}

bitflags! {
    /// The header length bitfield encodes the length of each header field as follows (starting from the least significant bit):
    /// 2-bit entry ID length (00 = 1 byte, 01 = 2 bytes, 10 = 3 bytes, 11 = 4 bytes)
    /// 2-bit payload size length (00 = 1 byte, to 11 = 4 bytes)
    /// 3-bit timestamp length (000 = 1 byte, to 111 = 8 bytes)
    /// 1-bit spare (zero)
    #[derive(Debug, Clone, Copy)]
    pub struct RecordElementBitfield: u8 {
        const ID_1 = 0b01;
        const ID_2 = 0b10;
        const ID_3 = 0b11;
        const PAYLOAD_1 = 0b0100;
        const PAYLOAD_2 = 0b1000;
        const PAYLOAD_3 = 0b1100;
        const TIMESTAMP_1 = 0b1_0000;
        const TIMESTAMP_2 = 0b10_0000;
        const TIMESTAMP_3 = 0b11_0000;
        const SPARE = 0b100_0000;
    }
}

impl RecordElementBitfield {
    pub fn id_length(self) -> usize {
        match self.bits() & 0b11 {
            0b00 => 1,
            0b01 => 2,
            0b10 => 3,
            0b11 => 4,
            _ => unreachable!(),
        }
    }

    pub fn payload_length(self) -> usize {
        match (self.bits() >> 2) & 0b11 {
            0b00 => 1,
            0b01 => 2,
            0b10 => 3,
            0b11 => 4,
            _ => unreachable!(),
        }
    }

    pub fn timestamp_length(self) -> usize {
        match (self.bits() >> 4) & 0b111 {
            0b000 => 1,
            0b001 => 2,
            0b010 => 3,
            0b011 => 4,
            0b100 => 5,
            0b101 => 6,
            0b110 => 7,
            0b111 => 8,
            _ => unreachable!(),
        }
    }

    pub fn total_length(self) -> usize {
        self.id_length() + self.payload_length() + self.timestamp_length()
    }
}

#[derive(Debug, Clone)]
struct RecordElementSizes {
    pub bit_field: RecordElementBitfield,
    pub timestamp: UInt,
    pub id: UInt,
    pub payload: UInt,
}
impl RecordElementSizes {
    fn create(timestamp: FrcTimestamp, id: EntryId, payload: u32) -> Self {
        let wrapped_timestamp = UInt::from(timestamp).shrink();
        let wrapped_id = UInt::from(id).shrink();
        let wrapped_payload = UInt::from(payload).shrink();
        // create bitfield as little endian byte
        // let bit_field = ((wrapped_id.get_byte_count() - 1) as LeByte) & 0b11
        //     | (((wrapped_payload.get_byte_count() - 1) as LeByte) & 0b11) << 2
        //     | (((wrapped_timestamp.get_byte_count() - 1) as LeByte) & 0b111) << 4;
        let bit_field = RecordElementBitfield::from_bits_truncate(
            (wrapped_id.get_byte_count().saturating_sub(1)) & 0b11
                | ((wrapped_payload.get_byte_count().saturating_sub(1)) & 0b11) << 2
                | ((wrapped_timestamp.get_byte_count().saturating_sub(1)) & 0b111) << 4,
        );
        Self {
            bit_field,
            timestamp: wrapped_timestamp,
            id: wrapped_id,
            payload: wrapped_payload,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Record {
    Data(DataRecord, FrcTimestamp, EntryId),
    Control(ControlRecord, FrcTimestamp, EntryId),
}

impl Record {
    pub const fn get_timestamp(&self) -> FrcTimestamp {
        match self {
            Self::Data(_, timestamp, _) | Self::Control(_, timestamp, _)=> *timestamp
        }
    }

    pub const fn get_id(&self) -> EntryId {
        match self {
            Self::Control(_, _, id) | Self::Data(_, _, id) => *id
        }
    }

    pub const fn is_data(&self) -> bool {
        match self {
            Self::Data(_, _, _) => true,
            Self::Control(_, _, _) => false,
        }
    }

    pub const fn is_control(&self) -> bool {
        match self {
            Self::Data(_, _, _) => false,
            Self::Control(_, _, _) => true,
        }
    }

    pub const fn as_data(&self) -> Option<&DataRecord> {
        match self {
            Self::Data(data, _, _) => Some(data),
            Self::Control(_, _, _) => None,
        }
    }

    pub const fn as_control(&self) -> Option<&ControlRecord> {
        match self {
            Self::Data(_, _, _) => None,
            Self::Control(control, _, _) => Some(control),
        }
    }

    pub fn write_to(self, out_buffer: &mut impl Write) -> Result<(), DataLogError> {
        match self {
            Self::Control(control, timestamp, id) => control.write_to(timestamp, id, out_buffer),
            Self::Data(data, timestamp, id) => data.write_to(timestamp, id, out_buffer),
        }
    }

    pub fn from_binary<H: BuildHasher>(bytes: &[u8], type_map: &HashMap<u32, u32, H>) -> Result<Self, DataLogError> {
        let mut reader = RecordByteReader::new(bytes);
        let bit_field = reader.byte().unwrap_or_default();

        let id_length = (bit_field & 0b11) + 1;
        let payload_length = ((bit_field >> 2) & 0b11) + 1;
        let timestamp_length = ((bit_field >> 4) & 0b111) + 1;

        let id: u32;
        if let Ok(bin_int) = reader.bytes(id_length as usize) {
            id = UInt::from_binary(bin_int)
                // .map_or_else(|| 0u32, |f| *f as u32);
                .and_then(|f| u32::try_from(*f).ok())
                .ok_or_else(|| DataLogError::RecordDeserialize("Failed to read entry id"))?;
        } else {
            return Err(DataLogError::RecordReaderOutOfBounds("Entry id"));
        }

        reader.skip(payload_length as usize)?;

        let timestamp: FrcTimestamp;
        if let Ok(bin_int) = reader.bytes(timestamp_length as usize) {
            timestamp = UInt::from_binary(bin_int)
                .map_or_else(|| 0u64, |f| *f);
        } else {
            return Err(DataLogError::RecordReaderOutOfBounds("Timestamp"));
        }

        let is_control = id == 0u32;

        let mut type_serial = *type_map.get(&id)
            .unwrap_or(&RAW_TYPE_SERIAL);
        if !is_control && !SUPPORTED_TYPES_SERIALS.contains(&type_serial)  {
            type_serial = RAW_TYPE_SERIAL;
        }

        let record_payload = reader.the_rest();
        if is_control {
            if let Ok(control_record) = ControlRecord::from_binary(record_payload) {
                Ok(Self::Control(
                    control_record.0,
                    timestamp,
                    control_record.1,
                ))
            } else {
                Err(DataLogError::RecordDeserialize(
                    "Unsupported control record",
                ))
            }
        } else if let Ok(data_record) = DataRecord::from_binary(record_payload, type_serial) {
            Ok(Self::Data(data_record, timestamp, id))
        } else {
            Err(DataLogError::RecordDeserialize(
                "Unsupported data record",
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub enum ControlRecord {
    Start(EntryName, EntryType, EntryMetadata),
    Finish,
    Metadata(EntryMetadata),
}


impl ControlRecord {
    pub const fn get_control_type(&self) -> u8 {
        match self {
            Self::Start(_, _, _) => 0,
            Self::Finish => 1,
            Self::Metadata(_) => 2,
        }
    }

    pub const fn is_start(&self) -> bool {
        match self {
            Self::Start(_, _, _) => true,
            Self::Finish | Self::Metadata(_) => false,
        }
    }

    pub const fn get_entry_name(&self) -> Option<&EntryName> {
        match self {
            Self::Start(name, _, _) => Some(name),
            Self::Finish | Self::Metadata(_) => None,
        }
    }

    pub const fn get_entry_type(&self) -> Option<&EntryType> {
        match self {
            Self::Start(_, entry_type, _) => Some(entry_type),
            Self::Finish | Self::Metadata(_) => None,
        }
    }

    pub const fn get_entry_metadata(&self) -> Option<&EntryMetadata> {
        match self {
            Self::Start(_, _, entry_metadata) | Self::Metadata(entry_metadata) => Some(entry_metadata),
            Self::Finish => None,
        }
    }

    #[allow(unused_results)]
    pub fn write_to(self, timestamp: FrcTimestamp, id: EntryId, out_buffer: &mut impl Write) -> Result<(), DataLogError> {
        match self {
            Self::Start(name, entry_type, entry_metadata) => {
                let name_len = u32::try_from(name.len())?;
                let entry_type_len = u32::try_from(entry_type.len())?;
                let entry_metadata_len = u32::try_from(entry_metadata.len())?;

                let payload_len = 17 + name_len + entry_type_len + entry_metadata_len;

                let element_sizes = RecordElementSizes::create(timestamp, 0, payload_len);

                out_buffer.write_u8(element_sizes.bit_field.bits())?;                             //1-byte header length bitfield
                out_buffer.write_u8(0u8)?;                                                        //1 to 4-byte (32-bit) entry ID (0 int for control records)
                out_buffer.write_all(element_sizes.payload.as_binary())?;                        // 1 to 4-byte (32-bit) payload size (in bytes)
                out_buffer.write_all(element_sizes.timestamp.as_binary())?;                      // 1 to 8-byte (64-bit) timestamp (in microseconds)
                out_buffer.write_u8(0u8)?;                                                        // 1-byte control record type (0 for Start control records)
                out_buffer.write_all(&id.to_le_bytes())?;                             // 4-byte (32-bit) entry ID of entry being started
                out_buffer.write_u32::<LittleEndian>(name_len)?;            // 4-byte (32-bit) length of entry name string
                out_buffer.write_all(name.as_bytes())?;                               // UTF-8 encoded entry name string
                out_buffer.write_u32::<LittleEndian>(entry_type_len)?;      // 4-byte (32-bit) length of entry type string
                out_buffer.write_all(entry_type.as_bytes())?;                         // UTF-8 encoded entry type string
                out_buffer.write_u32::<LittleEndian>(entry_metadata_len)?;  // 4-byte (32-bit) length of entry metadata string
                out_buffer.write_all(entry_metadata.as_bytes())?;                     // UTF-8 encoded entry metadata string
            }
            Self::Finish => {
                let payload_len = 5u32;

                let element_sizes = RecordElementSizes::create(timestamp, 0, payload_len);

                out_buffer.write_u8(element_sizes.bit_field.bits())?;             //1-byte header length bitfield
                out_buffer.write_u8(0u8)?;                                        //1 to 4-byte (32-bit) entry ID (0 int for control records)
                out_buffer.write_all(element_sizes.payload.as_binary())?;        // 1 to 4-byte (32-bit) payload size (in bytes)
                out_buffer.write_all(element_sizes.timestamp.as_binary())?;      // 1 to 8-byte (64-bit) timestamp (in microseconds)
                out_buffer.write_u8(1u8)?;                                          // 1-byte control record type (1 for Finish control records)
                out_buffer.write_all(&id.to_le_bytes())?;             // 4-byte (32-bit) entry ID of entry being finished
            }
            Self::Metadata(entry_metadata) => {
                let entry_metadata_len = u32::try_from(entry_metadata.len())?;

                let payload_len = 9 + entry_metadata_len;

                let element_sizes = RecordElementSizes::create(timestamp, 0, payload_len );

                out_buffer.write_u8(element_sizes.bit_field.bits())?;                             //1-byte header length bitfield
                out_buffer.write_u8(0u8)?;                                                        //1 to 4-byte (32-bit) entry ID (0 int for control records)
                out_buffer.write_all(element_sizes.payload.as_binary())?;                        // 1 to 4-byte (32-bit) payload size (in bytes)
                out_buffer.write_all(element_sizes.timestamp.as_binary())?;                      // 1 to 8-byte (64-bit) timestamp (in microseconds)
                out_buffer.write_u8(2u8)?;                                                          // 1-byte control record type (2 for Metadata control records)
                out_buffer.write_u32::<LittleEndian>(entry_metadata_len)?;  // 4-byte (32-bit) length of entry metadata string
                out_buffer.write_all(&id.to_le_bytes())?;                             // 4-byte (32-bit) entry ID of entry being finished
            }
        };
        Ok(())
    }

    pub fn from_binary(bytes: &[u8]) -> Result<(Self, EntryId), DataLogError> {
        let mut reader = RecordByteReader::new(bytes);
        let control_type = reader.byte()?;
        let entry_id = reader.u32()?;
        match control_type {
            0 => {
                if let Ok(name_len) = reader.u32() {
                    //checks name bytes
                    if reader.bytes_left() < name_len as usize {
                        return Err(DataLogError::RecordReaderOutOfBounds(
                            "Start control record name",
                        ));
                    }
                    let name = reader.string(name_len as usize)?;
                    if let Ok(type_len) = reader.u32() {
                        //checks type bytes
                        if reader.bytes_left() < type_len as usize {
                            return Err(DataLogError::RecordReaderOutOfBounds(
                                "Start control record type",
                            ));
                        }
                        let entry_type = reader.string(type_len as usize)?;
                        if let Ok(metadata_len) = reader.u32() {
                            //checks metadata bytes
                            if reader.bytes_left() < metadata_len as usize {
                                return Err(DataLogError::RecordReaderOutOfBounds(
                                    "Start control record metadata",
                                ));
                            }
                            let entry_metadata = reader.string(metadata_len as usize)?;
                            return Ok((
                                Self::Start(
                                    name.to_string(),
                                    entry_type.to_string(),
                                    entry_metadata.to_string()
                                ),
                                entry_id
                            ));
                        }
                    }
                }
                //one of the checks above failed
                Err(DataLogError::RecordReaderOutOfBounds(
                    "Start control record",
                ))
            }
            1 => Ok((Self::Finish, entry_id)),
            2 => {
                if let Ok(metadata_len) = reader.u32() {
                    if reader.bytes_left() != metadata_len as usize {
                        return Err(DataLogError::RecordReaderOutOfBounds(
                            "Metadata control record string",
                        ));
                    }
                    Ok((
                        Self::Metadata(reader.string(metadata_len as usize)?.to_string()),
                        entry_id,
                    ))
                } else {
                    Err(DataLogError::RecordReaderOutOfBounds(
                        "Metadata control record length",
                    ))
                }
            }
            _ => Err(DataLogError::RecordDeserialize("Unsupported control record type")),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DataRecord {
    Raw(Box<[u8]>),
    Boolean(bool),
    Integer(i64),
    Float(f32),
    Double(f64),
    String(Box<str>),
    BooleanArray(Box<[bool]>),
    IntegerArray(Box<[i64]>),
    FloatArray(Box<[f32]>),
    DoubleArray(Box<[f64]>),
    StringArray(Box<[Box<str>]>),
}

impl DataRecord {
    pub fn get_data_type(&self) -> EntryType {
        match self {
            Self::Raw(_) => "raw".to_string(),
            Self::Boolean(_) => "boolean".to_string(),
            Self::Integer(_) => "int64".to_string(),
            Self::Float(_) => "float".to_string(),
            Self::Double(_) => "double".to_string(),
            Self::String(_) => "string".to_string(),
            Self::BooleanArray(_) => "boolean[]".to_string(),
            Self::IntegerArray(_) => "int64[]".to_string(),
            Self::FloatArray(_) => "float[]".to_string(),
            Self::DoubleArray(_) => "double[]".to_string(),
            Self::StringArray(_) => "string[]".to_string(),
        }
    }

    pub fn matches_type(&self, e_type: &EntryType) -> bool {
        match self {
            Self::Raw(_) => e_type == "raw",
            Self::Boolean(_) => e_type == "boolean",
            Self::Integer(_) => e_type == "int64",
            Self::Float(_) => e_type == "float",
            Self::Double(_) => e_type == "double",
            Self::String(_) => e_type == "string",
            Self::BooleanArray(_) => e_type == "boolean[]",
            Self::IntegerArray(_) => e_type == "int64[]",
            Self::FloatArray(_) => e_type == "float[]",
            Self::DoubleArray(_) => e_type == "double[]",
            Self::StringArray(_) => e_type == "string[]",
        }
    }

    pub const fn get_type_serial(&self) -> u32 {
        match self {
            Self::Raw(_) => RAW_TYPE_SERIAL,
            Self::Boolean(_) => BOOLEAN_TYPE_SERIAL,
            Self::Integer(_) => INT_TYPE_SERIAL,
            Self::Float(_) => FLOAT_TYPE_SERIAL,
            Self::Double(_) => DOUBLE_TYPE_SERIAL,
            Self::String(_) => STRING_TYPE_SERIAL,
            Self::BooleanArray(_) => BOOLEAN_ARRAY_TYPE_SERIAL,
            Self::IntegerArray(_) => INT_ARRAY_TYPE_SERIAL,
            Self::FloatArray(_) => FLOAT_ARRAY_TYPE_SERIAL,
            Self::DoubleArray(_) => DOUBLE_ARRAY_TYPE_SERIAL,
            Self::StringArray(_) => STRING_ARRAY_TYPE_SERIAL,
        }
    }

    #[allow(unused_results)]
    #[inline]
    pub fn write_to(self, timestamp: FrcTimestamp, id: EntryId, out_buffer: &mut impl Write) -> Result<(), DataLogError> {
        let payload_size = self.binary_payload_size().ok_or(DataLogError::RecordTooLarge)?;
        let element_sizes = RecordElementSizes::create(timestamp, id, payload_size);

        out_buffer.write_u8(element_sizes.bit_field.bits())?; //1-byte header length bitfield
        out_buffer.write_all(element_sizes.id.as_binary())?; //1 to 4-byte (32-bit) entry ID
        out_buffer.write_all(element_sizes.payload.as_binary())?; // 1 to 4-byte (32-bit) payload size (in bytes)
        out_buffer.write_all(element_sizes.timestamp.as_binary())?; // 1 to 8-byte (64-bit) timestamp (in microseconds)

        match self {
            Self::Raw(data) => out_buffer.write_all(data.iter().as_slice())?,
            Self::Boolean(data) => out_buffer.write_u8(u8::from(data))?,
            Self::Integer(data) => out_buffer.write_all(&data.to_le_bytes())?,
            Self::Float(data) => out_buffer.write_all(&data.to_le_bytes())?,
            Self::Double(data) => out_buffer.write_f64::<LittleEndian>(data)?,
            Self::String(data) => out_buffer.write_all(data.as_bytes())?,
            Self::BooleanArray(data) => {
                for b in data.iter() {
                    out_buffer.write_u8(u8::from(*b))?;
                }
            },
            Self::IntegerArray(data) => {
                for i in data.iter() {
                    out_buffer.write_i64::<LittleEndian>(*i)?;
                }
            },
            Self::FloatArray(data) => {
                for f in data.iter() {
                    out_buffer.write_f32::<LittleEndian>(*f)?;
                }
            },
            Self::DoubleArray(data) => {
                for d in data.iter() {
                    out_buffer.write_f64::<LittleEndian>(*d)?;
                }
            },
            Self::StringArray(data) => {
                for s in data.iter() {
                    if let Ok(len) = <usize as TryInto<u32>>::try_into(s.len()) {
                        out_buffer.write_u32::<LittleEndian>(len)?;
                        out_buffer.write_all(s.as_bytes())?;
                    }
                }
            },
        };

        Ok(())
    }

    /// # Returns
    /// The size of the binary representation of the record in bytes
    /// or None if the record is too large to be represented in a u32
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    pub fn binary_payload_size(&self) -> Option<u32> {
        const BOOL_SIZE: u32 = core::mem::size_of::<bool>() as u32;
        const INT_SIZE: u32 = core::mem::size_of::<i64>() as u32;
        const FLOAT_SIZE: u32 = core::mem::size_of::<f32>() as u32;
        const DOUBLE_SIZE: u32 = core::mem::size_of::<f64>() as u32;
        #[inline]
        fn len_u32<T>(data: &[T]) -> Option<u32> {
            data.len().try_into().ok()
        }
        match self {
            Self::Raw(data) => data.len().try_into().ok(),
            Self::Boolean(_) => Some(BOOL_SIZE),
            Self::Integer(_) => Some(INT_SIZE),
            Self::Float(_) => Some(FLOAT_SIZE),
            Self::Double(_) => Some(DOUBLE_SIZE),
            Self::String(data) => len_u32(data.as_bytes()),
            Self::BooleanArray(data) => len_u32(data)
                .and_then(|len| len.checked_mul(BOOL_SIZE)),
            Self::IntegerArray(data) => len_u32(data)
                .and_then(|len| len.checked_mul(INT_SIZE)),
            Self::FloatArray(data) => len_u32(data)
                .and_then(|len| len.checked_mul(FLOAT_SIZE)),
            Self::DoubleArray(data) => len_u32(data)
                .and_then(|len| len.checked_mul(DOUBLE_SIZE)),
            Self::StringArray(data) => {
                let mut size: usize = 0;
                for s in data.iter() {
                    size = size.checked_add(core::mem::size_of::<u32>() + s.len())?;
                }
                size.try_into().ok()
            }
        }
    }

    pub fn from_binary(bytes: &[u8], type_serial: u32) -> Result<Self, DataLogError> {
        if bytes.is_empty() {
            return Err(DataLogError::RecordReaderOutOfBounds("Bytes len is 0"));
        }
        Self::from_binary_inner(bytes, type_serial)
    }


    fn from_binary_inner(bytes: &[u8], type_serial: u32) -> Result<Self, DataLogError> {
        let mut reader = RecordByteReader::new(bytes);
        // ordered by most to least used, structs fall under raw
        match type_serial {
            DOUBLE_TYPE_SERIAL => {
                Ok(Self::Double(reader.f64()?))
            }
            STRING_TYPE_SERIAL => {
                Ok(
                    Self::String(String::from_utf8(Vec::from(reader.the_rest()))?.into_boxed_str())
                )
            }
            RAW_TYPE_SERIAL => Ok(Self::Raw(Box::from(reader.the_rest()))),
            BOOLEAN_TYPE_SERIAL => {
                Ok(Self::Boolean(reader.bool()?))
            }
            DOUBLE_ARRAY_TYPE_SERIAL => {
                let mut doubles = Vec::new();
                while reader.bytes_left() >= 8 {
                    doubles.push(reader.f64()?);
                }
                Ok(Self::DoubleArray(doubles.into_boxed_slice()))
            }
            INT_TYPE_SERIAL => {
                Ok(Self::Integer(reader.i64()?))
            }
            FLOAT_TYPE_SERIAL => {
                Ok(Self::Float(reader.f32()?))
            }
            BOOLEAN_ARRAY_TYPE_SERIAL => {
                let mut bools = Vec::new();
                while !reader.is_empty() {
                    bools.push(reader.bool()?);
                }
                Ok(Self::BooleanArray(bools.into_boxed_slice()))
            }
            INT_ARRAY_TYPE_SERIAL => {
                let mut ints = Vec::new();
                while reader.bytes_left() >= 8 {
                    ints.push(reader.i64()?);
                }
                Ok(Self::IntegerArray(ints.into_boxed_slice()))
            }
            FLOAT_ARRAY_TYPE_SERIAL => {
                let mut floats = Vec::new();
                while reader.bytes_left() >= 4 {
                    floats.push(reader.f32()?);
                }
                Ok(Self::FloatArray(floats.into_boxed_slice()))
            }
            STRING_ARRAY_TYPE_SERIAL => {
                let mut strings = Vec::new();
                while reader.bytes_left() >= 4 {
                    let len = u32::try_from(*reader.uint(4)?)?;
                    if reader.bytes_left() < len as usize {
                        return Err(DataLogError::RecordReaderOutOfBounds("String[]"));
                    }
                    strings.push(
                        String::from_utf8(Vec::from(reader.bytes(len as usize)?))?,
                    );
                }
                Ok(Self::StringArray(strings.into_iter().map(String::into_boxed_str).collect()))
            }
            _ => Err(DataLogError::RecordType("Unsupported type")),
        }
    }
}

impl IntoFrcValue for DataRecord {
    #[inline]
    fn into_frc_value(self) -> FrcValue {
        match self {
            Self::Raw(data) => FrcValue::Raw(data),
            Self::Boolean(data) => FrcValue::Boolean(data),
            Self::Integer(data) => FrcValue::Int(data),
            Self::Float(data) => FrcValue::Float(data),
            Self::Double(data) => FrcValue::Double(data),
            Self::String(data) => FrcValue::String(data),
            Self::BooleanArray(data) => FrcValue::BooleanArray(data),
            Self::IntegerArray(data) => FrcValue::IntArray(data),
            Self::FloatArray(data) => FrcValue::FloatArray(data),
            Self::DoubleArray(data) => FrcValue::DoubleArray(data),
            Self::StringArray(data) => FrcValue::StringArray(data)
        }
    }
}

impl From<FrcValue> for DataRecord {
    fn from(value: FrcValue) -> Self {
        match value {
            FrcValue::Raw(data) => Self::Raw(data),
            FrcValue::Boolean(data) => Self::Boolean(data),
            FrcValue::Int(data) => Self::Integer(data),
            FrcValue::Float(data) => Self::Float(data),
            FrcValue::Double(data) => Self::Double(data),
            FrcValue::String(data) => Self::String(data),
            FrcValue::BooleanArray(data) => Self::BooleanArray(data),
            FrcValue::IntArray(data) => Self::IntegerArray(data),
            FrcValue::FloatArray(data) => Self::FloatArray(data),
            FrcValue::DoubleArray(data) => Self::DoubleArray(data),
            FrcValue::StringArray(data) => Self::StringArray(data),
            FrcValue::Struct(struct_packet) | FrcValue::StructArray(struct_packet) => Self::Raw(struct_packet.data),
            FrcValue::Void => Self::Raw(Box::new([])),
        }
    }
}
