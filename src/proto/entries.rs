
use std::num::NonZeroU32;

use frclib_core::value::FrcType;

#[inline]
#[allow(clippy::cast_possible_truncation)]
pub fn get_str_type_serial(ty: &str) -> u32 {
    let mut value = 0;
    for (i, char) in ty.chars().enumerate() {
        value += char as u32 * ((i + 1) % 8) as u32;
    }
    let len: u32 = ty.len() as u32;
    (value * len) + len
}

macro_rules! const_get_data_type_serial {
    ($($char:literal),*) => {
        {
            let mut value = 0u32;
            let mut index = 0u32;
            $(
                value += $char as u32 * ((index + 1) % 8);
                index += 1;
            )*
            (value * index) + index 
        }
    };
}

pub const RAW_TYPE_SERIAL: u32 = const_get_data_type_serial!('r', 'a', 'w');
pub const BOOLEAN_TYPE_SERIAL: u32 = const_get_data_type_serial!('b', 'o', 'o', 'l', 'e', 'a', 'n');
pub const INT_TYPE_SERIAL: u32 = const_get_data_type_serial!('i', 'n', 't', '6', '4');
pub const FLOAT_TYPE_SERIAL: u32 = const_get_data_type_serial!('f', 'l', 'o', 'a', 't');
pub const DOUBLE_TYPE_SERIAL: u32 = const_get_data_type_serial!('d', 'o', 'u', 'b', 'l', 'e');
pub const STRING_TYPE_SERIAL: u32 = const_get_data_type_serial!('s', 't', 'r', 'i', 'n', 'g');
pub const BOOLEAN_ARRAY_TYPE_SERIAL: u32 = const_get_data_type_serial!('b', 'o', 'o', 'l', 'e', 'a', 'n', '[', ']');
pub const INT_ARRAY_TYPE_SERIAL: u32 = const_get_data_type_serial!('i', 'n', 't', '6', '4', '[', ']');
pub const FLOAT_ARRAY_TYPE_SERIAL: u32 = const_get_data_type_serial!('f', 'l', 'o', 'a', 't', '[', ']');
pub const DOUBLE_ARRAY_TYPE_SERIAL: u32 = const_get_data_type_serial!('d', 'o', 'u', 'b', 'l', 'e', '[', ']');
pub const STRING_ARRAY_TYPE_SERIAL: u32 = const_get_data_type_serial!('s', 't', 'r', 'i', 'n', 'g', '[', ']');

#[allow(unused)]
pub const SUPPORTED_TYPES: [&str; 11] = [
    "raw",
    "boolean",
    "double",
    "float",
    "int64",
    "string",
    "boolean[]",
    "double[]",
    "float[]",
    "int64[]",
    "string[]",
];

pub const SUPPORTED_TYPES_SERIALS: [u32; 11] = [
    RAW_TYPE_SERIAL,
    BOOLEAN_TYPE_SERIAL,
    INT_TYPE_SERIAL,
    FLOAT_TYPE_SERIAL,
    DOUBLE_TYPE_SERIAL,
    STRING_TYPE_SERIAL,
    BOOLEAN_ARRAY_TYPE_SERIAL,
    INT_ARRAY_TYPE_SERIAL,
    FLOAT_ARRAY_TYPE_SERIAL,
    DOUBLE_ARRAY_TYPE_SERIAL,
    STRING_ARRAY_TYPE_SERIAL,
];

#[cfg(test)]
pub const TEST_SERIAL: u32 = const_get_data_type_serial!('t', 'e', 's', 't');

pub const fn get_data_type(ty: &FrcType) -> Option<&'static str> {
    match ty {
        FrcType::Raw => Some("raw"),
        FrcType::Boolean => Some("boolean"),
        FrcType::Int => Some("int64"),
        FrcType::Float => Some("float"),
        FrcType::Double => Some("double"),
        FrcType::String => Some("string"),
        FrcType::BooleanArray => Some("boolean[]"),
        FrcType::IntArray => Some("int64[]"),
        FrcType::FloatArray => Some("float[]"),
        FrcType::DoubleArray => Some("double[]"),
        FrcType::StringArray => Some("string[]"),
        FrcType::Struct(desc) | FrcType::StructArray(desc) => Some(desc.type_str),
        FrcType::Void => None,
    }
}

/// A faster way of comparing type equality, is not immune to hash collisions
pub fn get_data_type_serial(ty: &FrcType) -> NonZeroU32 {
    const INVALID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(u32::MAX) };
    match ty {
        FrcType::Raw => unsafe { NonZeroU32::new_unchecked(RAW_TYPE_SERIAL) },
        FrcType::Boolean => unsafe { NonZeroU32::new_unchecked(BOOLEAN_TYPE_SERIAL) },
        FrcType::Int => unsafe { NonZeroU32::new_unchecked(INT_TYPE_SERIAL) },
        FrcType::Float => unsafe { NonZeroU32::new_unchecked(FLOAT_TYPE_SERIAL) },
        FrcType::Double => unsafe { NonZeroU32::new_unchecked(DOUBLE_TYPE_SERIAL) },
        FrcType::String => unsafe { NonZeroU32::new_unchecked(STRING_TYPE_SERIAL) },
        FrcType::BooleanArray => unsafe { NonZeroU32::new_unchecked(BOOLEAN_ARRAY_TYPE_SERIAL) },
        FrcType::IntArray => unsafe { NonZeroU32::new_unchecked(INT_ARRAY_TYPE_SERIAL) },
        FrcType::FloatArray => unsafe { NonZeroU32::new_unchecked(FLOAT_ARRAY_TYPE_SERIAL) },
        FrcType::DoubleArray => unsafe { NonZeroU32::new_unchecked(DOUBLE_ARRAY_TYPE_SERIAL) },
        FrcType::StringArray => unsafe { NonZeroU32::new_unchecked(STRING_ARRAY_TYPE_SERIAL) },
        FrcType::Struct(desc) | FrcType::StructArray(desc) => {
            NonZeroU32::new(get_str_type_serial(desc.type_str))
                .unwrap_or(INVALID)
        }
        FrcType::Void => INVALID,
    }
}


#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub enum EntryLifeStatus {
    Alive{
        start: u64
    },
    Dead{
        start: u64,
        end: u64
    }
}


