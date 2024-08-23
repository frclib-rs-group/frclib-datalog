
use std::num::NonZeroU32;

use frclib_core::value::FrcType;

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
        FrcType::Raw => unsafe { NonZeroU32::new_unchecked(1995) },
        FrcType::Boolean => unsafe { NonZeroU32::new_unchecked(20594) },
        FrcType::Int => unsafe { NonZeroU32::new_unchecked(5745) },
        FrcType::Float => unsafe { NonZeroU32::new_unchecked(8095) },
        FrcType::Double => unsafe { NonZeroU32::new_unchecked(13266) },
        FrcType::String => unsafe { NonZeroU32::new_unchecked(13662) },
        FrcType::BooleanArray => unsafe { NonZeroU32::new_unchecked(40563) },
        FrcType::IntArray => unsafe { NonZeroU32::new_unchecked(16422) },
        FrcType::FloatArray => unsafe { NonZeroU32::new_unchecked(19712) },
        FrcType::DoubleArray => unsafe { NonZeroU32::new_unchecked(28736) },
        FrcType::StringArray => unsafe { NonZeroU32::new_unchecked(29264) },
        FrcType::Struct(desc) | FrcType::StructArray(desc) => {
            NonZeroU32::new(get_str_type_hash(desc.type_str))
                .unwrap_or(INVALID)
        }
        FrcType::Void => INVALID,
    }
}

#[inline]
#[allow(clippy::cast_possible_truncation)]
fn get_str_type_hash(ty: &str) -> u32 {
    let mut value = 0;
    for (i, char) in ty.chars().enumerate() {
        value += char as u32 * ((i + 1) % 10) as u32;
    }
    value * ty.len() as u32
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum EntryLifeStatus {
    Alive{
        start: u64
    },
    Dead{
        start: u64,
        end: u64
    }
}


