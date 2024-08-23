//! # frclib-datalog
#![cfg_attr(test, feature(test))]
#![deny(clippy::all, clippy::pedantic, clippy::nursery)]
#![deny(
    warnings,
    missing_copy_implementations,
    single_use_lifetimes,
    variant_size_differences,
    arithmetic_overflow,
    missing_debug_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unused_import_braces,
    unused_results,
    unused_lifetimes,
    unused_unsafe,
    useless_ptr_null_checks,
    cenum_impl_drop_cast,
    while_true,
    unused_features,
    absolute_paths_not_starting_with_crate,
    unused_allocation,
    unreachable_code,
    unused_comparisons,
    unused_parens,
    asm_sub_register,
    break_with_label_and_loop,
    bindings_with_variant_name,
    anonymous_parameters,
    clippy::unwrap_used,
    clippy::panicking_unwrap,
    missing_abi,
    missing_fragment_specifier,
    clippy::missing_safety_doc,
    clippy::missing_asserts_for_indexing,
    clippy::missing_assert_message,
    clippy::possible_missing_comma,
    deprecated
)]
#![allow(clippy::module_name_repetitions, clippy::option_if_let_else)]
#![cfg_attr(
    not(test),
    forbid(
        clippy::panic,
        clippy::todo,
        clippy::unimplemented,
        clippy::expect_used
    )
)]
#![cfg_attr(not(test), warn(missing_docs))]

#[macro_use]
extern crate bitflags;

pub(crate) mod proto;

/// # Errors
/// 
/// TODO
pub mod error;

/// # Reading
/// 
/// TODO
pub mod reader;

/// # Writing
/// 
/// TODO
pub mod writer;

#[cfg(test)]
mod test;

pub use error::DataLogError;
use frclib_core::value::FrcTimestamp;

///A unique identifier for a data entry
type EntryId = u32;
///A string representing a data entry name
type EntryName = String;
///A string representing a data entry type
type EntryType = String;
///A string in json format representing data entry metadata
type EntryMetadata = String;
///A hash map of entry id to entry types
type EntryTypeMap = std::collections::HashMap<EntryId, EntryType>;


/// Gets the uptime in microseconds
pub(crate) fn now() -> FrcTimestamp {
    frclib_core::time::uptime().as_micros()
        .try_into()
        .unwrap_or(FrcTimestamp::MAX)
}

/// A timestamped value
#[derive(Debug, Clone)]
pub struct TimestampedValue<T> {
    /// The timestamp of the value
    pub timestamp: FrcTimestamp,
    /// The value
    pub value: T,
}

impl <T> TimestampedValue<T> {
    /// Creates a new timestamped value
    pub const fn new(timestamp: FrcTimestamp, value: T) -> Self {
        Self {
            timestamp,
            value
        }
    }

    /// Creates a new timestamped value with the current time
    pub fn new_now(value: T) -> Self {
        Self {
            timestamp: now(),
            value
        }
    }
}