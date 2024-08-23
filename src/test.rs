

use std::{collections::HashMap, fs::File};

use frclib_core::value::{FrcValue, IntoFrcValue};

use crate::{now, proto::{entries::{get_data_type, get_str_type_serial, TEST_SERIAL}, records::{DataRecord, Record}, util::UInt}, reader::{DataLogReader, DataLogReaderConfig}, writer::DataLogWriter};

extern crate test;
use test::Bencher;

fn test_record_type(payload: impl IntoFrcValue) {
    let payload = payload.into_frc_value();
    let timestamp = now();
    let entry_id = 2u32.pow(24);
    let timestamp_size = UInt::from(timestamp).get_byte_count();
    let entry_id_size = UInt::from(entry_id).get_byte_count();
    let data_record = DataRecord::from(payload.clone());
    let payload_package_size = data_record.binary_payload_size()
        .expect("Payload size greater than u32::MAX");
    let payload_len_size = UInt::from(payload_package_size).get_byte_count();
    let record = Record::Data(data_record, timestamp, entry_id);
    let mut bytes = Vec::new();
    let _ = record.clone().write_to(&mut bytes);
    assert_eq!(bytes.len(),
        1 /*bit field */
        + entry_id_size as usize
        + payload_len_size as usize
        + timestamp_size as usize
        + payload_package_size as usize);

    let entry_type_map = HashMap::from([(entry_id, get_data_type(&payload.get_type()).expect("Payload type was void").to_string())]);
    let rerecord = Record::from_binary(&bytes, &entry_type_map).expect("Failed to read record");
    assert_eq!(rerecord.is_data(), record.is_data());
    assert_eq!(rerecord.get_id(), record.get_id());
    assert_eq!(rerecord.get_timestamp(), record.get_timestamp());
    assert_eq!(
        FrcValue::from(rerecord.as_data().expect("rerecord was not a data record").clone()),
        FrcValue::from(record.as_data().expect("record was not a data record").clone()));
}

#[test]
fn test_record_types() {
    test_record_type(true);
    test_record_type(10i32);
    test_record_type(10.0f32);
    test_record_type(10.0f64);
    test_record_type("owo");
    test_record_type([true, false, true]);
    test_record_type([1, 2, 3]);
    test_record_type([1.0, 2.0, 3.0]);
    test_record_type([1.0, 2.0, 3.0]);
    test_record_type(["owo", "uwu"]);
} 

#[test]
fn test_read() {
    let reader = DataLogReader::try_new(
        File::open("./test_logs/test_read.wpilog").expect("Failed to open file"),
        DataLogReaderConfig::default()
    ).expect("Failed to create reader");

    for key in reader.get_all_entry_keys() {
        let types = reader.read_entry_type_str(key);
        println!("{key}: {types:?}");
    }
}

#[bench]
fn bench_read(b: &mut Bencher) {
    let buffer = std::io::Cursor::new(std::fs::read("./test_logs/massive.wpilog").expect("Failed to read file"));
    let exec = || DataLogReader::try_new(
        buffer.clone(),
        DataLogReaderConfig::default()
    ).expect("Failed to create reader");

    b.iter(exec);
}

#[test]
fn test_write() {
    let mut writer = DataLogWriter::new(
        std::fs::File::create("./test_logs/test_write.wpilog").expect("Failed to create file"),
        "test"
    ).expect("Failed to create writer");

    let entry = writer.get_entry::<i32>("test", None).expect("Failed to get entry");
    writer.write_timestamped(entry, 10, now() - 5).expect("Failed to write entry");
    writer.write_timestamped(entry, 20, now() + 20).expect("Failed to write entry");
    writer.write_timestamped(entry, 30, now() + 50).expect("Failed to write entry");
}

#[test]
fn test_type_serial() {
    assert!(TEST_SERIAL == get_str_type_serial("test"));
}