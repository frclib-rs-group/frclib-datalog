# A Rust Implementation of WPILIB DataLog format

the specs can be found [here](https://github.com/wpilibsuite/allwpilib/blob/main/wpiutil/doc/datalog.adoc)

## Usage

### Writing

```rust
use std::{path::PathBuf, fs:File};
use frclib_datalog::DataLogWriter;
use frclib_core::value::FrcValue;

let path = PathBuf::from("path/to/file");
let reader = DataLogWriter::try_new(File::create(path).unwrap(), Default::default())
        .expect("Failed to create writer");

let entry = writer.get_entry::<i32>("test", None).expect("Failed to get entry");
writer.write_timestamped(entry, 10, now() - 5).expect("Failed to write entry");
writer.write_timestamped(entry, 20, now() + 20).expect("Failed to write entry");
writer.write_timestamped(entry, 30, now() + 50).expect("Failed to write entry");
```

### Reading

```rust
use std::{path::PathBuf, fs:File};
use frclib_datalog::DataLogReader;
use frclib_core::value::FrcValue;

let path = PathBuf::from("path/to/file");
let reader = DataLogReader::try_new(File::open(path).unwrap(), Default::default())
        .expect("Failed to create reader");

reader.read_entry("entry_name").into_iter().for_each(|value| {
    match value.value {
        FrcValue::Int(i) => println!("Int: {}", i),
        _ => println!("Not an int")
    }
});
```

## Benchmarks

Haven't setup anything formal yet but on my maching (r7 5800x mobile) it reads and decodes a 103mb file in 0.7s
and a 17mb file in 0.11s