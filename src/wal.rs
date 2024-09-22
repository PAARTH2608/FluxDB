use crate::mem_table::InMemoryTable;
use crate::utils::find_files_with_extension;
use crate::wal_iterator::{LogFileIterator, LogRecord};
use std::fs::{remove_file, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Write Ahead Log (WAL) - captures operations performed on an in-memory table (MemTable)
/// for potential recovery in case of system failures.
pub struct WAL {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    /// Initializes a new WAL file in the specified directory.
    pub fn create_new(dir: &Path) -> io::Result<WAL> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let path = dir.join(format!("{}.wal", timestamp));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(WAL { path, writer })
    }

    /// Opens an existing WAL file for appending new operations.
    pub fn open_existing(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let writer = BufWriter::new(file);

        Ok(WAL {
            path: path.to_owned(),
            writer,
        })
    }

    /// Loads existing WAL files in the given directory, recovering the in-memory state and returning
    /// a fresh WAL instance.
    pub fn recover_from_directory(dir: &Path) -> io::Result<(WAL, InMemoryTable)> {
        let mut wal_files = find_files_with_extension(dir, "wal");
        wal_files.sort();

        let mut mem_table = InMemoryTable::new();
        let mut active_wal = WAL::create_new(dir)?;

        for wal_path in wal_files.iter() {
            if let Ok(wal) = WAL::open_existing(wal_path) {
                for log in wal.into_iter() {
                    if log.is_removed {
                        mem_table.remove(&log.identifier, log.event_time);
                        active_wal.record_removal(&log.identifier, log.event_time)?;
                    } else {
                        mem_table.insert(
                            &log.identifier,
                            log.data.as_ref().unwrap(),
                            log.event_time,
                        );
                        active_wal.record_insertion(
                            &log.identifier,
                            &log.data.unwrap(),
                            log.event_time,
                        )?;
                    }
                }
            }
        }

        active_wal.flush()?; // Ensure all writes are saved
        for wal_path in wal_files {
            remove_file(wal_path)?; // Clean up WAL files
        }

        Ok((active_wal, mem_table))
    }

    /// Adds a new key-value pair operation to the WAL.
    pub fn record_insertion(
        &mut self,
        key: &[u8],
        value: &[u8],
        timestamp: u128,
    ) -> io::Result<()> {
        // Ensure the correct order and data types for writes
        self.writer.write_all(&(key.len() as u64).to_le_bytes())?; // Key size
        self.writer.write_all(&(false as u8).to_le_bytes())?; // Deletion flag (false)
        self.writer.write_all(&(value.len() as u64).to_le_bytes())?; // Value size
        self.writer.write_all(key)?; // Key
        self.writer.write_all(value)?; // Value
        self.writer.write_all(&timestamp.to_le_bytes())?; // Timestamp
        Ok(())
    }

    /// Records a removal operation in the WAL.
    pub fn record_removal(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.writer.write_all(&(key.len() as u64).to_le_bytes())?; // Key size
        self.writer.write_all(&(true as u8).to_le_bytes())?; // Deletion flag (true)
        self.writer.write_all(key)?; // Key
        self.writer.write_all(&timestamp.to_le_bytes())?; // Timestamp
        Ok(())
    }

    /// Ensures that all buffered writes are saved to disk.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl IntoIterator for WAL {
    type IntoIter = LogFileIterator;
    type Item = LogRecord;

    /// Converts a WAL instance into an iterator over the log entries.
    fn into_iter(self) -> LogFileIterator {
        LogFileIterator::from_path(self.path).expect("Failed to create log iterator")
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::WAL;
    use rand::Rng;
    use std::fs::{create_dir_all, remove_dir_all, File};
    use std::io::{BufReader, Read};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn validate_log_entry(
        reader: &mut BufReader<File>,
        expected_key: &[u8],
        expected_value: Option<&[u8]>,
        expected_timestamp: u128,
        is_deleted: bool,
    ) {
        let mut buffer = [0; 8];
        reader.read_exact(&mut buffer).unwrap();
        let key_size = usize::from_le_bytes(buffer);
        assert_eq!(key_size, expected_key.len(), "Key size mismatch");

        let mut flag = [0; 1];
        reader.read_exact(&mut flag).unwrap();
        let deletion_flag = flag[0] != 0;
        assert_eq!(deletion_flag, is_deleted, "Deletion flag mismatch");

        if deletion_flag {
            let mut key = vec![0; key_size];
            reader.read_exact(&mut key).unwrap();
            assert_eq!(key, expected_key, "Deleted key mismatch");

            let mut timestamp_buffer = [0; 16];
            reader.read_exact(&mut timestamp_buffer).unwrap();
            let timestamp = u128::from_le_bytes(timestamp_buffer);
            assert_eq!(timestamp, expected_timestamp, "Timestamp mismatch");
        } else {
            reader.read_exact(&mut buffer).unwrap();
            let value_size = usize::from_le_bytes(buffer);
            assert_eq!(
                value_size,
                expected_value.unwrap().len(),
                "Value size mismatch"
            );

            let mut key = vec![0; key_size];
            reader.read_exact(&mut key).unwrap();
            assert_eq!(key, expected_key, "Key mismatch for insertion");

            let mut value = vec![0; value_size];
            reader.read_exact(&mut value).unwrap();
            assert_eq!(
                value,
                expected_value.unwrap(),
                "Value mismatch for insertion"
            );

            let mut timestamp_buffer = [0; 16];
            reader.read_exact(&mut timestamp_buffer).unwrap();
            let timestamp = u128::from_le_bytes(timestamp_buffer);
            assert_eq!(timestamp, expected_timestamp, "Timestamp mismatch");
        }
    }

    #[test]
    fn test_single_write() {
        let mut rng = rand::thread_rng();
        let test_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir_all(&test_dir).unwrap();

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = WAL::create_new(&test_dir).unwrap();
        wal.record_insertion(b"Server", b"nginx", current_time)
            .unwrap();
        wal.flush().unwrap();

        let file = File::open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        validate_log_entry(&mut reader, b"Server", Some(b"nginx"), current_time, false);

        remove_dir_all(&test_dir).unwrap();
    }

    #[test]
    fn test_multiple_writes() {
        let mut rng = rand::thread_rng();
        let test_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir_all(&test_dir).unwrap();

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let entries: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"Server", Some(b"nginx")),
            (b"Database", Some(b"PostgreSQL")),
            (b"API", Some(b"GraphQL")),
        ];

        let mut wal = WAL::create_new(&test_dir).unwrap();

        for (key, value) in entries.iter() {
            wal.record_insertion(key, value.unwrap(), current_time)
                .unwrap();
        }
        wal.flush().unwrap();

        let file = File::open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        for (key, value) in entries.iter() {
            validate_log_entry(&mut reader, key, Some(value.unwrap()), current_time, false);
        }

        remove_dir_all(&test_dir).unwrap();
    }

    #[test]
    fn test_deletion() {
        let mut rng = rand::thread_rng();
        let test_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir_all(&test_dir).unwrap();

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = WAL::create_new(&test_dir).unwrap();
        wal.record_removal(b"Server", current_time).unwrap();
        wal.flush().unwrap();

        let file = File::open(&wal.path).unwrap();
        let mut reader = BufReader::new(file);

        validate_log_entry(&mut reader, b"Server", None, current_time, true);

        remove_dir_all(&test_dir).unwrap();
    }

    #[test]
    fn test_recover_empty_directory() {
        let mut rng = rand::thread_rng();
        let test_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir_all(&test_dir).unwrap();

        let (_new_wal, new_mem_table) = WAL::recover_from_directory(&test_dir).unwrap();
        assert_eq!(
            new_mem_table.current_size(),
            0,
            "Memory table should be empty"
        );

        remove_dir_all(&test_dir).unwrap();
    }

    #[test]
    fn test_recover_single_write() {
        let mut rng = rand::thread_rng();
        let test_dir = PathBuf::from(format!("./{}/", rng.gen::<u32>()));
        create_dir_all(&test_dir).unwrap();

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        let mut wal = WAL::create_new(&test_dir).unwrap();
        wal.record_insertion(b"Server", b"nginx", current_time)
            .unwrap();
        wal.flush().unwrap();

        let (new_wal, new_mem_table) = WAL::recover_from_directory(&test_dir).unwrap();

        let file = File::open(&new_wal.path).unwrap();
        let mut reader = BufReader::new(file);

        validate_log_entry(&mut reader, b"Server", Some(b"nginx"), current_time, false);

        let mem_entry = new_mem_table.fetch(b"Server").unwrap();
        assert_eq!(mem_entry.key, b"Server");
        assert_eq!(mem_entry.value.as_ref().unwrap().as_slice(), b"nginx");
        assert_eq!(mem_entry.timestamp, current_time);

        remove_dir_all(&test_dir).unwrap();
    }
}
