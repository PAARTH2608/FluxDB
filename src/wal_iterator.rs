use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read};
use std::path::PathBuf;

/// Represents an individual record in the Write-Ahead Log.
pub struct LogRecord {
    pub identifier: Vec<u8>,            // Key for identifying the record
    pub data: Option<Vec<u8>>,          // Value for the record (if not deleted)
    pub event_time: u128,               // Timestamp for tracking when the record was created or updated
    pub is_removed: bool,               // Flag indicating if the record has been deleted
}

/// Struct responsible for iterating through entries in a WAL (Write-Ahead Log) file.
pub struct LogFileIterator {
    file_reader: BufReader<File>,       // Buffer for reading from the WAL file
}

impl LogFileIterator {
    /// Constructs a new iterator for traversing the WAL file, given a path to the file.
    pub fn from_path(filepath: PathBuf) -> io::Result<LogFileIterator> {
        let wal_file = OpenOptions::new().read(true).open(filepath)?;
        let buffered_reader = BufReader::new(wal_file);
        Ok(LogFileIterator { file_reader: buffered_reader })
    }
}

impl Iterator for LogFileIterator {
    type Item = LogRecord;

    /// Advances the iterator, retrieving the next record in the WAL file if available.
    fn next(&mut self) -> Option<LogRecord> {
        let mut key_length_buffer = [0; 8];
        if self.file_reader.read_exact(&mut key_length_buffer).is_err() {
            return None;
        }
        let key_length = usize::from_le_bytes(key_length_buffer);

        let mut deletion_flag_buffer = [0; 1];
        if self.file_reader.read_exact(&mut deletion_flag_buffer).is_err() {
            return None;
        }
        let is_deleted = deletion_flag_buffer[0] != 0;

        let mut identifier = vec![0; key_length];
        let mut data = None;
        if is_deleted {
            if self.file_reader.read_exact(&mut identifier).is_err() {
                return None;
            }
        } else {
            if self.file_reader.read_exact(&mut key_length_buffer).is_err() {
                return None;
            }
            let value_length = usize::from_le_bytes(key_length_buffer);
            if self.file_reader.read_exact(&mut identifier).is_err() {
                return None;
            }
            let mut value_buffer = vec![0; value_length];
            if self.file_reader.read_exact(&mut value_buffer).is_err() {
                return None;
            }
            data = Some(value_buffer);
        }

        let mut timestamp_buf = [0; 16];
        if self.file_reader.read_exact(&mut timestamp_buf).is_err() {
            return None;
        }
        let event_time = u128::from_le_bytes(timestamp_buf);

        Some(LogRecord {
            identifier,
            data,
            event_time,
            is_removed: is_deleted,
        })
    }
}
