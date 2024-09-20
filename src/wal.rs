use crate::mem_table::InMemoryTable;
use crate::utils::files_with_ext;
use crate::wal_iterator::LogRecord;
use crate::wal_iterator::LogFileIterator;
use std::fs::{remove_file, File, OpenOptions};
use std::io::prelude::*;
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Write Ahead Log(WAL)
///
/// An append-only file that holds the operations performed on the MemTable.
/// The WAL is intended for recovery of the MemTable when the server is shutdown.
pub struct WAL {
  path: PathBuf,
  file: BufWriter<File>,
}

impl WAL {
  /// Creates a new WAL in a given directory.
  pub fn new(dir: &Path) -> io::Result<WAL> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let path = Path::new(dir).join(timestamp.to_string() + ".wal");
    let file = OpenOptions::new().append(true).create(true).open(&path)?;
    let file = BufWriter::new(file);

    Ok(WAL { path, file })
  }

  /// Creates a WAL from an existing file path.
  pub fn from_path(path: &Path) -> io::Result<WAL> {
    let file = OpenOptions::new().append(true).create(true).open(path)?;
    let file = BufWriter::new(file);

    Ok(WAL {
      path: path.to_owned(),
      file,
    })
  }

  /// Loads the WAL(s) within a directory, returning a new WAL and the recovered MemTable.
  ///
  /// If multiple WALs exist in a directory, they are merged by file date.
  pub fn load_from_dir(dir: &Path) -> io::Result<(WAL, InMemoryTable)> {
    let mut wal_files = files_with_ext(dir, "wal");
    wal_files.sort();

    let mut new_mem_table = InMemoryTable::new();
    let mut new_wal = WAL::new(dir)?;
    for wal_file in wal_files.iter() {
      if let Ok(wal) = WAL::from_path(wal_file) {
        for entry in wal.into_iter() {
          if entry.is_removed {
            new_mem_table.remove(&entry.identifier.as_slice(), entry.event_time);
            new_wal.delete(&entry.identifier.as_slice(), entry.event_time)?;
          } else {
            new_mem_table.insert(
              &entry.identifier.as_slice(),
              entry.data.as_ref().unwrap().as_slice(),
              entry.event_time,
            );
            new_wal.set(
              &entry.identifier.as_slice(),
              entry.data.unwrap().as_slice(),
              entry.event_time,
            )?;
          }
        }
      }
    }
    new_wal.flush().unwrap();
    wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

    Ok((new_wal, new_mem_table))
  }

  /// Sets a Key-Value pair and the operation is appended to the WAL.
  pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
    self.file.write_all(&key.len().to_le_bytes())?;
    self.file.write_all(&(false as u8).to_le_bytes())?;
    self.file.write_all(&value.len().to_le_bytes())?;
    self.file.write_all(key)?;
    self.file.write_all(value)?;
    self.file.write_all(&timestamp.to_le_bytes())?;

    Ok(())
  }

  /// Deletes a Key-Value pair and the operation is appended to the WAL.
  ///
  /// This is achieved using tombstones.
  pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
    self.file.write_all(&key.len().to_le_bytes())?;
    self.file.write_all(&(true as u8).to_le_bytes())?;
    self.file.write_all(key)?;
    self.file.write_all(&timestamp.to_le_bytes())?;

    Ok(())
  }

  /// Flushes the WAL to disk.
  ///
  /// This is useful for applying bulk operations and flushing the final result to
  /// disk. Waiting to flush after the bulk operations have been performed will improve
  /// write performance substantially.
  pub fn flush(&mut self) -> io::Result<()> {
    self.file.flush()
  }
}

impl IntoIterator for WAL {
  type IntoIter = LogFileIterator;
  type Item = LogRecord;

  /// Converts a WAL into a `WALIterator` to iterate over the entries.
  fn into_iter(self) -> LogFileIterator {
    LogFileIterator::from_path(self.path).unwrap()
  }
}