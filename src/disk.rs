use crate::mem_table::InMemoryTable;
use crate::wal::WAL;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct DiskEntry {
  key: Vec<u8>,
  value: Vec<u8>,
  timestamp: u128,
}

impl DiskEntry {
  pub fn key(&self) -> &[u8] {
    &self.key
  }

  pub fn value(&self) -> &[u8] {
    &self.value
  }

  pub fn timestamp(&self) -> u128 {
    self.timestamp
  }
}

pub struct Disk {
  mem_table: InMemoryTable,
  wal: WAL,
}

impl Disk {
  pub fn new(dir: &str) -> Disk {
    let dir = PathBuf::from(dir);

    let (wal, mem_table) = WAL::recover_from_directory(&dir).unwrap();

    Disk {
      mem_table,
      wal,
    }
  }

  pub fn get(&self, key: &[u8]) -> Option<DiskEntry> {
    if let Some(mem_entry) = self.mem_table.fetch(key) {
      return Some(DiskEntry {
        key: mem_entry.key.clone(),
        value: mem_entry.value.as_ref().unwrap().clone(),
        timestamp: mem_entry.timestamp,
      });
    }

    None
  }

  pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<usize, usize> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let wal_res = self.wal.record_insertion(key, value, timestamp);
    if wal_res.is_err() {
      return Err(0);
    }
    if self.wal.flush().is_err() {
      return Err(0);
    }

    self.mem_table.insert(key, value, timestamp);

    Ok(1)
  }

  pub fn delete(&mut self, key: &[u8]) -> Result<usize, usize> {
    let timestamp = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap()
      .as_micros();

    let wal_res = self.wal.record_removal(key, timestamp);
    if wal_res.is_err() {
      return Err(0);
    }
    if self.wal.flush().is_err() {
      return Err(0);
    }

    self.mem_table.remove(key, timestamp);

    Ok(1)
  }
}
