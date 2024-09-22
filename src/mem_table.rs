/// Represents an entry in the InMemoryTable.
pub struct InMemoryRecord {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub is_deleted: bool,
}

/* NOTE: A structure to hold the most recent written records, temporarily stored in memory.
   Entries in the InMemoryTable are kept in order to facilitate scans, and are
   moved to disk once the table reaches a predefined size limit.
*/

pub struct InMemoryTable {
    records: Vec<InMemoryRecord>,
    total_size: usize,
}

impl InMemoryTable {
    /// Initializes an empty InMemoryTable.
    pub fn new() -> InMemoryTable {
        InMemoryTable {
            records: Vec::new(),
            total_size: 0,
        }
    }

    /// Inserts or updates a key-value pair in the table.
    pub fn insert(&mut self, key: &[u8], value: &[u8], timestamp: u128) {
        let new_record = InMemoryRecord {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            timestamp,
            is_deleted: false,
        };

        match self.find_key_position(key) {
            Ok(index) => {
                if let Some(existing_value) = self.records[index].value.as_ref() {
                    if value.len() < existing_value.len() {
                        self.total_size -= existing_value.len() - value.len();
                    } else {
                        self.total_size += value.len() - existing_value.len();
                    }
                } else {
                    self.total_size += value.len();
                }
                self.records[index] = new_record;
            }
            Err(index) => {
                self.total_size += key.len() + value.len() + 17; // Key size + value size + 17 (timestamp + deletion flag)
                self.records.insert(index, new_record);
            }
        }
    }

    /// Marks a key as deleted in the InMemoryTable by using a tombstone.
    pub fn remove(&mut self, key: &[u8], timestamp: u128) {
        let tombstone_record = InMemoryRecord {
            key: key.to_vec(),
            value: None,
            timestamp,
            is_deleted: true,
        };

        match self.find_key_position(key) {
            Ok(index) => {
                if let Some(existing_value) = self.records[index].value.as_ref() {
                    self.total_size -= existing_value.len();
                }
                self.records[index] = tombstone_record;
            }
            Err(index) => {
                self.total_size += key.len() + 17; // Key size + timestamp + tombstone flag
                self.records.insert(index, tombstone_record);
            }
        }
    }

    /// Retrieves the value of a given key from the table.
    pub fn fetch(&self, key: &[u8]) -> Option<&InMemoryRecord> {
        self.find_key_position(key)
            .ok()
            .map(|idx| &self.records[idx])
    }

    /// Performs binary search to locate the index of the key or the insert position.
    fn find_key_position(&self, key: &[u8]) -> Result<usize, usize> {
        self.records
            .binary_search_by_key(&key, |record| record.key.as_slice())
    }

    /// Returns the number of records in the table.
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Returns all records stored in the table.
    pub fn all_records(&self) -> &[InMemoryRecord] {
        &self.records
    }

    /// Returns the total size of the data in memory.
    pub fn current_size(&self) -> usize {
        self.total_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_at_start() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);
        table.insert(b"SDK", b"Software Development Kit Guide", 10);
        table.insert(b"CLI", b"Command Line Interface Manual", 15);

        assert_eq!(table.records[0].key, b"API");
        assert_eq!(
            table.records[0].value.as_ref().unwrap(),
            b"REST API Documentation"
        );
        assert_eq!(table.records[0].timestamp, 5);
        assert!(!table.records[0].is_deleted);
        assert_eq!(table.current_size(), 141);
    }

    #[test]
    fn test_insert_in_middle() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);
        table.insert(b"CLI", b"Command Line Interface Manual", 15);
        table.insert(b"SDK", b"Software Development Kit Guide", 10);

        // After inserting, the expected order is: API, CLI, SDK
        assert_eq!(table.records[0].key, b"API");
        assert_eq!(table.records[1].key, b"CLI");
        assert_eq!(table.records[2].key, b"SDK");

        assert_eq!(table.current_size(), 141);
    }

    #[test]
    fn test_insert_at_end() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);
        table.insert(b"CLI", b"Command Line Interface Manual", 15);
        table.insert(b"SDK", b"Software Development Kit Guide", 10);

        // Check that the last inserted key is at the end of the records
        assert_eq!(table.records[2].key, b"SDK");
        assert_eq!(table.current_size(), 141);
    }

    #[test]
    fn test_overwrite_existing_entry() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);
        table.insert(b"API", b"Updated REST API Documentation", 10); // Overwriting

        let api_entry = table.fetch(b"API").unwrap();
        assert_eq!(api_entry.key, b"API");
        assert_eq!(
            api_entry.value.as_ref().unwrap(),
            b"Updated REST API Documentation"
        );
        assert_eq!(api_entry.timestamp, 10);
        assert!(!api_entry.is_deleted);
        assert_eq!(table.current_size(), 50);
    }

    #[test]
    fn test_fetch_existing_entry() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);

        let entry = table.fetch(b"API").unwrap();
        assert_eq!(entry.key, b"API");
        assert_eq!(entry.value.as_ref().unwrap(), b"REST API Documentation");
        assert_eq!(entry.timestamp, 5);
        assert!(!entry.is_deleted);
    }

    #[test]
    fn test_fetch_nonexistent_entry() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);

        let result = table.fetch(b"SDK"); // Non-existent
        assert!(result.is_none());
    }

    #[test]
    fn test_remove_existing_entry() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);
        table.remove(b"API", 10);

        let entry = table.fetch(b"API").unwrap();
        assert_eq!(entry.key, b"API");
        assert_eq!(entry.value, None); // Deleted, should be None
        assert_eq!(entry.timestamp, 10);
        assert!(entry.is_deleted);
        assert_eq!(table.current_size(), 20);
    }

    #[test]
    fn test_remove_nonexistent_entry() {
        let mut table = InMemoryTable::new();
        table.insert(b"API", b"REST API Documentation", 5);
        table.remove(b"SDK", 10); // Attempt to remove non-existent

        let entry = table.fetch(b"SDK").unwrap();
        assert_eq!(entry.key, b"SDK");
        assert_eq!(entry.value, None); // Should be tombstone
        assert_eq!(entry.timestamp, 10);
        assert!(entry.is_deleted);
        assert_eq!(table.current_size(), 62);
    }
}
