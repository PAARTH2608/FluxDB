![image](https://github.com/user-attachments/assets/5605b2ea-4696-4cc2-ba7b-98cbd63547c3)

# Flux-DB

🚀 **Flux-DB** is a minimalist NoSQL database built in Rust, leveraging the **Log-Structured Merge Tree (LSM)** algorithm for efficient data storage and high write performance. This project is an educational and experimental implementation that demonstrates how LSM trees work under the hood.

## Features

- **Log-Structured Merge Tree (LSM)**: Uses an efficient write-optimized data structure, designed to handle high volumes of sequential writes.
- **In-Memory Table**: Temporarily stores recent data before flushing it to disk.
- **Write-Ahead Logging (WAL)**: Ensures data durability and crash recovery.
- **Minimalist Design**: Focuses on simplicity while learning about database internals.
- **Open Source**: Contributions and feedback are welcome!

## Installation

To get started with Flux-DB, clone the repository and build the project with Cargo:

```bash
git clone https://github.com/PAARTH2608/FluxDB.git
cd FluxDB
cargo build
```

## Usage
After building the project, you can run the database and interact with it via the Rust API. Example usage:

```bash
use flux_db::{WAL, InMemoryTable};
use std::path::Path;

fn main() {
    let wal = WAL::create_new(Path::new("data/fluxdb")).unwrap();
    let mut mem_table = InMemoryTable::new();

    // Insert key-value pairs
    mem_table.insert(b"key1", b"value1", 12345);
    mem_table.insert(b"key2", b"value2", 12346);

    // Fetch a key
    let record = mem_table.fetch(b"key1");
    println!("{:?}", record);
}
```

## Blog
For a detailed explanation of the LSM tree algorithm and how it powers Flux-DB, check out my blog post:

👉 [Blog Link](https://medium.com/@paarth.jain/flux-db-a-minimalist-nosql-database-in-rust-using-lsm-trees-f3d5f78f2904)

## Contribution
Contributions are welcome! If you want to contribute:
</br>
1. Fork the repository</br>
2. Create a new branch (git checkout -b feature-branch) </br>
3. Make your changes and commit (git commit -m 'Add some feature') </br>
4. Push to the branch (git push origin feature-branch) </br>
5. Open a pull request </br>
