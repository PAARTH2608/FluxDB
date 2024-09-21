use std::fs::read_dir;
use std::path::{Path, PathBuf};

/// Gets the set of files with an extension for a given directory.
pub fn find_files_with_extension(dir: &Path, ext: &str) -> Vec<PathBuf> {
  let mut files = Vec::new();
  for file in read_dir(dir).unwrap() {
    let path = file.unwrap().path();
    if path.extension().unwrap() == ext {
      files.push(path);
    }
  }

  files
}
