use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Index {
    pub column_name: String,
    pub index: BTreeMap<String, Vec<usize>>, // 値 -> 行インデックスのマッピング
}

impl Index {
    pub fn new(column_name: &str) -> Self {
        Index {
            column_name: column_name.to_string(),
            index: BTreeMap::new(),
        }
    }

    pub fn add_entry(&mut self, value: &str, row_index: usize) {
        self.index
            .entry(value.to_string())
            .or_insert(vec![])
            .push(row_index);
    }
    pub fn save_to_file(&self, filename: &str) {
        let serialized = serde_json::to_string(self).unwrap();
        let mut file = File::create(filename).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
    }

    pub fn load_from_file(filename: &str) -> Self {
        let mut file = File::open(filename).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        serde_json::from_str(&contents).unwrap()
    }
}
