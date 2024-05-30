use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Read, Write};

pub use crate::index::Index;

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub dtype: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<String>>,
}

impl Table {
    pub fn insert(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }
    pub fn select(&self, column_name: &str, value: &str) -> Vec<&Vec<String>> {
        let column_index = self
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .unwrap();
        self.rows
            .iter()
            .filter(|row| row[column_index] == value)
            .collect()
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
    pub fn create_index(&self, column_name: &str) -> Index {
        let column_index = self
            .columns
            .iter()
            .position(|c| c.name == column_name)
            .expect("Column not found");
        let mut index = Index::new(column_name);

        for (i, row) in self.rows.iter().enumerate() {
            index.add_entry(&row[column_index], i);
        }

        index
    }
    pub fn select_with_index(&self, index: &Index, value: &str) -> Vec<&Vec<String>> {
        if let Some(rows) = index.index.get(value) {
            rows.iter().map(|&i| &self.rows[i]).collect()
        } else {
            vec![]
        }
    }
}
