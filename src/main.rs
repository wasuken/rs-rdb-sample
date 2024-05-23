use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Index {
    column_name: String,
    index: BTreeMap<String, Vec<usize>>, // 値 -> 行インデックスのマッピング
}

impl Index {
    fn new(column_name: &str) -> Self {
        Index {
            column_name: column_name.to_string(),
            index: BTreeMap::new(),
        }
    }

    fn add_entry(&mut self, value: &str, row_index: usize) {
        self.index
            .entry(value.to_string())
            .or_insert(vec![])
            .push(row_index);
    }
    fn save_to_file(&self, filename: &str) {
        let serialized = serde_json::to_string(self).unwrap();
        let mut file = File::create(filename).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
    }

    fn load_from_file(filename: &str) -> Self {
        let mut file = File::open(filename).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        serde_json::from_str(&contents).unwrap()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    dtype: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Table {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Vec<String>>,
}

impl Table {
    fn insert(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }
    fn select(&self, column_name: &str, value: &str) -> Vec<&Vec<String>> {
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
    fn save_to_file(&self, filename: &str) {
        let serialized = serde_json::to_string(self).unwrap();
        let mut file = File::create(filename).unwrap();
        file.write_all(serialized.as_bytes()).unwrap();
    }

    fn load_from_file(filename: &str) -> Self {
        let mut file = File::open(filename).unwrap();
        let mut contents = String::new();
        file.read_to_string(&mut contents).unwrap();
        serde_json::from_str(&contents).unwrap()
    }
    fn create_index(&self, column_name: &str) -> Index {
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
    fn select_with_index(&self, index: &Index, value: &str) -> Vec<&Vec<String>> {
        if let Some(rows) = index.index.get(value) {
            rows.iter().map(|&i| &self.rows[i]).collect()
        } else {
            vec![]
        }
    }
}

fn main() {
    let columns = vec![
        Column {
            name: "id".to_string(),
            dtype: "int".to_string(),
        },
        Column {
            name: "name".to_string(),
            dtype: "string".to_string(),
        },
    ];

    let mut table = Table {
        name: "users".to_string(),
        columns: columns,
        rows: vec![],
    };

    for i in 0..1000000 {
        table.insert(vec![i.to_string(), format!("test {i}").to_string()]);
    }
    table.insert(vec!["ZZZ".to_string(), "Alice".to_string()]);
    table.save_to_file("table.json");
    let index = table.create_index("name");
    index.save_to_file("name_index.json");

    // インデックスの読み込み
    let loaded_index = Index::load_from_file("name_index.json");

    {
        println!("インデックス有");
        let start = Instant::now();
        let results = table.select_with_index(&loaded_index, "Alice");
        println!("{:?}", results);
        let duration = start.elapsed();

        println!("Insert operation took: {:?}", duration.as_micros());
    }
    {
        println!("インデックス無");
        let start = Instant::now();
        let results = table.select("name", "Alice");
        println!("{:?}", results);
        let duration = start.elapsed();

        println!("Insert operation took: {:?}", duration.as_micros());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert() {
        let mut table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    dtype: "int".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    dtype: "string".to_string(),
                },
            ],
            rows: vec![],
        };

        table.insert(vec!["1".to_string(), "Alice".to_string()]);
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0], vec!["1".to_string(), "Alice".to_string()]);
    }

    #[test]
    fn test_select() {
        let table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    dtype: "int".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    dtype: "string".to_string(),
                },
            ],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
        };

        let results = table.select("name", "Alice");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &vec!["1".to_string(), "Alice".to_string()]);
    }

    #[test]
    fn test_create_index() {
        let table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    dtype: "int".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    dtype: "string".to_string(),
                },
            ],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
        };

        let index = table.create_index("name");
        assert!(index.index.contains_key("Alice"));
        assert_eq!(index.index["Alice"], vec![0]);
    }

    #[test]
    fn test_select_with_index() {
        let table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    dtype: "int".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    dtype: "string".to_string(),
                },
            ],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
        };

        let index = table.create_index("name");
        let results = table.select_with_index(&index, "Alice");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], &vec!["1".to_string(), "Alice".to_string()]);
    }

    #[test]
    fn test_save_and_load_index() {
        let table = Table {
            name: "users".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    dtype: "int".to_string(),
                },
                Column {
                    name: "name".to_string(),
                    dtype: "string".to_string(),
                },
            ],
            rows: vec![
                vec!["1".to_string(), "Alice".to_string()],
                vec!["2".to_string(), "Bob".to_string()],
            ],
        };

        let index = table.create_index("name");
        index.save_to_file("name_index.json");

        let loaded_index = Index::load_from_file("name_index.json");
        assert_eq!(index, loaded_index);
    }
}
