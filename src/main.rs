use std::time::Instant;
mod index;
mod table;

pub use crate::index::Index;
pub use crate::table::{Column, Table};

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
