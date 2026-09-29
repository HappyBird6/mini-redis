//! In-memory key-value storage will be implemented here.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone, Default)]
pub struct Db {
    entries: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl Db {
    pub fn set(&self, key: String, value: Vec<u8>) {
        self.entries.write().expect("db lock poisoned").insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.entries.read().expect("db lock poisoned").get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::Db;

    #[test]
    fn stores_and_reads_a_value() {
        let db = Db::default();
        db.set("language".to_owned(), b"rust".to_vec());
        assert_eq!(db.get("language"), Some(b"rust".to_vec()));
    }
}
