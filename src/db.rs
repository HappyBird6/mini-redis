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

    /*
    write() 함수로 쓰기 락 획득(delete는 값을 변화시키는거라 쓰기락을 얻어야 됨)
    remove() 
        성공 -> Some(값) 반환 
        실패 -> None 반환
    is_some으로 bool로 변환
    세미콜론 X -> 표현식이 바로 반환됨
    */
    pub fn delete(&self, key: &str) -> bool {
        self.entries.write().expect("db lock poisoned").remove(key).is_some()
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

    #[test]
    fn deletes_value() {
        let db = Db::default();
        db.set("language".to_owned(), b"rust".to_vec());

        assert!(db.delete("language"));
        assert_eq!(db.get("language"), None);
        assert!(!db.delete("language"));
    }
}
