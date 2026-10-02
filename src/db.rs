//! In-memory key-value storage will be implemented here.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

#[derive(Clone, Default)]
pub struct Db {
    entries: Arc<RwLock<HashMap<String, Entry>>>,
}

impl Db {
    pub fn set(&self, key: String, value: Vec<u8>) {
        let entry = Entry {
            value,
            expires_at: None,
        };
        self.entries
            .write()
            .expect("db lock poisoned")
            .insert(key, entry);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.entries
            .read()
            .expect("db lock poisoned")
            .get(key)
            //만료시에 값을 None으로 반환하기 위해 기존 map의 get() 결과를 and_then으로 처리
            .and_then(|entry| {
                if let Some(expires_at) = entry.expires_at {
                    if Instant::now() >= expires_at {
                        None
                    } else {
                        Some(entry.value.clone())
                    }
                } else {
                    Some(entry.value.clone())
                }
            })
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
        self.entries
            .write()
            .expect("db lock poisoned")
            .remove(key)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::{Db, Entry};
    use std::time::{Duration, Instant};

    // expire 메서드가 생기기 전까지 테스트에서 만료 시각을 직접 설정합니다.
    fn insert_with_expiration(db: &Db, key: &str, value: &[u8], expires_at: Instant) {
        db.entries.write().expect("db lock poisoned").insert(
            key.to_owned(),
            Entry {
                value: value.to_vec(),
                expires_at: Some(expires_at),
            },
        );
    }

    #[test]
    fn missing_key_returns_none() {
        let db = Db::default();
        assert_eq!(db.get("missing"), None);
    }

    #[test]
    fn reads_value_before_expiration() {
        let db = Db::default();
        let expires_at = Instant::now() + Duration::from_secs(3600);
        insert_with_expiration(&db, "language", b"rust", expires_at);

        assert_eq!(db.get("language"), Some(b"rust".to_vec()));
    }

    #[test]
    fn expired_value_returns_none() {
        let db = Db::default();
        let expires_at = Instant::now() - Duration::from_secs(1);
        insert_with_expiration(&db, "language", b"rust", expires_at);

        assert_eq!(db.get("language"), None);
    }

    #[test]
    fn set_overwrites_value_and_clears_expiration() {
        // 유효한 키와 이미 만료된 키 모두 일반 SET으로 덮어쓸 수 있습니다.
        let now = Instant::now();
        for expires_at in [
            now + Duration::from_secs(3600),
            now - Duration::from_secs(1),
        ] {
            let db = Db::default();
            insert_with_expiration(&db, "language", b"old", expires_at);

            db.set("language".to_owned(), b"rust".to_vec());

            assert_eq!(db.get("language"), Some(b"rust".to_vec()));
            let entries = db.entries.read().expect("db lock poisoned");
            assert!(entries.get("language").unwrap().expires_at.is_none());
        }
    }

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
