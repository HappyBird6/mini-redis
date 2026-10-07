//! In-memory key-value storage will be implemented here.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExpireError {
    InvalidTime, // 0 또는 음수
    OutOfRange,  // 시간 계산 범위 초과
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
                if entry.is_expired(Instant::now()) {
                    None
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
            .is_some_and(|entry| !entry.is_expired(Instant::now()))
    }

    pub fn purge_expired(&self) -> usize {
        let mut entries = self.entries.write().expect("db lock poisoned");
        let now = Instant::now();
        let before = entries.len();

        entries.retain(|_, entry| !entry.is_expired(now));
        before - entries.len()
    }

    pub fn set_expired_time(&self, key: &str, life_time: i64) -> Result<bool, ExpireError> {
        // 일단은 양수만 받게 하기
        if life_time <= 0 {
            return Err(ExpireError::InvalidTime);
        }

        // 쓰기락 획득
        let mut entries = self.entries.write().expect("db lock poisoned");

        // 종료시간 변환
        let now = Instant::now();
        let Some(expires_at) = now.checked_add(Duration::from_secs(life_time as u64)) else {
            return Err(ExpireError::OutOfRange);
        };

        // 수정가능한 get
        if let Some(entry) = entries.get_mut(key) {
            // 이미 만료됐으면? 리턴
            if entry.is_expired(now) {
                return Ok(false);
            }

            entry.expires_at = Some(expires_at);
            return Ok(true);
        }

        Ok(false)
    }

    pub fn ttl(&self, key: &str) -> i64 {
        let entries = self.entries.read().expect("db lock poisoned");
        let now = Instant::now();

        match entries.get(key) {
            None => -2,
            Some(entry) => {
                if let Some(expires_at) = entry.expires_at {
                    if entry.is_expired(now) {
                        -2
                    } else {
                        let remaining_ms = expires_at.duration_since(now).as_millis();
                        ((remaining_ms + 500) / 1000) as i64
                    }
                } else {
                    -1
                }
            }
        }
    }
}

// 실제 대기 없이 과거/미래 만료 상태를 구성하는 테스트 도우미입니다.
// executor 테스트에서도 쓰기 위해 크레이트 안에 공개합니다.
#[cfg(test)]
pub(crate) fn insert_with_expiration(db: &Db, key: &str, value: &[u8], expires_at: Instant) {
    db.entries.write().expect("db lock poisoned").insert(
        key.to_owned(),
        Entry {
            value: value.to_vec(),
            expires_at: Some(expires_at),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::{insert_with_expiration, Db, ExpireError};
    use std::time::{Duration, Instant};

    #[test]
    fn ttl_returns_minus_two_for_missing_and_expired_keys() {
        let db = Db::default();
        assert_eq!(db.ttl("missing"), -2);
        insert_with_expiration(
            &db,
            "expired",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );
        // 주기적 정리가 실행되기 전에도 만료된 키는 없는 키로 취급합니다.
        assert_eq!(db.ttl("expired"), -2);
        assert_eq!(db.purge_expired(), 1);
        assert_eq!(db.ttl("expired"), -2);
    }

    #[test]
    fn ttl_returns_minus_one_without_expiration_and_after_overwrite() {
        let db = Db::default();
        db.set("key".to_owned(), b"old".to_vec());
        assert_eq!(db.ttl("key"), -1);
        assert_eq!(db.set_expired_time("key", 3600), Ok(true));
        db.set("key".to_owned(), b"new".to_vec());
        assert_eq!(db.ttl("key"), -1);
        assert_eq!(db.get("key"), Some(b"new".to_vec()));
    }

    #[test]
    fn ttl_reads_configured_deadline_without_changing_entry() {
        let db = Db::default();
        db.set("key".to_owned(), b"keep".to_vec());
        assert_eq!(db.set_expired_time("key", 3600), Ok(true));
        let deadline = db.entries.read().unwrap()["key"].expires_at.unwrap();

        // 실제 호출은 두 관측 시점 사이에서 실행되므로 해당 범위에 있어야 합니다.
        let before = Instant::now();
        let actual = db.ttl("key");
        let after = Instant::now();
        let entries = db.entries.read().unwrap();
        let entry = &entries["key"];
        let upper = deadline.saturating_duration_since(before).as_secs() as i64 + 1;
        let lower = deadline.saturating_duration_since(after).as_secs() as i64;
        assert!(actual <= upper);
        assert!(actual >= lower);
        assert!(actual >= 0);
        assert_eq!(entry.value, b"keep");
        assert_eq!(entry.expires_at, Some(deadline));
    }

    #[test]
    fn purge_expired_removes_only_expired_entries() {
        let db = Db::default();
        let now = Instant::now();
        insert_with_expiration(&db, "expired_a", b"old", now - Duration::from_secs(2));
        insert_with_expiration(&db, "expired_b", b"old", now - Duration::from_secs(1));
        insert_with_expiration(&db, "future", b"keep", now + Duration::from_secs(3600));
        db.set("permanent".to_owned(), b"keep forever".to_vec());

        assert_eq!(db.purge_expired(), 2);

        // get()은 삭제 전에도 None을 반환하므로 실제 맵에서 제거됐는지 확인합니다.
        {
            let entries = db.entries.read().expect("db lock poisoned");
            assert!(!entries.contains_key("expired_a"));
            assert!(!entries.contains_key("expired_b"));
            assert_eq!(entries.len(), 2);
        } // 다음 purge 호출 전에 읽기 락을 해제합니다.

        assert_eq!(db.get("future"), Some(b"keep".to_vec()));
        assert_eq!(db.get("permanent"), Some(b"keep forever".to_vec()));
        assert_eq!(db.purge_expired(), 0);
    }

    #[test]
    fn purge_expired_on_empty_db_returns_zero() {
        let db = Db::default();
        assert_eq!(db.purge_expired(), 0);
        assert!(db.entries.read().expect("db lock poisoned").is_empty());
    }

    #[test]
    fn purge_expired_preserves_value_overwritten_by_set() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "language",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );
        db.set("language".to_owned(), b"rust".to_vec());

        assert_eq!(db.purge_expired(), 0);
        assert_eq!(db.get("language"), Some(b"rust".to_vec()));
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
    fn set_expired_time_hides_value_after_expiration() {
        let db = Db::default();
        db.set("language".to_owned(), b"rust".to_vec());
        assert_eq!(db.get("language"), Some(b"rust".to_vec()));

        assert_eq!(db.set_expired_time("language", 1), Ok(true));
        let expires_at = db.entries.read().unwrap()["language"].expires_at.unwrap();

        // DB가 std::time::Instant를 사용하므로 실제 시간을 기다립니다.
        // 대기하는 동안에는 위 블록의 쓰기 락이 해제된 상태입니다.
        std::thread::sleep(
            expires_at.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
        );

        // purge를 호출하지 않아도 GET 자체가 만료된 값을 숨겨야 합니다.
        assert_eq!(db.get("language"), None);
        assert!(db
            .entries
            .read()
            .expect("db lock poisoned")
            .contains_key("language"));
    }

    #[test]
    fn set_expired_time_sets_and_replaces_deadline_without_changing_value() {
        let db = Db::default();
        db.set("language".to_owned(), b"rust".to_vec());
        db.set("other".to_owned(), b"keep".to_vec());

        // 최초 설정 후 기존 만료 시간을 줄이거나 늘리는 경우를 확인합니다.
        for seconds in [3600, 1800, 7200] {
            let before = Instant::now();
            assert_eq!(db.set_expired_time("language", seconds), Ok(true));
            let after = Instant::now();
            let entries = db.entries.read().unwrap();
            let deadline = entries["language"].expires_at.unwrap();
            let duration = Duration::from_secs(seconds as u64);
            assert!(deadline >= before + duration);
            assert!(deadline <= after + duration);
            assert_eq!(entries["language"].value, b"rust");
            assert_eq!(entries["other"].value, b"keep");
            assert_eq!(entries["other"].expires_at, None);
        }
    }

    #[test]
    fn set_expired_time_does_not_create_missing_key() {
        let db = Db::default();
        assert_eq!(db.set_expired_time("missing", 10), Ok(false));
        assert!(db.entries.read().unwrap().is_empty());
    }

    #[test]
    fn set_expired_time_does_not_revive_expired_key() {
        let db = Db::default();
        let deadline = Instant::now() - Duration::from_secs(1);
        insert_with_expiration(&db, "expired", b"old", deadline);

        assert_eq!(db.set_expired_time("expired", 3600), Ok(false));
        assert_eq!(db.get("expired"), None);
        assert_eq!(
            db.entries.read().unwrap()["expired"].expires_at,
            Some(deadline)
        );
    }

    #[test]
    fn set_expired_time_rejects_nonpositive_time_without_mutation() {
        let db = Db::default();
        db.set("permanent".to_owned(), b"keep".to_vec());
        let deadline = Instant::now() + Duration::from_secs(3600);
        insert_with_expiration(&db, "temporary", b"keep", deadline);

        for seconds in [0, -1, i64::MIN] {
            for key in ["permanent", "temporary", "missing"] {
                assert_eq!(
                    db.set_expired_time(key, seconds),
                    Err(ExpireError::InvalidTime)
                );
            }
            let entries = db.entries.read().unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries["permanent"].expires_at, None);
            assert_eq!(entries["temporary"].expires_at, Some(deadline));
            assert_eq!(entries["permanent"].value, b"keep");
            assert_eq!(entries["temporary"].value, b"keep");
        }
    }

    #[test]
    fn set_expired_time_handles_platform_time_limit_without_losing_value() {
        let db = Db::default();
        let original = Instant::now() + Duration::from_secs(3600);
        insert_with_expiration(&db, "key", b"keep", original);

        // Instant의 표현 범위는 플랫폼마다 다르므로 최대 i64 초의 지원 여부를 확인합니다.
        let duration = Duration::from_secs(i64::MAX as u64);
        let before = Instant::now().checked_add(duration);
        let result = db.set_expired_time("key", i64::MAX);
        let after = Instant::now().checked_add(duration);
        let entries = db.entries.read().unwrap();
        assert_eq!(entries["key"].value, b"keep");
        match result {
            Err(ExpireError::OutOfRange) => {
                assert!(after.is_none());
                assert_eq!(entries["key"].expires_at, Some(original));
            }
            Ok(true) => {
                let deadline = entries["key"].expires_at.unwrap();
                assert!(deadline >= before.expect("successful addition must have a lower bound"));
                if let Some(upper) = after {
                    assert!(deadline <= upper);
                }
            }
            other => panic!("unexpected expiration result: {other:?}"),
        }
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

    #[test]
    fn delete_treats_unpurged_expired_key_as_missing() {
        let db = Db::default();
        let now = Instant::now();
        insert_with_expiration(&db, "expired", b"old", now - Duration::from_secs(1));
        insert_with_expiration(&db, "future", b"keep", now + Duration::from_secs(3600));

        // purge 전이라 맵에는 남아 있지만 논리적으로는 없는 키이므로 false입니다.
        assert!(!db.delete("expired"));
        assert!(!db
            .entries
            .read()
            .expect("db lock poisoned")
            .contains_key("expired"));

        // 아직 만료되지 않은 키는 기존처럼 삭제에 성공합니다.
        assert!(db.delete("future"));
        assert!(db.entries.read().expect("db lock poisoned").is_empty());
    }
}
