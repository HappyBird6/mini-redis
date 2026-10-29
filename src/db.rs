//! In-memory key-value storage will be implemented here.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
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

#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>, //ttl 설정된 키들
}

#[derive(Clone, Default)]
pub struct Db {
    state: Arc<RwLock<State>>,
}

impl Db {
    fn remove_if_expired(state: &mut State, key: &str, now: Instant) -> bool {
        let expired = state
            .entries
            .get(key)
            .is_some_and(|entry| entry.is_expired(now));
        if expired {
            if let Some(at) = state.entries.remove(key).and_then(|entry| entry.expires_at) {
                state.expirations.remove(&(at, key.to_owned()));
            }
        }
        expired
    }

    fn lock_and_remove_if_expired(&self, key: &str) {
        let mut state = self.state.write().expect("db lock poisoned");
        Self::remove_if_expired(&mut state, key, Instant::now());
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        let entry = Entry {
            value,
            expires_at: None,
        };
        let mut state = self.state.write().expect("db lock poisoned");
        if let Some(at) = state
            .entries
            .insert(key.clone(), entry)
            .and_then(|e| e.expires_at)
        {
            state.expirations.remove(&(at, key));
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let expired = {
            let state = self.state.read().expect("db lock poisoned");
            match state.entries.get(key) {
                None => return None,
                Some(entry) => {
                    if !entry.is_expired(Instant::now()) {
                        return Some(entry.value.clone());
                    }
                    true
                }
            }
        };

        if expired {
            self.lock_and_remove_if_expired(key);
        }
        None
    }

    /*
    이미 쓰기 락을 잡은 State에서 키 하나를 지우는 내부 도우미 (락은 호출하는 쪽이 잡음)
    State가 비공개 타입이라 pub 없이 db 모듈 안에서만 사용
    remove()
        성공 -> Some(값) 반환
        실패 -> None 반환
    지운 entry에 만료 시각이 있으면 expirations에서 (시각, 키) 제거
    is_some_and로 now 기준 만료 전이었는지 확인해 bool로 변환 (만료된 키는 지워도 false)
    now를 인자로 받아 같은 DEL 안의 키들을 같은 시각 기준으로 판정
    세미콜론 X -> 표현식이 바로 반환됨
    */
    fn delete_with_locked_state(state: &mut State, key: &str, now: Instant) -> bool {
        let removed = state.entries.remove(key);

        // and_then은 self를 값으로 받아서 Option을 소비
        // 지금처럼 removed에 같은 option을 두번이상 써야하면 as_ref로 빌려와서 마지막에만 소비
        if let Some(at) = removed.as_ref().and_then(|entry| entry.expires_at) {
            state.expirations.remove(&(at, key.to_owned()));
        }

        removed.is_some_and(|entry| !entry.is_expired(now))
    }

    /*
    DEL k1 k2 ... 처리: 여러 키를 지우고 실제로 지운(만료 전이던) 키 개수를 반환
    쓰기 락을 한 번만 잡고 모든 키를 지움
        키마다 락을 잡았다 놓으면 그 사이에 다른 요청이 끼어들어
        "k1만 지워지고 k2는 남은" 중간 상태를 읽을 수 있음
        Redis처럼 명령 하나는 다 끝난 상태로만 보이게 함
    같은 키가 두 번 오면 두 번째는 이미 없는 키라 한 번만 셈 (DEL a a -> 1)
    반환은 개수라 usize, RESP 정수(i64) 변환은 executor에서 함
    */
    pub fn delete(&self, keys: &[String]) -> usize {
        let mut state = self.state.write().expect("db lock poisoned");
        let mut deleted: usize = 0;
        let now = Instant::now();
        for key in keys {
            let success = Self::delete_with_locked_state(&mut state, key, now);
            if success {
                deleted += 1;
            }
        }
        deleted
    }

    pub fn purge_expired(&self) -> usize {
        let mut state = self.state.write().expect("db lock poisoned");
        let now = Instant::now();

        let State {
            entries,
            expirations,
        } = &mut *state;
        let before = entries.len();
        while let Some((at, _)) = expirations.first() {
            if *at > now {
                break;
            }
            let (_, key) = expirations.pop_first().unwrap();
            entries.remove(&key);
        }
        before - entries.len()
    }

    pub fn set_expired_time(&self, key: &str, life_time: i64) -> Result<bool, ExpireError> {
        // 일단은 양수만 받게 하기
        if life_time <= 0 {
            return Err(ExpireError::InvalidTime);
        }

        // 쓰기락 획득
        let mut state = self.state.write().expect("db lock poisoned");

        // 종료시간 변환
        let now = Instant::now();
        let Some(expires_at) = now.checked_add(Duration::from_secs(life_time as u64)) else {
            return Err(ExpireError::OutOfRange);
        };

        let expired = Self::remove_if_expired(&mut state, key, now);
        if expired {
            return Ok(false);
        }

        // 수정가능한 get
        if let Some(entry) = state.entries.get_mut(key) {
            let old = entry.expires_at.replace(expires_at);
            if let Some(old_at) = old {
                state.expirations.remove(&(old_at, key.to_owned()));
            }
            state.expirations.insert((expires_at, key.to_owned()));
            return Ok(true);
        }

        Ok(false)
    }

    pub fn ttl(&self, key: &str) -> i64 {
        let expired = {
            let state = self.state.read().expect("db lock poisoned");
            let now = Instant::now();

            match state.entries.get(key) {
                None => return -2,
                Some(entry) => match entry.expires_at {
                    None => return -1,
                    Some(expires_at) => {
                        if !entry.is_expired(now) {
                            let remaining_ms = expires_at.duration_since(now).as_millis();
                            return ((remaining_ms + 500) / 1000) as i64;
                        }
                        true
                    }
                },
            }
        };

        if expired {
            self.lock_and_remove_if_expired(key);
        }
        -2
    }
}

// 실제 대기 없이 과거/미래 만료 상태를 구성하는 테스트 도우미입니다.
// executor 테스트에서도 쓰기 위해 크레이트 안에 공개합니다.
#[cfg(test)]
pub(crate) fn insert_with_expiration(db: &Db, key: &str, value: &[u8], expires_at: Instant) {
    let mut state = db.state.write().expect("db lock poisoned");
    let previous = state.entries.insert(
        key.to_owned(),
        Entry {
            value: value.to_vec(),
            expires_at: Some(expires_at),
        },
    );
    // 같은 키를 덮어쓰면 이전 만료 시각의 인덱스 항목을 먼저 지웁니다.
    if let Some(old_at) = previous.and_then(|entry| entry.expires_at) {
        state.expirations.remove(&(old_at, key.to_owned()));
    }
    // 실제 EXPIRE처럼 만료 인덱스에도 등록해야 purge 대상이 됩니다.
    state.expirations.insert((expires_at, key.to_owned()));
}

#[cfg(test)]
mod tests {
    use super::{insert_with_expiration, Db, ExpireError};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
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
        // 접근하는 순간 만료된 키가 삭제되므로 정리할 키가 남아 있지 않습니다.
        assert_eq!(db.purge_expired(), 0);
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
        let deadline = db.state.read().unwrap().entries["key"].expires_at.unwrap();

        // 실제 호출은 두 관측 시점 사이에서 실행되므로 해당 범위에 있어야 합니다.
        let before = Instant::now();
        let actual = db.ttl("key");
        let after = Instant::now();
        let state = db.state.read().unwrap();
        let entries = &state.entries;
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
            let state = db.state.read().expect("db lock poisoned");
            let entries = &state.entries;
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
        assert!(db
            .state
            .read()
            .expect("db lock poisoned")
            .entries
            .is_empty());
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
        let expires_at = db.state.read().unwrap().entries["language"]
            .expires_at
            .unwrap();

        // DB가 std::time::Instant를 사용하므로 실제 시간을 기다립니다.
        // 대기하는 동안에는 위 블록의 쓰기 락이 해제된 상태입니다.
        std::thread::sleep(
            expires_at.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
        );

        // purge를 호출하지 않아도 GET 자체가 만료된 값을 숨겨야 합니다.
        assert_eq!(db.get("language"), None);
        // 만료된 키는 읽는 순간 삭제됩니다.
        assert!(!db
            .state
            .read()
            .expect("db lock poisoned")
            .entries
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
            let state = db.state.read().unwrap();
            let entries = &state.entries;
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
        assert!(db.state.read().unwrap().entries.is_empty());
    }

    #[test]
    fn set_expired_time_removes_expired_key_without_reviving_it() {
        let db = Db::default();
        let deadline = Instant::now() - Duration::from_secs(1);
        insert_with_expiration(&db, "expired", b"old", deadline);

        assert_eq!(db.set_expired_time("expired", 3600), Ok(false));
        assert_eq!(db.get("expired"), None);
        // 되살리지 않고 삭제만 합니다.
        assert!(!db.state.read().unwrap().entries.contains_key("expired"));
    }

    #[test]
    fn get_removes_expired_key_on_access() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "expired",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );

        assert_eq!(db.get("expired"), None);
        assert!(!db.state.read().unwrap().entries.contains_key("expired"));
        assert_eq!(db.purge_expired(), 0);
    }

    #[test]
    fn ttl_removes_expired_key_on_access() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "expired",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );

        assert_eq!(db.ttl("expired"), -2);
        assert!(!db.state.read().unwrap().entries.contains_key("expired"));
        assert_eq!(db.purge_expired(), 0);
    }

    #[test]
    fn get_keeps_live_keys_and_allows_overwrite_after_expired_access() {
        let db = Db::default();
        db.set("live".to_owned(), b"keep".to_vec());
        insert_with_expiration(
            &db,
            "expired",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );

        assert_eq!(db.get("expired"), None);
        assert_eq!(db.get("live"), Some(b"keep".to_vec()));

        db.set("expired".to_owned(), b"new".to_vec());
        assert_eq!(db.get("expired"), Some(b"new".to_vec()));
        assert_eq!(db.ttl("expired"), -1);
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
            let state = db.state.read().unwrap();
            let entries = &state.entries;
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
        let state = db.state.read().unwrap();
        let entries = &state.entries;
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
            let state = db.state.read().expect("db lock poisoned");
            let entries = &state.entries;
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

        assert_eq!(db.delete(&keys(&["language"])), 1);
        assert_eq!(db.get("language"), None);
        assert_eq!(db.delete(&keys(&["language"])), 0);
    }

    #[test]
    fn delete_treats_unpurged_expired_key_as_missing() {
        let db = Db::default();
        let now = Instant::now();
        insert_with_expiration(&db, "expired", b"old", now - Duration::from_secs(1));
        insert_with_expiration(&db, "future", b"keep", now + Duration::from_secs(3600));

        // purge 전이라 맵에는 남아 있지만 논리적으로는 없는 키이므로 개수에 세지 않습니다.
        assert_eq!(db.delete(&keys(&["expired"])), 0);
        assert!(!db
            .state
            .read()
            .expect("db lock poisoned")
            .entries
            .contains_key("expired"));

        // 아직 만료되지 않은 키는 기존처럼 삭제에 성공합니다.
        assert_eq!(db.delete(&keys(&["future"])), 1);
        assert!(db
            .state
            .read()
            .expect("db lock poisoned")
            .entries
            .is_empty());
    }

    // ---- 만료 인덱스(expirations) 동기화 테스트 ----

    // 인덱스에 키가 등록돼 있는지 확인합니다.
    fn in_index(db: &Db, key: &str) -> bool {
        db.state
            .read()
            .expect("db lock poisoned")
            .expirations
            .iter()
            .any(|(_, indexed)| indexed == key)
    }

    #[test]
    fn set_expired_time_registers_key_in_index() {
        let db = Db::default();
        db.set("key".to_owned(), b"value".to_vec());
        assert!(!in_index(&db, "key"));

        assert_eq!(db.set_expired_time("key", 3600), Ok(true));
        assert!(in_index(&db, "key"));

        // 같은 키에 EXPIRE를 다시 해도 한 번만 등록됩니다.
        assert_eq!(db.set_expired_time("key", 7200), Ok(true));
        assert_eq!(db.state.read().unwrap().expirations.len(), 1);
    }

    #[test]
    fn set_expired_time_does_not_register_missing_or_rejected_key() {
        let db = Db::default();
        db.set("permanent".to_owned(), b"keep".to_vec());

        assert_eq!(db.set_expired_time("missing", 10), Ok(false));
        assert_eq!(
            db.set_expired_time("permanent", 0),
            Err(ExpireError::InvalidTime)
        );
        assert!(db.state.read().unwrap().expirations.is_empty());
    }

    #[test]
    fn set_removes_key_from_index() {
        let db = Db::default();
        db.set("key".to_owned(), b"old".to_vec());
        assert_eq!(db.set_expired_time("key", 3600), Ok(true));

        // SET으로 덮어쓰면 TTL이 사라지므로 인덱스에서도 빠져야 합니다.
        db.set("key".to_owned(), b"new".to_vec());
        assert!(!in_index(&db, "key"));
        assert_eq!(db.ttl("key"), -1);
    }

    #[test]
    fn delete_removes_key_from_index() {
        let db = Db::default();
        let now = Instant::now();
        insert_with_expiration(&db, "live", b"v", now + Duration::from_secs(3600));
        insert_with_expiration(&db, "expired", b"v", now - Duration::from_secs(1));

        assert_eq!(db.delete(&keys(&["live"])), 1);
        // 만료된 키는 개수에 세지 않지만 인덱스에서는 정리돼야 합니다.
        assert_eq!(db.delete(&keys(&["expired"])), 0);
        assert!(db.state.read().unwrap().expirations.is_empty());
    }

    #[test]
    fn lazy_removal_on_get_and_ttl_removes_key_from_index() {
        let db = Db::default();
        let past = Instant::now() - Duration::from_secs(1);
        insert_with_expiration(&db, "by_get", b"v", past);
        insert_with_expiration(&db, "by_ttl", b"v", past);

        assert_eq!(db.get("by_get"), None);
        assert_eq!(db.ttl("by_ttl"), -2);
        assert!(db.state.read().unwrap().expirations.is_empty());
    }

    #[test]
    fn set_expired_time_on_expired_key_removes_it_from_index() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "expired",
            b"v",
            Instant::now() - Duration::from_secs(1),
        );

        assert_eq!(db.set_expired_time("expired", 3600), Ok(false));
        assert!(!in_index(&db, "expired"));
    }

    #[test]
    fn purge_expired_removes_only_expired_keys_from_index() {
        let db = Db::default();
        let now = Instant::now();
        insert_with_expiration(&db, "expired_a", b"v", now - Duration::from_secs(2));
        insert_with_expiration(&db, "expired_b", b"v", now - Duration::from_secs(1));
        insert_with_expiration(&db, "future", b"v", now + Duration::from_secs(3600));
        db.set("permanent".to_owned(), b"v".to_vec());

        assert_eq!(db.purge_expired(), 2);

        let state = db.state.read().unwrap();
        // 아직 만료되지 않은 TTL 키만 인덱스에 남고, TTL 없는 키는 처음부터 없습니다.
        assert_eq!(state.expirations.len(), 1);
        assert!(state.expirations.iter().any(|(_, key)| key == "future"));
        assert_eq!(state.entries.len(), 2);
    }

    #[test]
    fn purge_expired_ignores_keys_without_ttl() {
        let db = Db::default();
        for i in 0..100 {
            db.set(format!("permanent_{i}"), b"v".to_vec());
        }

        // TTL 키가 없으면 인덱스가 비어 있어 아무것도 지우지 않습니다.
        assert!(db.state.read().unwrap().expirations.is_empty());
        assert_eq!(db.purge_expired(), 0);
        assert_eq!(db.state.read().unwrap().entries.len(), 100);
    }

    // ---- 여러 키 삭제(DEL k1 k2 ...) 테스트 ----

    fn keys(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn delete_removes_multiple_keys_and_counts_only_live_ones() {
        let db = Db::default();
        let now = Instant::now();
        db.set("a".to_owned(), b"1".to_vec());
        insert_with_expiration(&db, "b", b"2", now + Duration::from_secs(3600));
        insert_with_expiration(&db, "expired", b"3", now - Duration::from_secs(1));
        db.set("other".to_owned(), b"keep".to_vec());

        // 없는 키와 만료된 키는 개수에 세지 않습니다.
        assert_eq!(db.delete(&keys(&["a", "missing", "b", "expired"])), 2);

        let state = db.state.read().unwrap();
        assert_eq!(state.entries.len(), 1);
        assert!(state.entries.contains_key("other"));
        // TTL이 있던 키는 살아 있었든 만료됐든 인덱스에서 빠져야 합니다.
        assert!(state.expirations.is_empty());
    }

    #[test]
    fn delete_counts_duplicate_key_once() {
        let db = Db::default();
        db.set("a".to_owned(), b"1".to_vec());

        // Redis처럼 첫 번째에서 지워지고 두 번째는 이미 없는 키로 봅니다.
        assert_eq!(db.delete(&keys(&["a", "a"])), 1);
        assert_eq!(db.get("a"), None);
    }

    #[test]
    fn delete_with_empty_key_list_removes_nothing() {
        let db = Db::default();
        db.set("a".to_owned(), b"1".to_vec());

        assert_eq!(db.delete(&[]), 0);
        assert_eq!(db.get("a"), Some(b"1".to_vec()));
    }

    #[test]
    fn delete_of_multiple_keys_is_not_observed_half_done() {
        const ROUNDS: usize = 20_000;
        let db = Db::default();
        let done = Arc::new(AtomicBool::new(false));

        // a를 먼저, b를 나중에 SET하므로 정상 상태에서는 "a 없음 + b 있음"이 나올 수 없습니다.
        // DEL a b가 키마다 따로 락을 잡으면 a만 지워진 순간에 그 상태가 보입니다.
        let writer = {
            let db = db.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                let targets = keys(&["a", "b"]);
                for _ in 0..ROUNDS {
                    db.set("a".to_owned(), b"1".to_vec());
                    db.set("b".to_owned(), b"2".to_vec());
                    db.delete(&targets);
                }
                done.store(true, Ordering::Release);
            })
        };

        let mut half_deleted = 0_usize;
        while !done.load(Ordering::Acquire) {
            // 두 키를 한 번의 읽기 락으로 확인해야 읽는 쪽의 경쟁이 섞이지 않습니다.
            let state = db.state.read().unwrap();
            if !state.entries.contains_key("a") && state.entries.contains_key("b") {
                half_deleted += 1;
            }
        }
        writer.join().unwrap();

        assert_eq!(half_deleted, 0, "DEL 중간 상태가 {half_deleted}번 관찰됨");
    }
}
