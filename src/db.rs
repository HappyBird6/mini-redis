//! In-memory key-value storage will be implemented here.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};

const SHARD_COUNT: usize = 64;
const _: () = assert!(SHARD_COUNT.is_power_of_two());
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

#[derive(Clone)]
pub struct Db {
    state: Arc<[RwLock<State>]>,
}

impl Default for Db {
    fn default() -> Self {
        Self {
            state: (0..SHARD_COUNT)
                .map(|_| RwLock::new(State::default()))
                .collect(),
        }
    }
}

impl Db {
    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash_key = hasher.finish() as usize;
        hash_key & (SHARD_COUNT - 1)
    }

    fn shard_for(&self, key: &str) -> &RwLock<State> {
        &self.state[self.shard_index(key)]
    }

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
        let mut state = self.shard_for(key).write().expect("db lock poisoned");
        Self::remove_if_expired(&mut state, key, Instant::now());
    }

    pub fn set(&self, key: String, value: Vec<u8>) {
        let entry = Entry {
            value,
            expires_at: None,
        };
        let mut state = self.shard_for(&key).write().expect("db lock poisoned");
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
            let state = self.shard_for(key).read().expect("db lock poisoned");
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
        let now = Instant::now();
        let mut deleted: usize = 0;
        let mut shard_indices: BTreeSet<usize> = BTreeSet::new();
        for key in keys {
            shard_indices.insert(self.shard_index(key));
        }

        let mut slots: Vec<Option<RwLockWriteGuard<State>>> =
            (0..SHARD_COUNT).map(|_| None).collect();
        for index in shard_indices {
            slots[index] = Some(self.state[index].write().expect("db lock poisoned"));
        }

        for key in keys {
            let state = slots[self.shard_index(key)]
                .as_mut()
                .expect("shard not locked");
            let success = Self::delete_with_locked_state(state, key, now);
            if success {
                deleted += 1;
            }
        }
        deleted
    }

    /*
    만료된 키를 칸(shard)마다 돌며 최대 limit개까지 지우고 지운 개수를 반환
    limit은 칸마다가 아니라 전체 합에 대한 상한 (server의 purge_until_done이 이 값으로 반복 여부를 판단)
    칸 하나를 처리하는 동안만 그 칸의 쓰기 락을 잡고, 다음 칸으로 넘어가며 놓음
        만료된 키는 지우기 전에도 get/ttl에서 이미 없는 키로 보이므로
        칸 사이에 락을 놓아도 다른 요청이 중간 상태를 볼 일이 없음
    */
    pub fn purge_expired(&self, limit: usize) -> usize {
        let now = Instant::now();
        let mut removed = 0;
        let iter = self.state.iter();
        for shard in iter {
            if removed >= limit {
                break;
            }
            let mut state = shard.write().expect("db lock poisoned");
            while let Some((at, _)) = state.expirations.first() {
                if *at > now {
                    break;
                }
                if removed >= limit {
                    break;
                }
                let (_, key) = state.expirations.pop_first().unwrap();
                removed += 1;
                state.entries.remove(&key);
            }
        }
        removed
    }

    pub fn set_expired_time(&self, key: &str, life_time: i64) -> Result<bool, ExpireError> {
        // 일단은 양수만 받게 하기
        if life_time <= 0 {
            return Err(ExpireError::InvalidTime);
        }

        // 쓰기락 획득
        let mut state = self.shard_for(key).write().expect("db lock poisoned");

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
            let state = self.shard_for(key).read().expect("db lock poisoned");
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
    let mut state = db.shard_for(key).write().expect("db lock poisoned");
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
    use super::{insert_with_expiration, Db, ExpireError, State};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLockReadGuard};
    use std::time::{Duration, Instant};

    // ---- 칸(shard) 내부를 들여다보는 도우미 ----
    // 키마다 들어 있는 칸이 다르므로 키로 칸을 찾아 확인하거나, 전체 칸을 합산합니다.

    // 키가 들어 있는 칸의 읽기 락을 잡습니다.
    fn shard_of<'a>(db: &'a Db, key: &str) -> RwLockReadGuard<'a, State> {
        db.shard_for(key).read().expect("db lock poisoned")
    }

    fn contains(db: &Db, key: &str) -> bool {
        shard_of(db, key).entries.contains_key(key)
    }

    fn expires_at_of(db: &Db, key: &str) -> Option<Instant> {
        shard_of(db, key).entries[key].expires_at
    }

    // 모든 칸의 항목 수 합계
    fn entry_count(db: &Db) -> usize {
        db.state
            .iter()
            .map(|shard| shard.read().unwrap().entries.len())
            .sum()
    }

    // 모든 칸의 만료 인덱스 항목 수 합계
    fn index_count(db: &Db) -> usize {
        db.state
            .iter()
            .map(|shard| shard.read().unwrap().expirations.len())
            .sum()
    }

    // 같은 칸에 들어가는 키를 count개 고릅니다.
    fn keys_in_same_shard(db: &Db, count: usize) -> Vec<String> {
        let target = db.shard_index("k0");
        (0..)
            .map(|i| format!("k{i}"))
            .filter(|key| db.shard_index(key) == target)
            .take(count)
            .collect()
    }

    // 서로 다른 칸에 들어가는 두 키를 고릅니다.
    fn keys_in_different_shards(db: &Db) -> (String, String) {
        let first = "a".to_owned();
        let second = (0..)
            .map(|i| format!("b{i}"))
            .find(|key| db.shard_index(key) != db.shard_index(&first))
            .unwrap();
        (first, second)
    }

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
        assert_eq!(db.purge_expired(usize::MAX), 0);
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
        let deadline = expires_at_of(&db, "key").unwrap();

        // 실제 호출은 두 관측 시점 사이에서 실행되므로 해당 범위에 있어야 합니다.
        let before = Instant::now();
        let actual = db.ttl("key");
        let after = Instant::now();
        let state = shard_of(&db, "key");
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

        assert_eq!(db.purge_expired(usize::MAX), 2);

        // get()은 삭제 전에도 None을 반환하므로 실제 맵에서 제거됐는지 확인합니다.
        assert!(!contains(&db, "expired_a"));
        assert!(!contains(&db, "expired_b"));
        assert_eq!(entry_count(&db), 2);

        assert_eq!(db.get("future"), Some(b"keep".to_vec()));
        assert_eq!(db.get("permanent"), Some(b"keep forever".to_vec()));
        assert_eq!(db.purge_expired(usize::MAX), 0);
    }

    #[test]
    fn purge_expired_on_empty_db_returns_zero() {
        let db = Db::default();
        assert_eq!(db.purge_expired(usize::MAX), 0);
        assert_eq!(entry_count(&db), 0);
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

        assert_eq!(db.purge_expired(usize::MAX), 0);
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
        let expires_at = expires_at_of(&db, "language").unwrap();

        // DB가 std::time::Instant를 사용하므로 실제 시간을 기다립니다.
        // 대기하는 동안에는 위 블록의 쓰기 락이 해제된 상태입니다.
        std::thread::sleep(
            expires_at.saturating_duration_since(Instant::now()) + Duration::from_millis(10),
        );

        // purge를 호출하지 않아도 GET 자체가 만료된 값을 숨겨야 합니다.
        assert_eq!(db.get("language"), None);
        // 만료된 키는 읽는 순간 삭제됩니다.
        assert!(!contains(&db, "language"));
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
            let deadline = expires_at_of(&db, "language").unwrap();
            let duration = Duration::from_secs(seconds as u64);
            assert!(deadline >= before + duration);
            assert!(deadline <= after + duration);
            assert_eq!(shard_of(&db, "language").entries["language"].value, b"rust");
            assert_eq!(shard_of(&db, "other").entries["other"].value, b"keep");
            assert_eq!(expires_at_of(&db, "other"), None);
        }
    }

    #[test]
    fn set_expired_time_does_not_create_missing_key() {
        let db = Db::default();
        assert_eq!(db.set_expired_time("missing", 10), Ok(false));
        assert_eq!(entry_count(&db), 0);
    }

    #[test]
    fn set_expired_time_removes_expired_key_without_reviving_it() {
        let db = Db::default();
        let deadline = Instant::now() - Duration::from_secs(1);
        insert_with_expiration(&db, "expired", b"old", deadline);

        assert_eq!(db.set_expired_time("expired", 3600), Ok(false));
        assert_eq!(db.get("expired"), None);
        // 되살리지 않고 삭제만 합니다.
        assert!(!contains(&db, "expired"));
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
        assert!(!contains(&db, "expired"));
        assert_eq!(db.purge_expired(usize::MAX), 0);
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
        assert!(!contains(&db, "expired"));
        assert_eq!(db.purge_expired(usize::MAX), 0);
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
            assert_eq!(entry_count(&db), 2);
            assert_eq!(expires_at_of(&db, "permanent"), None);
            assert_eq!(expires_at_of(&db, "temporary"), Some(deadline));
            assert_eq!(
                shard_of(&db, "permanent").entries["permanent"].value,
                b"keep"
            );
            assert_eq!(
                shard_of(&db, "temporary").entries["temporary"].value,
                b"keep"
            );
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
        let state = shard_of(&db, "key");
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
            assert!(expires_at_of(&db, "language").is_none());
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
        assert!(!contains(&db, "expired"));

        // 아직 만료되지 않은 키는 기존처럼 삭제에 성공합니다.
        assert_eq!(db.delete(&keys(&["future"])), 1);
        assert_eq!(entry_count(&db), 0);
    }

    // ---- 만료 인덱스(expirations) 동기화 테스트 ----

    // 인덱스에 키가 등록돼 있는지 확인합니다.
    fn in_index(db: &Db, key: &str) -> bool {
        shard_of(db, key)
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
        assert_eq!(index_count(&db), 1);
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
        assert_eq!(index_count(&db), 0);
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
        assert_eq!(index_count(&db), 0);
    }

    #[test]
    fn lazy_removal_on_get_and_ttl_removes_key_from_index() {
        let db = Db::default();
        let past = Instant::now() - Duration::from_secs(1);
        insert_with_expiration(&db, "by_get", b"v", past);
        insert_with_expiration(&db, "by_ttl", b"v", past);

        assert_eq!(db.get("by_get"), None);
        assert_eq!(db.ttl("by_ttl"), -2);
        assert_eq!(index_count(&db), 0);
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

        assert_eq!(db.purge_expired(usize::MAX), 2);

        // 아직 만료되지 않은 TTL 키만 인덱스에 남고, TTL 없는 키는 처음부터 없습니다.
        assert_eq!(index_count(&db), 1);
        assert!(in_index(&db, "future"));
        assert_eq!(entry_count(&db), 2);
    }

    #[test]
    fn purge_expired_ignores_keys_without_ttl() {
        let db = Db::default();
        for i in 0..100 {
            db.set(format!("permanent_{i}"), b"v".to_vec());
        }

        // TTL 키가 없으면 인덱스가 비어 있어 아무것도 지우지 않습니다.
        assert_eq!(index_count(&db), 0);
        assert_eq!(db.purge_expired(usize::MAX), 0);
        assert_eq!(entry_count(&db), 100);
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

        assert_eq!(entry_count(&db), 1);
        assert!(contains(&db, "other"));
        // TTL이 있던 키는 살아 있었든 만료됐든 인덱스에서 빠져야 합니다.
        assert_eq!(index_count(&db), 0);
    }

    #[test]
    fn delete_removes_keys_spread_across_shards() {
        let db = Db::default();
        let (a, b) = keys_in_different_shards(&db);
        let same = keys_in_same_shard(&db, 2);
        for key in [&a, &b, &same[0], &same[1]] {
            db.set(key.clone(), b"v".to_vec());
        }
        db.set("other".to_owned(), b"keep".to_vec());

        // 여러 칸에 걸친 키와 같은 칸에 모인 키를 한 번에 지웁니다.
        let targets = vec![a, b, same[0].clone(), same[1].clone()];
        assert_eq!(db.delete(&targets), 4);
        for key in &targets {
            assert!(!contains(&db, key));
        }
        assert_eq!(entry_count(&db), 1);
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
        // 두 키가 서로 다른 칸에 있어야 여러 칸에 걸친 DEL을 검증할 수 있습니다.
        let (a, b) = keys_in_different_shards(&db);

        // a를 먼저, b를 나중에 SET하므로 정상 상태에서는 "a 없음 + b 있음"이 나올 수 없습니다.
        // DEL a b가 키마다 따로 락을 잡으면 a만 지워진 순간에 그 상태가 보입니다.
        let writer = {
            let db = db.clone();
            let done = Arc::clone(&done);
            let (a, b) = (a.clone(), b.clone());
            std::thread::spawn(move || {
                let targets = vec![a.clone(), b.clone()];
                for _ in 0..ROUNDS {
                    db.set(a.clone(), b"1".to_vec());
                    db.set(b.clone(), b"2".to_vec());
                    db.delete(&targets);
                }
                done.store(true, Ordering::Release);
            })
        };

        // 두 칸의 읽기 락을 DEL과 같은 순서(작은 번호부터)로 함께 잡아야
        // 읽는 쪽이 두 키를 같은 순간의 상태로 확인할 수 있습니다.
        let (index_a, index_b) = (db.shard_index(&a), db.shard_index(&b));
        let mut half_deleted = 0_usize;
        while !done.load(Ordering::Acquire) {
            let low = db.state[index_a.min(index_b)].read().unwrap();
            let high = db.state[index_a.max(index_b)].read().unwrap();
            let (shard_a, shard_b) = if index_a < index_b {
                (&low, &high)
            } else {
                (&high, &low)
            };
            if !shard_a.entries.contains_key(&a) && shard_b.entries.contains_key(&b) {
                half_deleted += 1;
            }
        }
        writer.join().unwrap();

        assert_eq!(half_deleted, 0, "DEL 중간 상태가 {half_deleted}번 관찰됨");
    }

    // ---- purge 한 번에 지우는 개수 상한(limit) 테스트 ----

    #[test]
    fn purge_expired_removes_at_most_limit_keys() {
        let db = Db::default();
        let past = Instant::now() - Duration::from_secs(1);
        for i in 0..5 {
            insert_with_expiration(&db, &format!("expired_{i}"), b"v", past);
        }

        // 상한을 꽉 채워 지우면 아직 남은 키가 있을 수 있다는 신호입니다.
        assert_eq!(db.purge_expired(2), 2);
        assert_eq!(entry_count(&db), 3);
        assert_eq!(index_count(&db), 3);

        // 다시 호출하면 남은 키를 이어서 지우고, 상한보다 적게 지우면 다 비운 것입니다.
        assert_eq!(db.purge_expired(2), 2);
        assert_eq!(db.purge_expired(2), 1);
        assert_eq!(db.purge_expired(2), 0);

        assert_eq!(entry_count(&db), 0);
        assert_eq!(index_count(&db), 0);
    }

    #[test]
    fn purge_expired_limit_is_shared_across_shards() {
        let db = Db::default();
        let past = Instant::now() - Duration::from_secs(1);
        let (a, b) = keys_in_different_shards(&db);
        let same = keys_in_same_shard(&db, 2);
        for key in [&a, &b, &same[0], &same[1]] {
            insert_with_expiration(&db, key, b"v", past);
        }

        // limit은 칸마다가 아니라 한 번 호출 전체의 상한입니다.
        assert_eq!(db.purge_expired(3), 3);
        assert_eq!(index_count(&db), 1);
        assert_eq!(db.purge_expired(3), 1);
        assert_eq!(entry_count(&db), 0);
    }

    #[test]
    fn purge_expired_removes_earliest_expiration_first_within_shard() {
        // 칸끼리는 번호 순서로 돌기 때문에 이른 만료 순서는 같은 칸 안에서만 보장됩니다.
        let db = Db::default();
        let now = Instant::now();
        let keys = keys_in_same_shard(&db, 3);
        let (newest, oldest, middle) = (&keys[0], &keys[1], &keys[2]);
        insert_with_expiration(&db, newest, b"v", now - Duration::from_secs(1));
        insert_with_expiration(&db, oldest, b"v", now - Duration::from_secs(3));
        insert_with_expiration(&db, middle, b"v", now - Duration::from_secs(2));

        assert_eq!(db.purge_expired(1), 1);
        assert!(!in_index(&db, oldest));
        assert!(in_index(&db, middle));
        assert!(in_index(&db, newest));

        assert_eq!(db.purge_expired(1), 1);
        assert!(!in_index(&db, middle));
        assert!(in_index(&db, newest));
    }

    #[test]
    fn purge_expired_stops_at_live_key_before_reaching_limit() {
        let db = Db::default();
        let now = Instant::now();
        insert_with_expiration(&db, "expired_a", b"v", now - Duration::from_secs(2));
        insert_with_expiration(&db, "expired_b", b"v", now - Duration::from_secs(1));
        insert_with_expiration(&db, "future", b"keep", now + Duration::from_secs(3600));

        // 상한이 남아도 아직 만료되지 않은 키는 지우지 않습니다.
        assert_eq!(db.purge_expired(10), 2);
        assert!(in_index(&db, "future"));
        assert_eq!(db.get("future"), Some(b"keep".to_vec()));
    }

    #[test]
    fn purge_expired_with_zero_limit_removes_nothing() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "expired",
            b"v",
            Instant::now() - Duration::from_secs(1),
        );

        assert_eq!(db.purge_expired(0), 0);
        assert!(in_index(&db, "expired"));
        assert_eq!(entry_count(&db), 1);
    }

    // ---- 측정용 테스트 ----
    // 평소에는 건너뛰고 아래 명령으로만 실행합니다.
    // cargo test --release --bin mini-redis bench_ -- --ignored --nocapture --test-threads=1

    const BENCH_EXPIRED: usize = 200_000;

    struct PurgeMeasurement {
        // purge 한 번이 쓰기 락을 쥔 시간 중 최댓값과 중앙값
        slowest_call: Duration,
        median_call: Duration,
        // 쉬는 시간을 뺀 purge 호출 시간의 합
        busy: Duration,
        calls: usize,
        // purge가 도는 동안 get 한 번의 최악 지연
        worst_get: Duration,
    }

    // 만료된 키 BENCH_EXPIRED개를 limit씩 나눠 지우면서 get 지연을 잽니다.
    fn measure_purge(limit: usize) -> PurgeMeasurement {
        let db = Db::default();
        let past = Instant::now() - Duration::from_secs(1);
        for i in 0..BENCH_EXPIRED {
            insert_with_expiration(&db, &format!("expired_{i}"), b"v", past);
        }
        db.set("live".to_owned(), b"v".to_vec());

        // 상한을 꽉 채운 동안은 다시 돕니다. 서버의 다음 tick처럼 사이에 쉬어서
        // 기다리던 get이 락을 잡게 합니다. (yield_now만으로는 purge가 락을
        // 바로 다시 잡아 get이 끝까지 기다리는 경우가 많습니다.)
        let purger = db.clone();
        let handle = std::thread::spawn(move || {
            let mut calls = Vec::new();
            loop {
                let start = Instant::now();
                let removed = purger.purge_expired(limit);
                calls.push(start.elapsed());
                if removed < limit {
                    break calls;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        let mut worst_get = Duration::ZERO;
        while !handle.is_finished() {
            let start = Instant::now();
            let _ = db.get("live");
            worst_get = worst_get.max(start.elapsed());
        }
        let mut calls = handle.join().unwrap();
        assert_eq!(index_count(&db), 0);

        calls.sort();
        PurgeMeasurement {
            slowest_call: *calls.last().unwrap(),
            median_call: calls[calls.len() / 2],
            busy: calls.iter().sum(),
            calls: calls.len(),
            worst_get,
        }
    }

    fn limit_label(limit: usize) -> String {
        if limit == usize::MAX {
            "none".to_owned()
        } else {
            limit.to_string()
        }
    }

    #[test]
    #[ignore]
    fn bench_get_latency_during_purge() {
        // usize::MAX는 상한이 없던 기존 동작, 1_000은 나눠서 지우는 동작입니다.
        for limit in [usize::MAX, 1_000] {
            let m = measure_purge(limit);
            println!(
                "limit={:>5} slowest purge call={:?} worst get={:?}",
                limit_label(limit),
                m.slowest_call,
                m.worst_get
            );
        }
    }

    #[test]
    #[ignore]
    fn bench_purge_limit_sweep() {
        const RUNS: usize = 5;

        println!("limit,run,calls,median_call_us,slowest_call_us,worst_get_us,busy_ms,ns_per_key");
        for limit in [100, 500, 1_000, 2_000, 5_000, 10_000, 50_000, usize::MAX] {
            for run in 1..=RUNS {
                let m = measure_purge(limit);
                println!(
                    "{},{run},{},{},{},{},{:.2},{}",
                    limit_label(limit),
                    m.calls,
                    m.median_call.as_micros(),
                    m.slowest_call.as_micros(),
                    m.worst_get.as_micros(),
                    m.busy.as_secs_f64() * 1000.0,
                    m.busy.as_nanos() / BENCH_EXPIRED as u128
                );
            }
        }
    }

    // ---- 동시 접근 처리량 측정 ----
    // 여러 스레드가 서로 다른 키에 GET/SET을 섞어 보낼 때 락 경합으로 처리량이 얼마나
    // 막히는지 잽니다. 공개 API(set/get)만 쓰므로 샤드 도입 전후에 그대로 비교할 수 있습니다.

    const CONTENTION_KEYS: usize = 100_000;
    const CONTENTION_DURATION: Duration = Duration::from_secs(2);
    // 매 연산 시간을 재면 측정 비용이 커서 16번에 한 번만 지연을 기록합니다.
    const CONTENTION_SAMPLE_EVERY: u64 = 16;

    struct ContentionMeasurement {
        ops_per_sec: f64,
        p99: Duration,
        worst: Duration,
    }

    // 외부 크레이트 없이 스레드마다 다른 키를 고르기 위한 xorshift 난수입니다.
    fn next_random(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    // threads개 스레드가 CONTENTION_DURATION 동안 연산을 반복합니다.
    // 100번 중 write_percent번은 SET, 나머지는 GET입니다.
    fn measure_contention(threads: usize, write_percent: u64) -> ContentionMeasurement {
        let db = Db::default();
        let keys: Arc<Vec<String>> =
            Arc::new((0..CONTENTION_KEYS).map(|i| format!("key:{i}")).collect());
        for key in keys.iter() {
            db.set(key.clone(), b"value".to_vec());
        }

        // 모든 스레드가 준비된 뒤 같은 시각에 출발시킵니다.
        let barrier = Arc::new(std::sync::Barrier::new(threads + 1));
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let db = db.clone();
                let keys = Arc::clone(&keys);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let mut rng = 0x9E37_79B9_7F4A_7C15 ^ (t as u64 + 1);
                    let mut ops: u64 = 0;
                    let mut samples = Vec::new();
                    barrier.wait();
                    let started = Instant::now();
                    while started.elapsed() < CONTENTION_DURATION {
                        // elapsed 호출 비용을 줄이려고 시간 확인 사이에 연산을 묶어서 돌립니다.
                        for _ in 0..64 {
                            let r = next_random(&mut rng);
                            let key = &keys[(r as usize) % keys.len()];
                            let sample = ops.is_multiple_of(CONTENTION_SAMPLE_EVERY);
                            let op_started = sample.then(Instant::now);
                            if r % 100 < write_percent {
                                db.set(key.clone(), b"value".to_vec());
                            } else {
                                std::hint::black_box(db.get(key));
                            }
                            if let Some(op_started) = op_started {
                                samples.push(op_started.elapsed());
                            }
                            ops += 1;
                        }
                    }
                    (ops, started.elapsed(), samples)
                })
            })
            .collect();

        barrier.wait();
        let mut total_ops = 0;
        let mut longest_run = Duration::ZERO;
        let mut samples = Vec::new();
        for handle in handles {
            let (ops, elapsed, mut thread_samples) = handle.join().unwrap();
            total_ops += ops;
            longest_run = longest_run.max(elapsed);
            samples.append(&mut thread_samples);
        }

        samples.sort();
        ContentionMeasurement {
            ops_per_sec: total_ops as f64 / longest_run.as_secs_f64(),
            p99: samples[samples.len() * 99 / 100],
            worst: *samples.last().unwrap(),
        }
    }

    #[test]
    #[ignore]
    fn bench_contention_sweep() {
        const RUNS: usize = 3;

        let cpus = std::thread::available_parallelism().map_or(0, |n| n.get());
        println!("# cpus={cpus} keys={CONTENTION_KEYS} duration={CONTENTION_DURATION:?}");
        println!("write_percent,threads,run,ops_per_sec,p99_ns,worst_us");
        // 10: 읽기 위주, 50: 쓰기가 많은 경우
        for write_percent in [10, 50] {
            for threads in [1, 2, 4, 8, 16] {
                for run in 1..=RUNS {
                    let m = measure_contention(threads, write_percent);
                    println!(
                        "{write_percent},{threads},{run},{:.0},{},{}",
                        m.ops_per_sec,
                        m.p99.as_nanos(),
                        m.worst.as_micros()
                    );
                }
            }
        }
    }
}
