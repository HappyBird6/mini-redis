use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub addr: SocketAddr,
    pub name: Option<String>,
}
pub struct ClientGuard {
    clients: Clients,
    client_id: u64,
}

impl ClientGuard {
    pub fn register(clients: Clients, client_id: u64, addr: SocketAddr) -> Self {
        {
            let mut registry = clients.lock().expect("clients lock poisoned");
            registry.insert(client_id, ClientInfo { addr, name: None });
        }
        Self { clients, client_id }
    }
}
impl Drop for ClientGuard {
    fn drop(&mut self) {
        // 다른 panic으로 락이 poisoned 상태여도,
        // 정리 중 다시 panic하지 않고 자신의 항목을 제거합니다.
        let mut registry = self
            .clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.remove(&self.client_id);
    }
}
/*
연결 client 리스트를 공유하기 위한 파일
*/
// RwLock은 읽기는 동시허용이지만 Mutex는 그렇지 않음.
// ID와 연결정보를 저장하는 BTreeMap을 Mutex로 감사고
// Arc로 감싸서 공동 소유
// type은 위의 긴 타입에 별칭을 붙이는 거임
pub type Clients = Arc<Mutex<BTreeMap<u64, ClientInfo>>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_removes_registration_while_unwinding_with_poisoned_lock() {
        let clients = Clients::default();
        let result = std::panic::catch_unwind(|| {
            let _guard =
                ClientGuard::register(Arc::clone(&clients), 1, "127.0.0.1:50000".parse().unwrap());
            let _lock = clients.lock().unwrap();
            panic!("simulate a panic while holding the registry lock");
        });

        assert!(result.is_err());
        assert!(clients.is_poisoned());
        let registry = clients.lock().unwrap_or_else(|error| error.into_inner());
        assert!(registry.is_empty());
    }
}
