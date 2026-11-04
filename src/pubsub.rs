use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Multiple Producer, Single Consumer
use tokio::sync::{mpsc, watch};
/*
메세지 구조체
채널과 메시지 가짐
*/
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    pub channel: Vec<u8>,
    pub message: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct Subscriber {
    pub sender: mpsc::Sender<PubSubMessage>,
    pub disconnect: watch::Sender<bool>,
}
// 특정 채널의 구독자 목록 타입. clientid를 key값으로 센더를 value로
pub type Subscribers = HashMap<u64, Subscriber>;

// K : 채널 이름, V : 그 채널의 구독자 목록
pub type PubSub = Arc<Mutex<HashMap<Vec<u8>, Subscribers>>>;

pub struct SubscriptionGuard {
    pubsub: PubSub,
    client_id: u64,
}

impl SubscriptionGuard {
    pub fn register(pubsub: PubSub, client_id: u64) -> Self {
        Self { pubsub, client_id }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        // 정리 중에는 poisoned 상태여도 데이터를 얻어 정리
        let mut registry = self
            .pubsub
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        registry.retain(|_, subscribers| {
            // 해당 채널에서 현재 연결만 제거
            subscribers.remove(&self.client_id);

            // 다른 구독자가 남아 있는 채널만 유지
            !subscribers.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subscriber() -> Subscriber {
        let (sender, _) = mpsc::channel(1);
        let (disconnect, _) = watch::channel(false);
        Subscriber { sender, disconnect }
    }

    #[test]
    fn guard_removes_own_subscriptions_and_empty_channels_only() {
        let pubsub = PubSub::default();
        {
            let mut registry = pubsub.lock().unwrap();
            registry.insert(
                b"shared".to_vec(),
                HashMap::from([(1, subscriber()), (2, subscriber())]),
            );
            registry.insert(b"private".to_vec(), HashMap::from([(1, subscriber())]));
        }
        let guard = SubscriptionGuard::register(pubsub.clone(), 1);
        drop(guard);
        let registry = pubsub.lock().unwrap();
        assert_eq!(registry.len(), 1);
        let remaining = registry.get(b"shared".as_slice()).unwrap();
        assert_eq!(remaining.keys().copied().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn guard_cleans_up_during_unwind_even_with_poisoned_registry() {
        let pubsub = PubSub::default();
        let result = std::panic::catch_unwind(|| {
            let _guard = SubscriptionGuard::register(pubsub.clone(), 1);
            let mut registry = pubsub.lock().unwrap();
            registry.insert(b"news".to_vec(), HashMap::from([(1, subscriber())]));
            panic!("simulate failure while holding registry lock");
        });
        assert!(result.is_err());
        assert!(pubsub.is_poisoned());
        assert!(pubsub
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }
}
