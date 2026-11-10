use std::collections::HashSet;

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::client::Clients;
use crate::command::{ClientCommand, PubSubCommand};
use crate::db::ExpireError;
use crate::pubsub::{PubSub, PubSubMessage, Subscriber};
use crate::{command::Command, db::Db, resp::Frame};

pub fn execute(command: Command, db: &Db) -> Frame {
    match command {
        Command::Ping => Frame::SimpleString("PONG".to_owned()),
        Command::Set { key, value } => {
            db.set(key, value);
            Frame::SimpleString("OK".to_owned())
        }
        Command::Get { key } => match db.get(&key) {
            Some(value) => Frame::BulkString(value),
            None => Frame::Null,
        },
        Command::Del { keys } => {
            let count = db.delete(&keys) as i64;
            //redis 삭제는 삭제한 갯수를 리턴한다고 함
            Frame::Integer(count)
        }
        Command::Expire { key, seconds } => match db.set_expired_time(&key, seconds) {
            Ok(true) => Frame::Integer(1),
            Ok(false) => Frame::Integer(0),
            Err(ExpireError::InvalidTime) => {
                Frame::Error("ERR expire time must be positive".to_owned())
            }
            Err(ExpireError::OutOfRange) => {
                Frame::Error("ERR expire time is out of range".to_owned())
            }
        },
        Command::Ttl { key } => Frame::Integer(db.ttl(&key)),
        Command::Quit | Command::Client(_) | Command::PubSub(_) => {
            unreachable!("Command unreachable!")
        }
    }
}
pub fn execute_pubsub(
    command: PubSubCommand,
    client_id: u64,
    pubsub: &PubSub,
    subscriber: &Subscriber,
    subscriptions: &mut HashSet<Vec<u8>>,
    receiver: &mut mpsc::Receiver<PubSubMessage>,
) -> Vec<Frame> {
    // 여러 채널 구독하면 프레임이 여러개 나오니까 vec로 리턴
    // Subscribe / Unsubscribe / Publish 처리
    match command {
        PubSubCommand::Subscribe { channels } => {
            let mut response = Vec::with_capacity(channels.len());
            {
                let mut registry = pubsub.lock().expect("pubsub lock poisoned");

                // 종료 요청된 연결은 새로 구독하지 않음
                if *subscriber.disconnect.borrow() {
                    return Vec::new();
                }

                for channel in channels {
                    // 해당 채널의 구독자 목록 가져오기.
                    // 채널이 없으면 빈 HashMap을 만들어서 넣음.
                    let subscribers = registry.entry(channel.clone()).or_default();
                    subscribers.insert(client_id, subscriber.clone());

                    // 해당 연결의 구독 목록에도 추가
                    subscriptions.insert(channel.clone());

                    // 구독한 채널 마다 응답 생성
                    response.push(Frame::Array(vec![
                        Frame::BulkString(b"subscribe".to_vec()),
                        Frame::BulkString(channel),
                        Frame::Integer(subscriptions.len() as i64),
                    ]));
                }
            }
            response
        }
        PubSubCommand::Unsubscribe { channels } => {
            // 인자가 없으면 현재 연결이 구독 중인 모든 채널을 대상으로 함
            let targets: Vec<Vec<u8>> = if channels.is_empty() {
                subscriptions.iter().cloned().collect()
            } else {
                channels
            };

            let mut response = Vec::with_capacity(targets.len());
            {
                let mut registry = pubsub.lock().expect("pubsub lock poisoned");

                // 이미 큐에 들어온 메시지를 먼저 응답 목록에 넣음.
                // 해제할 채널뿐 아니라 모든 채널의 대기 메시지를 FIFO로 꺼냄.
                // 구독 해제전 받은 메세지를 처리하기 위함
                while let Ok(event) = receiver.try_recv() {
                    response.push(Frame::Array(vec![
                        Frame::BulkString(b"message".to_vec()),
                        Frame::BulkString(event.channel),
                        Frame::BulkString(event.message),
                    ]));
                }

                // 구독이 없는 경우에도 대기 메시지 뒤에 확인 응답을 추가한다.
                if targets.is_empty() {
                    response.push(Frame::Array(vec![
                        Frame::BulkString(b"unsubscribe".to_vec()),
                        Frame::Null,
                        Frame::Integer(0),
                    ]));
                } else {
                    for channel in targets {
                        let channel_empty = if let Some(subscribers) = registry.get_mut(&channel) {
                            subscribers.remove(&client_id);
                            subscribers.is_empty()
                        } else {
                            false
                        };
                        // 구독자가 없어진 채널 항목도 제거
                        if channel_empty {
                            registry.remove(&channel);
                        }

                        // 현재 연결의 구독 목록에서도 제거
                        subscriptions.remove(&channel);

                        // 채널별 해제 확인 응답
                        response.push(Frame::Array(vec![
                            Frame::BulkString(b"unsubscribe".to_vec()),
                            Frame::BulkString(channel),
                            Frame::Integer(subscriptions.len() as i64),
                        ]));
                    }
                }
            }
            response
        }
        PubSubCommand::Publish { channel, message } => {
            let mut receiver_count = 0_i64;
            let mut failed_clients = HashSet::new();
            {
                let mut registry = pubsub.lock().expect("pubsub lock poisoned");
                if let Some(subscribers) = registry.get(&channel) {
                    // 값들 순회하면서 메세지 publish
                    for (&subscriber_id, target_subscriber) in subscribers {
                        if *target_subscriber.disconnect.borrow() {
                            failed_clients.insert(subscriber_id);
                            continue;
                        }
                        let event = PubSubMessage {
                            channel: channel.clone(),
                            message: message.clone(),
                        };
                        // 메세지 try_send
                        match target_subscriber.sender.try_send(event) {
                            Ok(()) => {
                                receiver_count += 1;
                            }
                            Err(TrySendError::Closed(_)) => {
                                // Receiver 닫힘

                                // 상대 연결에 종료 요청
                                target_subscriber.disconnect.send_replace(true);
                                failed_clients.insert(subscriber_id);
                            }
                            Err(TrySendError::Full(_)) => {
                                // 메세지 큐 가득참
                                tracing::warn!(
                                    client_id = subscriber_id,
                                    "pubsub queue limit reached"
                                );

                                target_subscriber.disconnect.send_replace(true);
                                failed_clients.insert(subscriber_id);
                            }
                        }
                    }
                }

                // 구독자 순회가 끝난 다음 실행. retain -> true만 반환하는 애들만남김
                // 밖 retain : subscribers가 empty 아닌것
                // 안 retain : 리시버 닫힌 연결의 clients id를 가지고 있는 애들은 버림
                if !failed_clients.is_empty() {
                    registry.retain(|_, subscribers| {
                        subscribers.retain(|id, _| !failed_clients.contains(id));

                        !subscribers.is_empty()
                    });
                }
            }

            vec![Frame::Integer(receiver_count)]
        }
    }
}
pub fn execute_client(command: ClientCommand, client_id: u64, clients: &Clients) -> Frame {
    match command {
        ClientCommand::Id => Frame::Integer(client_id as i64),
        ClientCommand::GetName => {
            let name = {
                let registry = clients.lock().expect("clients lock poisoned");

                registry
                    .get(&client_id)
                    .expect("current client must be registered")
                    .name
                    .clone()
            }; // 이름을 복사하고 락 해제

            match name {
                Some(name) => Frame::BulkString(name.into_bytes()),
                None => Frame::Null,
            }
        }
        ClientCommand::SetName { name } => {
            {
                // 쓰기 락 획득
                let mut registry = clients.lock().expect("clients lock poisoned");

                let client = registry
                    .get_mut(&client_id)
                    .expect("current client must be registered");
                client.name = if name.is_empty() { None } else { Some(name) };
            }
            Frame::SimpleString("OK".to_owned())
        }
        ClientCommand::List => {
            let snapshot = {
                let registry = clients.lock().expect("clients lock poisoned");
                registry.clone()
            }; // 목록을 복사한 뒤 락 해제

            let mut output = String::new();
            for (id, client) in snapshot {
                output.push_str(&format!(
                    "id={} addr={} name={}\n",
                    id,
                    client.addr,
                    client.name.as_deref().unwrap_or("")
                ));
            }
            Frame::BulkString(output.into_bytes())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::{execute, execute_pubsub};
    use crate::command::PubSubCommand;
    use crate::db::insert_with_expiration;
    use crate::pubsub::{PubSub, PubSubMessage, Subscriber};
    use crate::{command::Command, db::Db, resp::Frame};
    use std::collections::HashSet;
    use std::time::{Duration, Instant};
    use tokio::sync::{mpsc, watch};

    struct PubSubClient {
        id: u64,
        subscriber: Subscriber,
        receiver: mpsc::Receiver<PubSubMessage>,
        channels: HashSet<Vec<u8>>,
    }

    impl PubSubClient {
        fn new(id: u64, capacity: usize) -> Self {
            let (sender, receiver) = mpsc::channel(capacity);
            let (disconnect, _) = watch::channel(false);
            Self {
                id,
                subscriber: Subscriber { sender, disconnect },
                receiver,
                channels: HashSet::new(),
            }
        }

        fn execute(&mut self, pubsub: &PubSub, command: PubSubCommand) -> Vec<Frame> {
            execute_pubsub(
                command,
                self.id,
                pubsub,
                &self.subscriber,
                &mut self.channels,
                &mut self.receiver,
            )
        }

        fn subscribe(&mut self, pubsub: &PubSub, channels: &[&[u8]]) -> Vec<Frame> {
            self.execute(
                pubsub,
                PubSubCommand::Subscribe {
                    channels: channels.iter().map(|channel| channel.to_vec()).collect(),
                },
            )
        }

        fn publish(&mut self, pubsub: &PubSub, channel: &[u8], message: &[u8]) -> Vec<Frame> {
            self.execute(
                pubsub,
                PubSubCommand::Publish {
                    channel: channel.to_vec(),
                    message: message.to_vec(),
                },
            )
        }
    }

    fn acknowledgement(kind: &[u8], channel: &[u8], count: i64) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(kind.to_vec()),
            Frame::BulkString(channel.to_vec()),
            Frame::Integer(count),
        ])
    }

    fn message(channel: &[u8], payload: &[u8]) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(b"message".to_vec()),
            Frame::BulkString(channel.to_vec()),
            Frame::BulkString(payload.to_vec()),
        ])
    }

    #[test]
    fn pubsub_duplicate_subscription_counts_once_and_publish_fans_out_in_order() {
        let pubsub = PubSub::default();
        let mut a = PubSubClient::new(1, 8);
        let mut b = PubSubClient::new(2, 8);
        let mut publisher = PubSubClient::new(3, 8);
        assert_eq!(
            a.subscribe(&pubsub, &[b"news", b"news", b"other"]),
            vec![
                acknowledgement(b"subscribe", b"news", 1),
                acknowledgement(b"subscribe", b"news", 1),
                acknowledgement(b"subscribe", b"other", 2),
            ]
        );
        b.subscribe(&pubsub, &[b"news"]);
        assert_eq!(
            publisher.publish(&pubsub, b"missing", b"ignored"),
            vec![Frame::Integer(0)]
        );
        assert!(!pubsub.lock().unwrap().contains_key(b"missing".as_slice()));
        for payload in [b"\x00\xff".as_slice(), b"second"] {
            assert_eq!(
                publisher.publish(&pubsub, b"news", payload),
                vec![Frame::Integer(2)]
            );
        }
        for client in [&mut a, &mut b] {
            for payload in [b"\x00\xff".as_slice(), b"second"] {
                let event = client.receiver.try_recv().unwrap();
                assert_eq!(event.channel, b"news");
                assert_eq!(event.message, payload);
            }
            assert!(matches!(
                client.receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
        assert!(
            publisher.receiver.try_recv().is_err(),
            "publisher must not receive subscriber messages"
        );
    }

    #[test]
    fn unsubscribe_preserves_queued_messages_before_acknowledgement_and_resubscribe() {
        let pubsub = PubSub::default();
        let mut subscriber = PubSubClient::new(1, 8);
        let mut publisher = PubSubClient::new(2, 8);
        subscriber.subscribe(&pubsub, &[b"news", b"chat"]);
        publisher.publish(&pubsub, b"chat", b"first");
        publisher.publish(&pubsub, b"news", b"second");
        assert_eq!(
            subscriber.execute(
                &pubsub,
                PubSubCommand::Unsubscribe {
                    channels: vec![b"news".to_vec()]
                }
            ),
            vec![
                message(b"chat", b"first"),
                message(b"news", b"second"),
                acknowledgement(b"unsubscribe", b"news", 1),
            ]
        );
        assert!(subscriber.receiver.try_recv().is_err());
        assert_eq!(
            publisher.publish(&pubsub, b"news", b"gone"),
            vec![Frame::Integer(0)]
        );
        subscriber.subscribe(&pubsub, &[b"news"]);
        publisher.publish(&pubsub, b"news", b"new");
        let event = subscriber.receiver.try_recv().unwrap();
        assert_eq!(event.message, b"new");

        let responses =
            subscriber.execute(&pubsub, PubSubCommand::Unsubscribe { channels: vec![] });
        assert_eq!(responses.len(), 2);
        let mut removed = HashSet::new();
        for (index, response) in responses.into_iter().enumerate() {
            let Frame::Array(parts) = response else {
                panic!("expected unsubscribe acknowledgement")
            };
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Frame::BulkString(b"unsubscribe".to_vec()));
            assert_eq!(parts[2], Frame::Integer(1 - index as i64));
            let Frame::BulkString(channel) = &parts[1] else {
                panic!("expected channel")
            };
            removed.insert(channel.clone());
        }
        assert_eq!(removed, HashSet::from([b"news".to_vec(), b"chat".to_vec()]));
        assert!(subscriber.channels.is_empty());
        assert!(pubsub.lock().unwrap().is_empty());
        assert_eq!(
            subscriber.execute(&pubsub, PubSubCommand::Unsubscribe { channels: vec![] }),
            vec![Frame::Array(vec![
                Frame::BulkString(b"unsubscribe".to_vec()),
                Frame::Null,
                Frame::Integer(0),
            ])]
        );
        assert_eq!(
            subscriber.execute(
                &pubsub,
                PubSubCommand::Unsubscribe {
                    channels: vec![b"missing".to_vec()]
                }
            ),
            vec![acknowledgement(b"unsubscribe", b"missing", 0)]
        );
    }

    #[test]
    fn full_queue_disconnects_only_slow_subscriber_and_removes_all_its_channels() {
        let pubsub = PubSub::default();
        let mut slow = PubSubClient::new(1, 1);
        let mut fast = PubSubClient::new(2, 8);
        let mut publisher = PubSubClient::new(3, 8);
        slow.subscribe(&pubsub, &[b"news", b"slow-only"]);
        fast.subscribe(&pubsub, &[b"news"]);
        assert_eq!(
            publisher.publish(&pubsub, b"news", b"one"),
            vec![Frame::Integer(2)]
        );
        assert_eq!(
            publisher.publish(&pubsub, b"news", b"two"),
            vec![Frame::Integer(1)]
        );
        assert!(*slow.subscriber.disconnect.borrow());
        assert!(!*fast.subscriber.disconnect.borrow());
        assert!(!*publisher.subscriber.disconnect.borrow());
        assert!(slow.subscribe(&pubsub, &[b"again"]).is_empty());
        {
            let registry = pubsub.lock().unwrap();
            assert_eq!(registry.len(), 1);
            assert_eq!(
                registry
                    .get(b"news".as_slice())
                    .unwrap()
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
                vec![2]
            );
        }
        assert_eq!(fast.receiver.try_recv().unwrap().message, b"one");
        assert_eq!(fast.receiver.try_recv().unwrap().message, b"two");
    }

    #[test]
    fn closed_receiver_is_removed_without_affecting_other_subscribers() {
        let pubsub = PubSub::default();
        let mut closed = PubSubClient::new(1, 4);
        let mut live = PubSubClient::new(2, 4);
        let mut publisher = PubSubClient::new(3, 4);
        closed.subscribe(&pubsub, &[b"news", b"closed-only"]);
        live.subscribe(&pubsub, &[b"news"]);
        closed.receiver.close();
        assert_eq!(
            publisher.publish(&pubsub, b"news", b"hello"),
            vec![Frame::Integer(1)]
        );
        assert!(*closed.subscriber.disconnect.borrow());
        assert_eq!(live.receiver.try_recv().unwrap().message, b"hello");
        let registry = pubsub.lock().unwrap();
        assert_eq!(registry.len(), 1);
        assert!(!registry.get(b"news".as_slice()).unwrap().contains_key(&1));
    }

    #[test]
    fn ping_returns_pong() {
        let db = Db::default();
        assert_eq!(
            execute(Command::Ping, &db),
            Frame::SimpleString("PONG".to_owned())
        );
    }

    #[test]
    fn set_get_del_sequence_returns_expected_responses() {
        let db = Db::default();
        assert_eq!(
            execute(
                Command::Set {
                    key: "name".to_owned(),
                    value: b"kim".to_vec(),
                },
                &db,
            ),
            Frame::SimpleString("OK".to_owned())
        );
        assert_eq!(
            execute(
                Command::Get {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::BulkString(b"kim".to_vec())
        );
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["name".to_owned()],
                },
                &db
            ),
            Frame::Integer(1)
        );
        assert_eq!(
            execute(
                Command::Get {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::Null
        );
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["name".to_owned()],
                },
                &db
            ),
            Frame::Integer(0)
        );
    }

    #[test]
    fn missing_key_returns_null_for_get_and_zero_for_del() {
        let db = Db::default();
        assert_eq!(
            execute(
                Command::Get {
                    key: "missing".to_owned()
                },
                &db
            ),
            Frame::Null
        );
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["missing".to_owned()],
                },
                &db
            ),
            Frame::Integer(0)
        );
    }

    #[test]
    fn set_overwrites_existing_value() {
        let db = Db::default();
        db.set("name".to_owned(), b"kim".to_vec());
        assert_eq!(
            execute(
                Command::Set {
                    key: "name".to_owned(),
                    value: b"lee".to_vec(),
                },
                &db,
            ),
            Frame::SimpleString("OK".to_owned())
        );
        assert_eq!(
            execute(
                Command::Get {
                    key: "name".to_owned()
                },
                &db
            ),
            Frame::BulkString(b"lee".to_vec())
        );
    }

    #[test]
    fn set_and_get_preserve_binary_and_empty_values() {
        let db = Db::default();
        for value in [vec![0xff, 0x00, b'\r', b'\n'], Vec::new()] {
            assert_eq!(
                execute(
                    Command::Set {
                        key: "data".to_owned(),
                        value: value.clone(),
                    },
                    &db,
                ),
                Frame::SimpleString("OK".to_owned())
            );
            assert_eq!(
                execute(
                    Command::Get {
                        key: "data".to_owned()
                    },
                    &db
                ),
                Frame::BulkString(value)
            );
        }
    }

    #[test]
    fn del_preserves_other_keys() {
        let db = Db::default();
        db.set("name".to_owned(), b"kim".to_vec());
        db.set("other".to_owned(), b"keep".to_vec());
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["name".to_owned()],
                },
                &db
            ),
            Frame::Integer(1)
        );
        assert_eq!(db.get("name"), None);
        assert_eq!(db.get("other"), Some(b"keep".to_vec()));
    }

    #[test]
    fn del_returns_zero_for_expired_key_before_purge() {
        let db = Db::default();
        insert_with_expiration(
            &db,
            "expired",
            b"old",
            Instant::now() - Duration::from_secs(1),
        );
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["expired".to_owned()],
                },
                &db
            ),
            Frame::Integer(0)
        );
    }

    #[test]
    fn del_returns_number_of_deleted_keys() {
        let db = Db::default();
        db.set("a".to_owned(), b"1".to_vec());
        db.set("b".to_owned(), b"2".to_vec());
        db.set("keep".to_owned(), b"3".to_vec());
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["a".to_owned(), "missing".to_owned(), "b".to_owned()],
                },
                &db
            ),
            Frame::Integer(2)
        );
        assert_eq!(db.get("a"), None);
        assert_eq!(db.get("keep"), Some(b"3".to_vec()));
    }

    #[test]
    fn del_counts_duplicate_key_once() {
        let db = Db::default();
        db.set("a".to_owned(), b"1".to_vec());
        assert_eq!(
            execute(
                Command::Del {
                    keys: vec!["a".to_owned(), "a".to_owned()],
                },
                &db
            ),
            Frame::Integer(1)
        );
    }
}
