use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;
use crate::connection::Connection;
use crate::queue::{QueuedMessage, QueueStore};
use crate::config::Config;

#[derive(Serialize, Deserialize)]
pub struct Envelope
{
    pub msg_id: String,
    pub from: String,
    pub to: String,
    pub payload: String,
}

#[derive(Serialize, Deserialize)]
pub struct DeliveryStatus
{
    pub msg_id: String,
    pub status: String,
}

#[derive(Serialize, Deserialize, Default)]
pub struct PublicKeyStore
{
    pub keys: Vec<(String, Vec<u8>)>,
}

impl PublicKeyStore
{
    pub fn save(&self, path: &str) -> Result<(), String>
    {
        let data = serde_json::to_vec(self)
            .map_err(|e| format!("Failed to serialize public keys: {}", e))?;
        let tmp = format!("{}.tmp", path);
        std::fs::write(&tmp, data)
            .map_err(|e| format!("Failed to write temp public key file: {}", e))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| format!("Failed to rename public key file: {}", e))?;
        Ok(())
    }

    pub fn load(path: &str) -> Self
    {
        if !Path::new(path).exists()
        {
            return Self::default();
        }

        match std::fs::read(path)
        {
            Ok(data) =>
            {
                serde_json::from_slice(&data).unwrap_or_default()
            }
            Err(_) => Self::default(),
        }
    }
}

pub struct Router
{
    pub connections: DashMap<String, Vec<Connection>>,
    pub offline_queue: DashMap<String, Vec<QueuedMessage>>,
    pub public_keys: DashMap<String, Vec<u8>>,
    pub pending_receipts: DashMap<String, (String, String)>,
    pub config: Config,
    dirty: AtomicBool,
    total_queued_bytes: AtomicUsize,
    save_lock: Mutex<()>,
    public_key_save_lock: Mutex<()>,
}

impl Router
{
    pub fn new(config: Config) -> Self
    {
        let store = QueueStore::load("queue.dat");
        let offline_queue = DashMap::new();
        let pending_receipts = DashMap::new();
        let mut initial_bytes: usize = 0;

        for (key, messages) in store.queues
        {
            for message in messages.iter()
            {
                pending_receipts.insert(
                    message.msg_id.clone(),
                    (message.from_hash.clone(), message.to_hash.clone()),
                );
                initial_bytes += Self::queued_message_size(message);
            }
            offline_queue.insert(key, messages);
        }

        let public_key_store = PublicKeyStore::load("public_keys.dat");
        let public_keys = DashMap::new();

        for (key, pk_bytes) in public_key_store.keys
        {
            public_keys.insert(key, pk_bytes);
        }

        Self
        {
            connections: DashMap::new(),
            offline_queue,
            public_keys,
            pending_receipts,
            config,
            dirty: AtomicBool::new(false),
            total_queued_bytes: AtomicUsize::new(initial_bytes),
            save_lock: Mutex::new(()),
            public_key_save_lock: Mutex::new(()),
        }
    }

    pub fn register_connection(&self, key_hash: &str, conn: Connection)
    {
        self.connections
            .entry(key_hash.to_string())
            .or_default()
            .push(conn);
    }

    pub fn remove_connection(&self, key_hash: &str, device_id: &str)
    {
        if let Some(mut conns) = self.connections.get_mut(key_hash)
        {
            conns.retain(|c| c.device_id != device_id);

            if conns.is_empty()
            {
                drop(conns);
                self.connections.remove(key_hash);
            }
        }
    }

    pub fn is_online(&self, key_hash: &str) -> bool
    {
        self.connections.contains_key(key_hash)
    }

    pub fn route_message(&self, envelope: &Envelope) -> DeliveryStatus
    {
        let msg_bytes = serde_json::to_vec(envelope).unwrap_or_default();

        if let Some(conns) = self.connections.get(&envelope.to)
        {
            let mut delivered = false;
            for conn in conns.iter()
            {
                if conn.send(msg_bytes.clone())
                {
                    delivered = true;
                }
            }
            if delivered
            {
                self.pending_receipts.insert(
                    envelope.msg_id.clone(),
                    (envelope.from.clone(), envelope.to.clone()),
                );

                return DeliveryStatus
                {
                    msg_id: envelope.msg_id.clone(),
                    status: "delivered".to_string(),
                };
            }
        }

        let queue_len = self.offline_queue
            .get(&envelope.to)
            .map(|q| q.len())
            .unwrap_or(0);

        if queue_len >= self.config.max_queue_per_user
        {
            return DeliveryStatus
            {
                msg_id: envelope.msg_id.clone(),
                status: "queue_full".to_string(),
            };
        }

        let per_sender_cap = 20usize;
        let sender_queue_len = self.offline_queue
            .get(&envelope.to)
            .map(|q| q.iter().filter(|m| m.from_hash == envelope.from).count())
            .unwrap_or(0);

        if sender_queue_len >= per_sender_cap
        {
            return DeliveryStatus
            {
                msg_id: envelope.msg_id.clone(),
                status: "queue_full".to_string(),
            };
        }

        let queued_message = QueuedMessage::new(
            envelope.msg_id.clone(),
            envelope.from.clone(),
            envelope.to.clone(),
            msg_bytes,
        );

        let queued_message_size = Self::queued_message_size(&queued_message);
        let current_total = self.total_queued_bytes.load(Ordering::Relaxed);

        if current_total.saturating_add(queued_message_size) > self.config.max_total_queue_bytes
        {
            return DeliveryStatus
            {
                msg_id: envelope.msg_id.clone(),
                status: "queue_full".to_string(),
            };
        }

        self.total_queued_bytes.fetch_add(queued_message_size, Ordering::Relaxed);

        self.offline_queue
            .entry(envelope.to.clone())
            .or_default()
            .push(queued_message);

        self.pending_receipts.insert(
            envelope.msg_id.clone(),
            (envelope.from.clone(), envelope.to.clone()),
        );

        self.mark_dirty();

        DeliveryStatus
        {
            msg_id: envelope.msg_id.clone(),
            status: "queued".to_string(),
        }
    }

    pub fn flush_queue(&self, key_hash: &str) -> Vec<Vec<u8>>
    {
        if let Some((_, messages)) = self.offline_queue.remove(key_hash)
        {
            self.mark_dirty();

            let mut valid_payloads = Vec::new();
            let mut removed_bytes: usize = 0;

            for message in messages
            {
                let msg_size = Self::queued_message_size(&message);

                if message.is_expired(self.config.message_time_to_live.as_secs())
                {
                    self.pending_receipts.remove(&message.msg_id);
                    removed_bytes += msg_size;
                    continue;
                }

                match serde_json::from_slice::<Envelope>(&message.payload)
                {
                    Ok(envelope)
                        if envelope.msg_id == message.msg_id
                        && envelope.from == message.from_hash
                        && envelope.to == message.to_hash
                        && envelope.to == key_hash =>
                    {
                        removed_bytes += msg_size;
                        valid_payloads.push(message.payload);
                    }
                    _ =>
                    {
                        self.pending_receipts.remove(&message.msg_id);
                        removed_bytes += msg_size;
                    }
                }
            }

            self.total_queued_bytes.fetch_sub(removed_bytes, Ordering::Relaxed);
            valid_payloads
        }
        else
        {
            vec![]
        }
    }

    pub async fn validate_and_store_public_key(&self, key_hash: &str, pk_bytes: Vec<u8>) -> Result<(), String>
    {
        if let Some(existing) = self.public_keys.get(key_hash)
        {
            if existing.value().as_slice() != pk_bytes.as_slice()
            {
                return Err("Public key mismatch".to_string());
            }

            return Ok(());
        }

        self.public_keys.insert(key_hash.to_string(), pk_bytes);
        self.save_public_keys().await
    }

    pub fn validate_read_receipt(&self, msg_id: &str, recipient: &str, sender: &str) -> bool
    {
        if let Some(entry) = self.pending_receipts.get(msg_id)
        {
            let (stored_sender, stored_recipient) = entry.value();
            if stored_sender == sender && stored_recipient == recipient
            {
                drop(entry);
                self.pending_receipts.remove(msg_id);
                true
            }
            else
            {
                false
            }
        }
        else
        {
            false
        }
    }

    fn mark_dirty(&self)
    {
        self.dirty.store(true, Ordering::Release);
    }

    pub async fn save_queue_if_dirty(&self)
    {
        if !self.dirty.swap(false, Ordering::AcqRel)
        {
            return;
        }
        self.save_queue().await;
    }

    pub async fn save_queue(&self)
    {
        let _lock = self.save_lock.lock().await;

        let queues: Vec<(String, Vec<QueuedMessage>)> = self.offline_queue
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().iter().map(|m|
            {
                QueuedMessage
                {
                    msg_id: m.msg_id.clone(),
                    from_hash: m.from_hash.clone(),
                    to_hash: m.to_hash.clone(),
                    payload: m.payload.clone(),
                    created: m.created,
                }
            }).collect()))
            .collect();

        let store = QueueStore { queues };
        if let Err(e) = store.save("queue.dat")
        {
            tracing::warn!("Failed to persist queue: {}", e);
        }
    }

    async fn save_public_keys(&self) -> Result<(), String>
    {
        let _lock = self.public_key_save_lock.lock().await;

        let keys: Vec<(String, Vec<u8>)> = self.public_keys
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        let store = PublicKeyStore { keys };
        store.save("public_keys.dat")
    }

    pub fn cleanup_expired(&self)
    {
        let ttl = self.config.message_time_to_live.as_secs();
        let mut removed_bytes: usize = 0;

        let keys: Vec<String> = self.offline_queue
            .iter()
            .map(|r| r.key().clone())
            .collect();

        for key in keys
        {
            if let Some(mut queue) = self.offline_queue.get_mut(&key)
            {
                let mut expired_ids = Vec::new();

                for m in queue.iter()
                {
                    if m.is_expired(ttl)
                    {
                        expired_ids.push(m.msg_id.clone());
                        removed_bytes += Self::queued_message_size(m);
                    }
                }

                queue.retain(|m| !m.is_expired(ttl));

                for msg_id in expired_ids
                {
                    self.pending_receipts.remove(&msg_id);
                }
            }
        }

        if removed_bytes > 0
        {
            self.total_queued_bytes.fetch_sub(removed_bytes, Ordering::Relaxed);
            self.mark_dirty();
        }
    }

    fn queued_message_size(message: &QueuedMessage) -> usize
    {
        message.msg_id.len()
            + message.from_hash.len()
            + message.to_hash.len()
            + message.payload.len()
            + std::mem::size_of::<u64>()
    }
}