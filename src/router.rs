use dashmap::DashMap;
use serde::{Serialize, Deserialize};
use crate::connection::Connection;
use crate::queue::QueuedMessage;
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

pub struct Router
{
    pub connections: DashMap<String, Vec<Connection>>, // dashmap is threadsafe
    pub offline_queue: DashMap<String, Vec<QueuedMessage>>,
    pub public_keys: DashMap<String, Vec<u8>>,
    pub config: Config,
}

impl Router
{
    pub fn new(config: Config) -> Self
    {
        Self
        {
            connections: DashMap::new(),
            offline_queue: DashMap::new(),
            public_keys: DashMap::new(),
            config,
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
                return DeliveryStatus
                {
                    msg_id: envelope.msg_id.clone(),
                    status: "delivered".to_string(),
                }
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


        self.offline_queue
        .entry(envelope.to.clone())
        .or_default()
        .push(QueuedMessage::new
            (
                envelope.msg_id.clone(),
                envelope.from.clone(),
                envelope.to.clone(),
                msg_bytes,
            )
        );

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
            messages
            .into_iter()
            .filter(|m| !m.is_expired(self.config.message_time_to_live.as_secs()))
            .map(|m| m.payload)
            .collect()
        }
        else
        {
            vec![]
        }
    }

    pub fn store_public_key(&self, key_hash: &str, pk_bytes: Vec<u8>)
    {
        self.public_keys.insert(key_hash.to_string(), pk_bytes);
    }
}