use std::time::{SystemTime, UNIX_EPOCH};
use std::path::Path;
use std::fs;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct QueuedMessage
{
    pub msg_id: String,
    pub from_hash: String,
    pub to_hash: String,
    pub payload: Vec<u8>,
    pub created: u64,
}

impl QueuedMessage
{
    pub fn new(msg_id: String, from_hash: String, to_hash: String, payload: Vec<u8>) -> Self
    {
        let created = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self
        {
            msg_id,
            from_hash,
            to_hash,
            payload,
            created,
        }
    }

    pub fn is_expired(&self, time_to_live_seconds: u64) -> bool
    {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now - self.created > time_to_live_seconds
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct QueueStore
{
    pub queues: Vec<(String, Vec<QueuedMessage>)>, // (key_hash, messages)
}

impl QueueStore
{
    pub fn save(&self, path: &str) -> Result<(), String>
    {
        let data = serde_json::to_vec(self)
            .map_err(|e| format!("Failed to serialize queue: {}", e))?;
        let tmp = format!("{}.tmp", path);
        fs::write(&tmp, &data)
            .map_err(|e| format!("Failed to write temp queue file: {}", e))?;
        fs::rename(&tmp, path)
            .map_err(|e| format!("Failed to rename queue file: {}", e))?;
        Ok(())
    }

    pub fn load(path: &str) -> Self
    {
        if !Path::new(path).exists()
        {
            return Self::default();
        }

        match fs::read(path)
        {
            Ok(data) =>
            {
                serde_json::from_slice(&data).unwrap_or_default()
            }
            Err(_) => Self::default(),
        }
    }
}
