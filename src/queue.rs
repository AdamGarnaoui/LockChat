use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize)]
pub struct QueuedMessage 
{
    pub msg_id: String,
    pub from_hash: String,  // store user public key as hash because its big
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