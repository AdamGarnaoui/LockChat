use std::collections::HashMap;
use tokio::sync::mpsc;

pub struct Connection
{
    pub device_id: String,
    pub sender: mpsc::UnboundedSender<Vec<u8>>,
}

pub struct ConnectionManager
{
    connections: HashMap<String, Vec<Connection>>,
}

impl ConnectionManager
{

    pub fn new() -> Self
    {
        Self
        {
            connections: HashMap::new(),
        }
    }

    pub fn add(&mut self, key_hash: &str, con: Connection)
    {
        self.connections.entry(key_hash.to_string()).or_default().push(con);
    }

    pub fn remove(&mut self, key_hash: &str, device_id: &str)
    {

        if let Some(conns) = self.connections.get_mut(key_hash)
        {
            conns.retain(|c| c.device_id != device_id);

            if conns.is_empty()
            {
                self.connections.remove(key_hash);
            }
        }
    }

    pub fn get(&self, key_hash: &str) -> Option<&Vec<Connection>>
    {
        self.connections.get(key_hash)
    }

}