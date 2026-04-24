use tokio::sync::mpsc;

pub struct Connection
{
    pub device_id: String,
    pub sender: mpsc::UnboundedSender<Vec<u8>>,
}

impl Connection
{
    pub fn new(device_id: String, sender: mpsc::UnboundedSender<Vec<u8>>) -> Self
    {
        Self { device_id, sender }
    }

    pub fn send(&self, data: Vec<u8>) -> bool
    {
        self.sender.send(data).is_ok()
    }
}