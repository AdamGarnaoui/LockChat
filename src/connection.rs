use tokio::sync::mpsc;
use crate::server::OutboundMessage;

pub struct Connection
{
    pub device_id: String,
    pub sender: mpsc::UnboundedSender<OutboundMessage>,
}

impl Connection
{
    pub fn new(device_id: String, sender: mpsc::UnboundedSender<OutboundMessage>) -> Self
    {
        Self { device_id, sender }
    }

    pub fn send(&self, data: Vec<u8>) -> bool
    {
        self.sender.send(OutboundMessage::Envelope(data)).is_ok()
    }

    pub fn send_raw(&self, json: String) -> bool
    {
        self.sender.send(OutboundMessage::Raw(json)).is_ok()
    }
}