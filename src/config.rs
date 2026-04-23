use std::time::Duration;

pub struct Config
{
    pub bind_address: String,
    pub max_queue_per_user: usize,
    pub message_time_to_live: Duration,
    pub ping_interval: Duration,
    pub max_message_size: usize,
}

impl Default for Config
{
    fn default() -> Self
    {
        Self
        {
            bind_address: "0.0.0.0:8443".to_string(),
            max_queue_per_user: 500,
            message_time_to_live: Duration::from_secs(604800), // 7 days if my calculations are right
            ping_interval: Duration::from_secs(300), // ping ever 5 minutes
            max_message_size: 65536, // 64 kb
        }
    }
}