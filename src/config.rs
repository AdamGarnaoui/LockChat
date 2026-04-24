use std::time::Duration;

pub struct Config
{
    pub bind_address: String,
    pub max_queue_per_user: usize,
    pub message_time_to_live: Duration,
    pub ping_interval: Duration,
    pub max_message_size: usize,
    pub rate_limit_messages: usize,
    pub rate_limit_window_secs: u64,
    pub rate_limit_connections: usize,
    pub rate_limit_connection_window_secs: u64,
}

impl Default for Config
{
    fn default() -> Self
    {
        Self
        {
            bind_address: "0.0.0.0:8443".to_string(),
            max_queue_per_user: 500,
            message_time_to_live: Duration::from_secs(604800),
            ping_interval: Duration::from_secs(300),
            max_message_size: 65536,
            rate_limit_messages: 60,          // 60 messages per window
            rate_limit_window_secs: 60,       // per minute
            rate_limit_connections: 5,        // 5 connection attempts
            rate_limit_connection_window_secs: 60, // per minute
        }
    }
}