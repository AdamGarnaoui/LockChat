use dashmap::DashMap;
use std::time::Instant;

pub struct RateLimiter
{
    requests: DashMap<String, Vec<Instant>>,
    max_requests: usize,
    window_secs: u64,
}

impl RateLimiter
{
    pub fn new(max_requests: usize, window_secs: u64) -> Self
    {
        Self
        {
            requests: DashMap::new(),
            max_requests,
            window_secs,
        }
    }

    pub fn check(&self, key: &str) -> bool
    {
        let now = Instant::now();
        let mut entry = self.requests.entry(key.to_string()).or_default();

        // remove old entries outside the window
        entry.retain(|t| now.duration_since(*t).as_secs() < self.window_secs);

        if entry.len() >= self.max_requests
        {
            return false;
        }

        entry.push(now);
        true
    }

    pub fn cleanup(&self)
    {
        let now = Instant::now();
        self.requests.retain(|_, timestamps|
        {
            timestamps.retain(|t| now.duration_since(*t).as_secs() < self.window_secs);
            !timestamps.is_empty()
        });
    }
}