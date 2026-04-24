mod auth;
mod config;
mod connection;
mod queue;
mod router;

use config::Config;
use connection::ConnectionManager;
use router::Router;

fn main()
{
    println!("LockChat Server Starting");

    let config = Config::default();
    println!("Default Config loaded");
    println!("LockChat server on {}", config.bind_address);

    let manager = ConnectionManager::new();
    println!("Initialized Connection Manager");

    let router = Router::new(config);
    println!("Router initialized");
}