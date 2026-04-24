mod auth;
mod config;
mod connection;
mod queue;

use config::Config;
use connection::ConnectionManager;

fn main()
{
    println!("LockChat Server Starting");

    let config = Config::default();
    println!("Default Config loaded");
    println!("LockChat server on {}", config.bind_address);


    let manager = ConnectionManager::new();
    println!("Initialized Connection Manager");
}