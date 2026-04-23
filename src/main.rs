mod config;
mod connection;

use config::Config;
use connection::ConnectionManager;

fn main()
{
    println!("LockChat Server Starting");

    let config = Config::default();
    println!("Default Config loaded");

    let manager = ConnectionManager::new();
    println!("Initialized Connection Manager");
}