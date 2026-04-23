mod connection;

use connection::ConnectionManager;

fn main()
{
    println!("LockChat Server Starting...");
    let manager = ConnectionManager::new();
    println!("Initialized Connection Manager");
}