mod auth;
mod config;
mod connection;
mod queue;
mod ratelimit;
mod router;
mod server;

use axum::{extract::State, routing::get, Router as AxumRouter};
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::ConnectInfo;
use axum::response::IntoResponse;
use config::Config;
use ratelimit::RateLimiter;
use router::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

struct AppState
{
    router: Arc<Router>,
    message_limiter: Arc<RateLimiter>,
    connection_limiter: Arc<RateLimiter>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse
{
    let router = state.router.clone();
    let message_limiter = state.message_limiter.clone();
    let connection_limiter = state.connection_limiter.clone();
    let remote_addr = addr.ip().to_string();

    ws.on_upgrade(move |socket|
    {
        server::handle_socket(socket, router, message_limiter, connection_limiter, remote_addr)
    })
}

async fn health() -> &'static str
{
    "ok"
}

#[tokio::main]
async fn main()
{
    tracing_subscriber::fmt::init();

    println!("LockChat Server Starting");

    let config = Config::default();
    let bind_address = config.bind_address.clone();

    let message_limiter = Arc::new(RateLimiter::new(
        config.rate_limit_messages,
        config.rate_limit_window_secs,
    ));
    let connection_limiter = Arc::new(RateLimiter::new(
        config.rate_limit_connections,
        config.rate_limit_connection_window_secs,
    ));

    let router = Arc::new(Router::new(config));

    let state = Arc::new(AppState
    {
        router: router.clone(),
        message_limiter: message_limiter.clone(),
        connection_limiter: connection_limiter.clone(),
    });

    let app = AxumRouter::new()
        .route("/ws", get(ws_handler))
        .route("/health", get(health))
        .with_state(state);

    // clean up expired messages, persist queue, clean rate limiter every hour
    let router_cleanup = router.clone();
    let limiter_cleanup_msg = message_limiter.clone();
    let limiter_cleanup_conn = connection_limiter.clone();
    tokio::spawn(async move
    {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600));
        loop
        {
            interval.tick().await;
            let keys: Vec<String> = router_cleanup.offline_queue
                .iter()
                .map(|r| r.key().clone())
                .collect();
            for key in keys
            {
                if let Some(mut queue) = router_cleanup.offline_queue.get_mut(&key)
                {
                    queue.retain(|m| !m.is_expired(604800));
                }
            }
            router_cleanup.save_queue();
            limiter_cleanup_msg.cleanup();
            limiter_cleanup_conn.cleanup();
            info!("Hourly cleanup complete");
        }
    });

    // save queue on shutdown
    let router_shutdown = router.clone();
    tokio::spawn(async move
    {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutting down, saving queue...");
        router_shutdown.save_queue();
        std::process::exit(0);
    });

    info!("LockChat server on {}", bind_address);
    let listener = tokio::net::TcpListener::bind(&bind_address).await.unwrap();
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await.unwrap();
}