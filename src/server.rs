use crate::auth::{generate_challenge, verify_auth, AuthResponse};
use crate::connection::Connection;
use crate::ratelimit::RateLimiter;
use crate::router::{Envelope, Router};
use axum::extract::ws::{Message, WebSocket};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage
{
    #[serde(rename = "auth_response")]
    AuthResponse(AuthResponse),
    #[serde(rename = "send")]
    Send(Envelope),
    #[serde(rename = "delivery_status")]
    DeliveryStatus { msg_id: String, status: String },
    #[serde(rename = "is_online")]
    IsOnline { key_hash: String },
    #[serde(rename = "read_receipt")]
    ReadReceipt { msg_id: String, to: String },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage
{
    #[serde(rename = "challenge")]
    Challenge { nonce: String },
    #[serde(rename = "auth_result")]
    AuthResult { success: bool, error: Option<String> },
    #[serde(rename = "delivery_status")]
    DeliveryStatus { msg_id: String, status: String },
    #[serde(rename = "message")]
    Message { data: String },
    #[serde(rename = "queued_messages")]
    QueuedMessages { messages: Vec<String> },
    #[serde(rename = "online_status")]
    OnlineStatus { key_hash: String, online: bool },
    #[serde(rename = "read_receipt")]
    ReadReceipt { msg_id: String, from: String },
    #[serde(rename = "error")]
    Error { message: String },
    #[serde(rename = "rate_limited")]
    RateLimited { retry_after_secs: u64 },
}

pub async fn handle_socket(
    socket: WebSocket,
    router: Arc<Router>,
    message_limiter: Arc<RateLimiter>,
    connection_limiter: Arc<RateLimiter>,
    remote_addr: String,
)
{
    // check connection rate limit by IP
    if !connection_limiter.check(&remote_addr)
    {
        warn!("Connection rate limited: {}", remote_addr);
        return;
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    // send challenge

    let challenge = generate_challenge();
    let challenge_msg = serde_json::to_string(&ServerMessage::Challenge
    {
        nonce: challenge.nonce.clone(),
    }).unwrap();

    if ws_sender.send(Message::Text(challenge_msg.into())).await.is_err()
    {
        warn!("Failed to send challenge");
        return;
    }

    // wait for auth response with a 30 second timeout

    let auth_msg = match tokio::time::timeout(
        std::time::Duration::from_secs(30),
        ws_receiver.next(),
    ).await
    {
        Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
        _ =>
        {
            warn!("Auth timeout or invalid message");
            return;
        }
    };

    let auth_response: AuthResponse = match serde_json::from_str(&auth_msg)
    {
        Ok(r) => r,
        Err(_) =>
        {
            warn!("Failed to parse auth response");
            return;
        }
    };

    // verify signature

    match verify_auth(&challenge.nonce, &auth_response)
    {
        Ok(true) =>
        {
            let result = serde_json::to_string(&ServerMessage::AuthResult
            {
                success: true,
                error: None,
            }).unwrap();
            let _ = ws_sender.send(Message::Text(result.into())).await;
        }
        _ =>
        {
            let result = serde_json::to_string(&ServerMessage::AuthResult
            {
                success: false,
                error: Some("Authentication failed".to_string()),
            }).unwrap();
            let _ = ws_sender.send(Message::Text(result.into())).await;
            warn!("Auth failed for {}", auth_response.key_hash);
            return;
        }
    }

    let key_hash = auth_response.key_hash.clone();
    let device_id = auth_response.device_id.clone();

    // store public key

    if let Ok(pk_bytes) =
        base64::engine::general_purpose::STANDARD.decode(&auth_response.public_key)
    {
        router.store_public_key(&key_hash, pk_bytes);
    }

    // register connection

    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let conn = Connection::new(device_id.clone(), tx);
    router.register_connection(&key_hash, conn);
    info!("Authenticated: {} device: {}", key_hash, device_id);

    // flush offline queue

    let queued = router.flush_queue(&key_hash);
    if !queued.is_empty()
    {
        let msgs: Vec<String> = queued.iter()
            .filter_map(|m| String::from_utf8(m.clone()).ok())
            .collect();

        let flush_msg = serde_json::to_string(&ServerMessage::QueuedMessages
        {
            messages: msgs.clone(),
        }).unwrap();

        let _ = ws_sender.send(Message::Text(flush_msg.into())).await;
        info!("Flushed {} queued messages to {}", msgs.len(), key_hash);
    }

    // main loop

    let router_send = router.clone();
    let key_hash_send = key_hash.clone();
    let max_message_size = router.config.max_message_size;
    let ping_interval = router.config.ping_interval;
    let rate_limit_window = router.config.rate_limit_window_secs;

    // push messages from channel to socket + ping

    let mut send_task = tokio::spawn(async move
    {
        let mut ping_timer = tokio::time::interval(ping_interval);
        loop
        {
            tokio::select!
            {
                data = rx.recv() =>
                {
                    match data
                    {
                        Some(data) =>
                        {
                            let msg = ServerMessage::Message
                            {
                                data: base64::engine::general_purpose::STANDARD.encode(&data),
                            };
                            let text = serde_json::to_string(&msg).unwrap();
                            if ws_sender.send(Message::Text(text.into())).await.is_err()
                            {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = ping_timer.tick() =>
                {
                    if ws_sender.send(Message::Ping(vec![].into())).await.is_err()
                    {
                        break;
                    }
                }
            }
        }
    });

    // receive messages from websocket and route them

    let mut recv_task = tokio::spawn(async move
    {
        while let Some(Ok(msg)) = ws_receiver.next().await
        {
            match msg
            {
                Message::Text(text) =>
                {
                    // message size validation
                    if text.len() > max_message_size
                    {
                        let err = serde_json::to_string(&ServerMessage::Error
                        {
                            message: "Message too large".to_string(),
                        }).unwrap();
                        if let Some(conns) = router_send.connections.get(&key_hash_send)
                        {
                            for conn in conns.iter()
                            {
                                let _ = conn.send(err.as_bytes().to_vec());
                            }
                        }
                        continue;
                    }

                    // rate limit check
                    if !message_limiter.check(&key_hash_send)
                    {
                        let limited = serde_json::to_string(&ServerMessage::RateLimited
                        {
                            retry_after_secs: rate_limit_window,
                        }).unwrap();
                        if let Some(conns) = router_send.connections.get(&key_hash_send)
                        {
                            for conn in conns.iter()
                            {
                                let _ = conn.send(limited.as_bytes().to_vec());
                            }
                        }
                        continue;
                    }

                    if let Ok(client_msg) = serde_json::from_str::<ClientMessage>(&text)
                    {
                        match client_msg
                        {
                            ClientMessage::Send(envelope) =>
                            {
                                let status = router_send.route_message(&envelope);

                                let delivery = ServerMessage::DeliveryStatus
                                {
                                    msg_id: status.msg_id,
                                    status: status.status,
                                };
                                let status_json = serde_json::to_string(&delivery).unwrap();

                                if let Some(conns) = router_send.connections.get(&key_hash_send)
                                {
                                    for conn in conns.iter()
                                    {
                                        let _ = conn.send(status_json.as_bytes().to_vec());
                                    }
                                }
                            }
                            ClientMessage::DeliveryStatus { msg_id, status } =>
                            {
                                info!("Delivery status received: {} - {}", msg_id, status);
                            }
                            ClientMessage::IsOnline { key_hash } =>
                            {
                                let online = router_send.is_online(&key_hash);
                                let response = ServerMessage::OnlineStatus
                                {
                                    key_hash,
                                    online,
                                };
                                let response_json = serde_json::to_string(&response).unwrap();
                                if let Some(conns) = router_send.connections.get(&key_hash_send)
                                {
                                    for conn in conns.iter()
                                    {
                                        let _ = conn.send(response_json.as_bytes().to_vec());
                                    }
                                }
                            }
                            ClientMessage::ReadReceipt { msg_id, to } =>
                            {
                                // forward read receipt to the original sender
                                let receipt = ServerMessage::ReadReceipt
                                {
                                    msg_id,
                                    from: key_hash_send.clone(),
                                };
                                let receipt_json = serde_json::to_string(&receipt).unwrap();

                                if let Some(conns) = router_send.connections.get(&to)
                                {
                                    for conn in conns.iter()
                                    {
                                        let _ = conn.send(receipt_json.as_bytes().to_vec());
                                    }
                                }
                                // if recipient is offline, read receipts are not queued — they're ephemeral
                            }
                            _ => {}
                        }
                    }
                }
                Message::Pong(_) => {}
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    // wait for either task to finish

    tokio::select!
    {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }

    // cleanup

    router.remove_connection(&key_hash, &device_id);
    info!("Disconnected: {} device: {}", key_hash, device_id);
}