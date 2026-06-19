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
    Message
    {
        msg_id: String,
        from: String,
        to: String,
        payload: String,
    },
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

pub enum OutboundMessage
{
    Raw(String),
    Envelope(Vec<u8>),
}

fn to_json(message: &ServerMessage) -> Option<String>
{
    serde_json::to_string(message).ok()
}

fn short_id(value: &str) -> String
{
    value.chars().take(12).collect()
}

fn valid_device_id(device_id: &str, max_len: usize) -> bool
{
    !device_id.is_empty()
        && device_id.len() <= max_len
        && device_id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

pub async fn handle_socket(
    socket: WebSocket,
    router: Arc<Router>,
    message_limiter: Arc<RateLimiter>,
    connection_limiter: Arc<RateLimiter>,
    remote_addr: String,
)
{
    if !connection_limiter.check(&remote_addr)
    {
        warn!("Connection rate limited: {}", remote_addr);
        return;
    }

    let (mut ws_sender, mut ws_receiver) = socket.split();

    let challenge = generate_challenge();
    let challenge_msg = match to_json(&ServerMessage::Challenge
    {
        nonce: challenge.nonce.clone(),
    })
    {
        Some(msg) => msg,
        None => return,
    };

    if ws_sender.send(Message::Text(challenge_msg.into())).await.is_err()
    {
        warn!("Failed to send challenge");
        return;
    }

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

    match verify_auth(&challenge.nonce, &auth_response)
    {
        Ok(true) => {}
        _ =>
        {
            if let Some(result) = to_json(&ServerMessage::AuthResult
            {
                success: false,
                error: Some("Authentication failed".to_string()),
            })
            {
                let _ = ws_sender.send(Message::Text(result.into())).await;
            }
            warn!("Auth failed for {}", short_id(&auth_response.key_hash));
            return;
        }
    }

    let key_hash = auth_response.key_hash.clone();
    let device_id = auth_response.device_id.clone();

    if !valid_device_id(&device_id, router.config.max_device_id_len)
    {
        if let Some(result) = to_json(&ServerMessage::AuthResult
        {
            success: false,
            error: Some("Invalid device id".to_string()),
        })
        {
            let _ = ws_sender.send(Message::Text(result.into())).await;
        }
        return;
    }

    let pk_bytes = match base64::engine::general_purpose::STANDARD.decode(&auth_response.public_key)
    {
        Ok(pk_bytes) => pk_bytes,
        Err(_) =>
        {
            if let Some(result) = to_json(&ServerMessage::AuthResult
            {
                success: false,
                error: Some("Invalid public key".to_string()),
            })
            {
                let _ = ws_sender.send(Message::Text(result.into())).await;
            }
            return;
        }
    };

    if let Err(err) = router.validate_and_store_public_key(&key_hash, pk_bytes).await
    {
        if let Some(result) = to_json(&ServerMessage::AuthResult
        {
            success: false,
            error: Some(err),
        })
        {
            let _ = ws_sender.send(Message::Text(result.into())).await;
        }
        return;
    }

    if let Some(result) = to_json(&ServerMessage::AuthResult
    {
        success: true,
        error: None,
    })
    {
        let _ = ws_sender.send(Message::Text(result.into())).await;
    }
    else
    {
        return;
    }

    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(256);
    let self_tx = tx.clone();
    let conn = Connection::new(device_id.clone(), tx);
    router.register_connection(&key_hash, conn);
    info!("Authenticated: {} device: {}", short_id(&key_hash), device_id);

    let queued = router.flush_queue(&key_hash);
    if !queued.is_empty()
    {
        let msgs: Vec<String> = queued.iter()
            .filter_map(|m|
            {
                if let Ok(envelope) = serde_json::from_slice::<Envelope>(m)
                {
                    let server_msg = ServerMessage::Message
                    {
                        msg_id: envelope.msg_id,
                        from: envelope.from,
                        to: envelope.to,
                        payload: envelope.payload,
                    };
                    to_json(&server_msg)
                }
                else
                {
                    String::from_utf8(m.clone()).ok()
                }
            })
            .collect();

        if let Some(flush_msg) = to_json(&ServerMessage::QueuedMessages
        {
            messages: msgs.clone(),
        })
        {
            let _ = ws_sender.send(Message::Text(flush_msg.into())).await;
        }

        info!("Flushed {} queued messages to {}", msgs.len(), short_id(&key_hash));
    }

    let router_send = router.clone();
    let key_hash_send = key_hash.clone();
    let max_message_size = router.config.max_message_size;
    let ping_interval = router.config.ping_interval;
    let rate_limit_window = router.config.rate_limit_window_secs;

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
                        Some(outbound) =>
                        {
                            let text = match outbound
                            {
                                OutboundMessage::Raw(s) => s,
                                OutboundMessage::Envelope(bytes) =>
                                {
                                    if let Ok(envelope) = serde_json::from_slice::<Envelope>(&bytes)
                                    {
                                        let msg = ServerMessage::Message
                                        {
                                            msg_id: envelope.msg_id,
                                            from: envelope.from,
                                            to: envelope.to,
                                            payload: envelope.payload,
                                        };
                                        match to_json(&msg)
                                        {
                                            Some(json) => json,
                                            None => continue,
                                        }
                                    }
                                    else
                                    {
                                        match String::from_utf8(bytes)
                                        {
                                            Ok(s) => s,
                                            Err(_) => continue,
                                        }
                                    }
                                }
                            };

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

    let mut recv_task = tokio::spawn(async move
    {
        while let Some(Ok(msg)) = ws_receiver.next().await
        {
            match msg
            {
                Message::Text(text) =>
                {
                    if text.len() > max_message_size
                    {
                        if let Some(err) = to_json(&ServerMessage::Error
                        {
                            message: "Message too large".to_string(),
                        })
                        {
                            let _ = self_tx.try_send(OutboundMessage::Raw(err));
                        }
                        continue;
                    }

                    if !message_limiter.check(&key_hash_send)
                    {
                        if let Some(limited) = to_json(&ServerMessage::RateLimited
                        {
                            retry_after_secs: rate_limit_window,
                        })
                        {
                            let _ = self_tx.try_send(OutboundMessage::Raw(limited));
                        }
                        continue;
                    }

                    let client_msg = match serde_json::from_str::<ClientMessage>(&text)
                    {
                        Ok(client_msg) => client_msg,
                        Err(_) =>
                        {
                            if let Some(err) = to_json(&ServerMessage::Error
                            {
                                message: "Malformed message".to_string(),
                            })
                            {
                                let _ = self_tx.try_send(OutboundMessage::Raw(err));
                            }
                            continue;
                        }
                    };

                    match client_msg
                    {
                        ClientMessage::Send(envelope) =>
                        {
                            if envelope.from != key_hash_send
                            {
                                if let Some(err) = to_json(&ServerMessage::Error
                                {
                                    message: "Invalid sender".to_string(),
                                })
                                {
                                    let _ = self_tx.try_send(OutboundMessage::Raw(err));
                                }
                                continue;
                            }

                            let status = router_send.route_message(&envelope);

                            if let Some(status_json) = to_json(&ServerMessage::DeliveryStatus
                            {
                                msg_id: status.msg_id,
                                status: status.status,
                            })
                            {
                                let _ = self_tx.try_send(OutboundMessage::Raw(status_json));
                            }
                        }
                        ClientMessage::DeliveryStatus { msg_id, status } =>
                        {
                            info!("Delivery status received: {} - {}", msg_id, status);
                        }
                        ClientMessage::IsOnline { key_hash } =>
                        {
                            let online = router_send.is_online(&key_hash);
                            if let Some(response_json) = to_json(&ServerMessage::OnlineStatus
                            {
                                key_hash,
                                online,
                            })
                            {
                                let _ = self_tx.try_send(OutboundMessage::Raw(response_json));
                            }
                        }
                        ClientMessage::ReadReceipt { msg_id, to } =>
                        {
                            if !router_send.validate_read_receipt(&msg_id, &key_hash_send, &to)
                            {
                                if let Some(err) = to_json(&ServerMessage::Error
                                {
                                    message: "Invalid read receipt".to_string(),
                                })
                                {
                                    let _ = self_tx.try_send(OutboundMessage::Raw(err));
                                }
                                continue;
                            }

                            if let Some(receipt_json) = to_json(&ServerMessage::ReadReceipt
                            {
                                msg_id,
                                from: key_hash_send.clone(),
                            })
                            {
                                if let Some(conns) = router_send.connections.get(&to)
                                {
                                    for conn in conns.iter()
                                    {
                                        let _ = conn.send_raw(receipt_json.clone());
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Message::Pong(_) => {}
                Message::Close(_) => break,
                _ => {}
            }
        }
    });

    tokio::select!
    {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }

    router.remove_connection(&key_hash, &device_id);
    info!("Disconnected: {} device: {}", short_id(&key_hash), device_id);
}
