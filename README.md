# LockChat

End to end chat relay server. Relays encrypted messages between clients.

Uses WebSockets over TLS and Dilithium3 for post-quantum auth.

## Features

- Post-quantum auth (Dilithium3)
- Encrypted payload relay only
- Offline message queue with persistence
- Multi-device support
- Rate limiting
- Delivery status + read receipts
- Online status checks

## Requirements

- Rust (edition 2021)
- TLS cert + key

## Run

```bash
export LOCKCHAT_TLS_CERT=/path/to/cert.pem
export LOCKCHAT_TLS_KEY=/path/to/key.pem

cargo run --release
