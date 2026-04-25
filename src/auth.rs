use oqs::sig::{Algorithm, Sig};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngExt;
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

#[derive(Serialize, Deserialize)]
pub struct AuthChallenge
{
    pub nonce: String,
}

#[derive(Serialize, Deserialize)]
pub struct AuthResponse
{
    pub key_hash: String,
    pub public_key: String,
    pub signature: String,
    pub device_id: String,
}

pub fn generate_challenge() -> AuthChallenge
{
    let mut nonce_bytes = [0u8; 32];
    rand::rng().fill(&mut nonce_bytes);

    AuthChallenge
    {
        nonce: STANDARD.encode(nonce_bytes),
    }
}

pub fn hash_public_key(pk_bytes: &[u8]) -> String
{
    let mut hasher = Sha256::new();
    hasher.update(pk_bytes);
    hex::encode(hasher.finalize())
}

pub fn verify_auth(challenge_nonce: &str, response: &AuthResponse) -> Result<bool, String>
{
    let pk_bytes = STANDARD.decode(&response.public_key)
        .map_err(|e| format!("Invalid public key base64: {}", e))?;

    let sig_bytes = STANDARD.decode(&response.signature)
        .map_err(|e| format!("Invalid signature base64: {}", e))?;

    let computed_hash = hash_public_key(&pk_bytes);

    if computed_hash != response.key_hash
    {
        return Err(format!(
            "key hash mismatch: computed={}, received={}",
            &computed_hash[..16], &response.key_hash[..16]
        ));
    }

    let nonce_bytes = STANDARD.decode(challenge_nonce)
        .map_err(|e| format!("Invalid nonce base64: {}", e))?;

    let sigalg = Sig::new(Algorithm::Dilithium3)
        .map_err(|e| format!("Failed to init Dilithium3: {:?}", e))?;

    let pk = sigalg.public_key_from_bytes(&pk_bytes)
        .ok_or_else(|| format!(
            "Invalid public key length: got {} expected {}",
            pk_bytes.len(),
            sigalg.length_public_key()
        ))?;

    let signature = sigalg.signature_from_bytes(&sig_bytes)
        .ok_or_else(|| format!(
            "Invalid signature length: got {} expected max {}",
            sig_bytes.len(),
            sigalg.length_signature()
        ))?;

    match sigalg.verify(&nonce_bytes, &signature, &pk)
    {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}