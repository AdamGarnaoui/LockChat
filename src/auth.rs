use pqcrypto_dilithium::dilithium3;
use pqcrypto_traits::sign::{PublicKey, DetachedSignature};
use base64::{engine::general_purpose::STANDARD, Engine};
use rand::RngExt;
use serde::{Serialize, Deserialize};
use sha2::{Sha256, Digest};

#[derive(Serialize, Deserialize)]
pub struct AuthChallenge
{
    pub nonce: String, // short for number once used
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
    rand::rng().fill(&mut nonce_bytes); // why did rand change their wording

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
    .map_err(|e| format!("Invalid public key: {}", e))?;

    let sig_bytes = STANDARD.decode(&response.signature)
    .map_err(|e| format!("Invalid signature: {}", e))?;

    let pk = dilithium3::PublicKey::from_bytes(&pk_bytes)
    .map_err(|_| "Invalid public key bytes".to_string())?;

    let sig = dilithium3::DetachedSignature::from_bytes(&sig_bytes)
    .map_err(|_| "Invalid signature bytes".to_string())?;

    let computed_hash = hash_public_key(&pk_bytes);

    if computed_hash != response.key_hash
    {
        return Err("key hash mismatch".to_string());
    }

    let nonce_bytes = STANDARD.decode(challenge_nonce)
    .map_err(|e| format!("Invalid nonce: {}", e))?;

    match dilithium3::verify_detached_signature(&sig, &nonce_bytes, &pk)
    {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }

}