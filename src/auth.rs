use reqwest::header::{HeaderMap, CONTENT_TYPE, ACCEPT};
use serde::{Deserialize, Serialize};
use std::fs;
use anyhow::Result;

#[derive(Deserialize)]
pub struct AuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub audience: String,
    pub token_url: String,
}

#[derive(Serialize, Deserialize)]
struct AuthData {
    grant_type: String,
    client_id: String,
    client_secret: String,
    audience: String,
}

#[derive(Serialize, Deserialize)]
pub struct Token {
    pub access_token: String,
}

pub async fn auth(cfg: AuthConfig) -> Result<Token, reqwest::Error> {
    let client = reqwest::Client::new();
    let auth_data = AuthData {
        grant_type: "client_credentials".to_string(),
        client_id: cfg.client_id,
        client_secret: cfg.client_secret,
        audience: cfg.audience
    };
    let auth_data = serde_urlencoded::to_string(&auth_data).expect("serialization issue");

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        "application/x-www-form-urlencoded".parse().unwrap(),
    );
    headers.insert(ACCEPT, "application/json".parse().unwrap());

    let response = client
        .post(cfg.token_url)
        .headers(headers)
        .body(auth_data)
        .send()
        .await?;

    let token: Token = response.json().await?;

    Ok(token)
}

pub fn read_config() -> Result<AuthConfig> {
    let contents = fs::read_to_string("config/auth.toml")?;
    let config: AuthConfig = toml::from_str(&contents)?;
    Ok(config)
}
