use anyhow::Result;
use reqwest::header::{HeaderMap, ACCEPT, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub pulsar_url: String,
    pub pulsar_admin_url: String,
    pub default_tenant: String,
    pub auth: Auth,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", content = "args")]
pub enum Auth {
    Token {
        token: String,
    },
    OAuth {
        client_id: String,
        client_secret: String,
        issuer_url: String,
        audience: String,
        token_url: String,
        credentials_file_url: String,
    },
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

pub async fn auth(cfg: Config) -> anyhow::Result<Token> {
    let token = match cfg.auth {
        Auth::Token { token } => Token {
            access_token: token,
        },
        Auth::OAuth {
            client_id,
            client_secret,
            issuer_url,
            audience,
            token_url,
            credentials_file_url,
        } => {
            let client = reqwest::Client::new();
            let auth_data = AuthData {
                grant_type: "client_credentials".to_string(),
                client_id,
                client_secret,
                audience,
            };
            let auth_data = serde_urlencoded::to_string(&auth_data)?;

            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/x-www-form-urlencoded".parse()?);
            headers.insert(ACCEPT, "application/json".parse()?);

            let response = client
                .post(token_url)
                .headers(headers)
                .body(auth_data)
                .send()
                .await?;

            let token: Token = response.json().await?;
            token
        }
    };

    Ok(token)
}

pub fn read_config(path: &str) -> Result<Config> {
    let contents = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}
