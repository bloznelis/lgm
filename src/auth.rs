use anyhow::Result;
use reqwest::{header::{HeaderMap, ACCEPT, CONTENT_TYPE}, Url};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

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
        issuer_url: String,
        audience: String,
        credentials_file_url: String,
    },
}

#[derive(Deserialize, Debug)]
struct OAuth2PrivateParams {
    client_id: String,
    client_secret: String,
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
            issuer_url,
            audience,
            credentials_file_url,
        } => {
            let client = reqwest::Client::new();

            let credentials_url = Url::parse(credentials_file_url.as_str())?;
            let creds: OAuth2PrivateParams =
                serde_json::from_str(fs::read_to_string(credentials_url.path())?.as_str())?;

            let auth_data = AuthData {
                grant_type: "client_credentials".to_string(),
                client_id: creds.client_id,
                client_secret: creds.client_secret,
                audience,
            };
            let auth_data = serde_urlencoded::to_string(&auth_data)?;

            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/x-www-form-urlencoded".parse()?);
            headers.insert(ACCEPT, "application/json".parse()?);

            let token_url = format!("{}/oauth/token", issuer_url);

            let response = client
                .post(token_url)
                .headers(headers)
                .body(auth_data)
                .send()
                .await?;

            response.json().await?
        }
    };

    Ok(token)
}

pub fn read_config(path: PathBuf) -> Result<Config> {
    let contents = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}
