pub mod auth;
pub mod draw;
pub mod pulsar_admin;
pub mod pulsar_listener;
pub mod update;

use crate::update::update;

use auth::{auth, read_config};
use clap::Parser;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Pulsar, TokioExecutor};
use pulsar_admin::{fetch_clusters, fetch_namespaces};
use pulsar_admin_sdk::apis::configuration::Configuration;
use pulsar_listener::TopicEvent;
use reqwest::header::HeaderValue;
use reqwest::Client;
use serde::Deserialize;
use std::path::PathBuf;
use std::time::Duration;
use std::{
    io,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread,
};
use update::{
    App, ConfirmedCommand, Consumers, Listening, Namespace, Namespaces, PulsarApp, Resource,
    Resources, SelectedPanel, Subscriptions, Tenant, Tenants, Topics,
};

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    //let _ = simple_logging::log_to_file("app.log", LevelFilter::Info);
    //log::info!("Starting the app");

    match run(args).await {
        Ok(_) => println!("bye!"),
        Err(error) => eprintln!("Failed unexpectedlly. Reason: {:?}", error),
    }
}

#[derive(Deserialize)]
struct ReleaseResponse {
    pub tag_name: String,
}

async fn fetch_latest_version(sender: Sender<AppEvent>) -> anyhow::Result<()> {
    println!("helo?");
    let client = Client::builder()
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("User-Agent", HeaderValue::from_static("lgm"));
            headers
        })
        .build()?;
    let body = client.get("https://api.github.com/repos/bloznelis/lgm/releases/latest").send().await.expect("pls");

    //println!("{:?}", body.text().await);

    let json = body.json::<ReleaseResponse>()
        .await.expect("pls2");
    //println!("{}", json.tag_name);
    //tokio::time::sleep(Duration::from_secs(1)).await;
    //println!("sending");

    sender.send(AppEvent::LatestVersion(json.tag_name))?;

    Ok(())
}

async fn run(args: Args) -> anyhow::Result<()> {
    let config_path = args.config.unwrap_or_else(|| {
        println!("Config not provided, reading from '$HOME/.config/lgm/config.toml'");

        #[allow(deprecated)] // XXX: Warning regarding Windows, we don't care now
        std::env::home_dir()
            .expect("Home dir not found")
            .join(".config/lgm/config.toml")
    });
    let config = read_config(config_path)?;

    let url = &config.pulsar_url.clone();

    let builder = match &config.auth {
        //TODO: Add token as auth here
        auth::Auth::Token { token: _ } => Pulsar::builder(url, TokioExecutor),
        auth::Auth::OAuth {
            issuer_url,
            audience,
            credentials_file_url,
        } => Pulsar::builder(url, TokioExecutor).with_auth_provider(
            OAuth2Authentication::client_credentials(OAuth2Params {
                issuer_url: issuer_url.clone(),
                credentials_url: credentials_file_url.clone(),
                audience: Some(audience.clone()),
                scope: None,
            }),
        ),
    };

    let pulsar = builder.build().await?;
    let pulsar = Arc::new(pulsar);

    let default_tenant = config.default_tenant.clone();

    let pulsar_admin_url = config.pulsar_admin_url.clone();
    let token = auth(config).await?;
    let conf = Configuration {
        base_path: format!("{}/admin/v2", pulsar_admin_url),
        bearer_access_token: Some(token.access_token.clone()),
        ..Configuration::default()
    };

    let (sender, receiver): (Sender<AppEvent>, Receiver<AppEvent>) = channel();
    let control_sender = sender.clone();
    let version_sender = sender.clone();
    //can we use tokio thread here?
    let _handle = thread::spawn(move || listen_input(control_sender));
    let _ = tokio::spawn(async move { fetch_latest_version(version_sender).await });
    let namespaces: Vec<Namespace> = fetch_namespaces(&default_tenant, &conf).await?;
    let cluster_name: String = fetch_clusters(&conf)
        .await?
        .first()
        .cloned()
        .unwrap_or("unknown cluster".to_string());

    let mut app = App {
        pulsar: PulsarApp {
            sender,
            client: pulsar,
            token,
            active_sub_handle: None,
        },
        confirmation_modal: None,
        input_modal: None,
        info_to_show: None,
        active_resource: Resource::Namespaces,
        resources: Resources {
            tenants: Tenants {
                tenants: vec![Tenant { name: default_tenant }],
                cursor: Some(0),
            },
            namespaces: Namespaces {
                namespaces,
                cursor: Some(0), // FIXME: will crash on empty namespaces
            },
            topics: Topics { topics: vec![], cursor: None },
            subscriptions: Subscriptions {
                subscriptions: vec![],
                cursor: None,
            },
            consumers: Consumers {
                consumers: vec![],
                cursor: None,
            },
            listening: Listening {
                messages: vec![],
                filtered_messages: vec![],
                panel: SelectedPanel::Left,
                cursor: None,
                search: None,
            },
        },
        pulsar_admin_cfg: conf,
        cluster_name,
        lgm_version: env!("CARGO_PKG_VERSION").to_string(),
        latest_lgm_version: None,
        receiver,
    };

    let mut stdout = io::stdout();

    execute!(stdout, EnterAlternateScreen)?;
    enable_raw_mode()?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    let result = update(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

pub enum ControlEvent {
    Enter,
    CycleSide,
    Yank,
    Back,
    Esc,
    BackSpace,
    ClearInput,
    Up,
    Search,
    Down,
    Terminate,
    Delete,
    Subscribe,
    Skip,
    Accept,
    Refuse,
    Seek,
}

pub enum ResetLength {
    OneHour,
    TwentyFourHours,
    Week,
}

pub enum AppEvent {
    Input(KeyCode),
    Control(ControlEvent),
    Command(ConfirmedCommand),
    SubscriptionEvent(TopicEvent),
    LatestVersion(String),
}

fn listen_input(sender: Sender<AppEvent>) {
    loop {
        let event = event::read().unwrap();

        let input_event = match &event {
            Event::Key(key) => Some(AppEvent::Input(key.code)),
            _ => None,
        };

        if let Some(input_event) = input_event {
            sender.send(input_event).unwrap()
        }

        let control_event = match &event {
            Event::Key(key) => match key.code {
                KeyCode::Char('a') if key.modifiers == KeyModifiers::CONTROL => {
                    Some(AppEvent::Control(ControlEvent::Accept))
                }
                KeyCode::Char('n') => Some(AppEvent::Control(ControlEvent::Refuse)),
                KeyCode::Char('c') | KeyCode::Char('q')
                    if key.modifiers == KeyModifiers::CONTROL =>
                {
                    Some(AppEvent::Control(ControlEvent::Terminate))
                }
                KeyCode::Char('d') if key.modifiers == KeyModifiers::CONTROL => {
                    Some(AppEvent::Control(ControlEvent::Delete))
                }
                KeyCode::Char('s') if key.modifiers == KeyModifiers::CONTROL => {
                    Some(AppEvent::Control(ControlEvent::Subscribe))
                }
                KeyCode::Char('p') if key.modifiers == KeyModifiers::CONTROL => {
                    Some(AppEvent::Control(ControlEvent::Skip))
                }
                KeyCode::Char('u') if key.modifiers == KeyModifiers::CONTROL => {
                    Some(AppEvent::Control(ControlEvent::ClearInput))
                }
                KeyCode::Char('s') => Some(AppEvent::Control(ControlEvent::Seek)),
                KeyCode::Tab => Some(AppEvent::Control(ControlEvent::CycleSide)),
                KeyCode::Enter => Some(AppEvent::Control(ControlEvent::Enter)),
                KeyCode::Char('h') | KeyCode::Left => Some(AppEvent::Control(ControlEvent::Back)),
                KeyCode::Esc => Some(AppEvent::Control(ControlEvent::Esc)),
                KeyCode::Backspace => Some(AppEvent::Control(ControlEvent::BackSpace)),
                KeyCode::Char('j') | KeyCode::Down => Some(AppEvent::Control(ControlEvent::Down)),
                KeyCode::Char('y') => Some(AppEvent::Control(ControlEvent::Yank)),
                KeyCode::Char('k') | KeyCode::Up => Some(AppEvent::Control(ControlEvent::Up)),
                KeyCode::Char('/') => Some(AppEvent::Control(ControlEvent::Search)),
                _ => None,
            },
            _ => None,
        };

        if let Some(control_event) = control_event {
            sender.send(control_event).unwrap()
        }
    }
}
