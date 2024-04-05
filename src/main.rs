pub mod auth;
pub mod draw;
pub mod pulsar_admin;
pub mod pulsar_listener;
pub mod update;

use crate::update::update;

use auth::{auth, read_config};
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Pulsar, TokioExecutor};
use pulsar_admin::fetch_namespaces;
use pulsar_admin_sdk::apis::configuration::Configuration;
use pulsar_listener::TopicEvent;
use std::{
    io,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread,
};
use tokio::sync::Mutex;
use update::{App, Namespace, PulsarApp, Resource, Side, ConfirmedCommand};

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};

#[tokio::main]
async fn main() -> () {
    // env_logger::init();

    match run().await {
        Ok(_) => println!("bye!"),
        Err(error) => eprintln!("Failed unexpectedlly. Reason: {:?}", error),
    }
}

async fn run() -> anyhow::Result<()> {
    // let app_config = read_config("config/local.toml")?;
    let app_config = read_config("config/staging.toml")?;
    // let app_config = read_config("config/production.toml")?;

    let url = &app_config.pulsar_url.clone();

    let builder = match &app_config.auth {
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
    let pulsar = Arc::new(Mutex::new(pulsar));

    let default_tenant = app_config.default_tenant.clone();

    let pulsar_admin_url = app_config.pulsar_admin_url.clone();
    let token = auth(app_config).await?;
    let conf = Configuration {
        base_path: format!("{}/admin/v2", pulsar_admin_url),
        bearer_access_token: Some(token.access_token.clone()),
        ..Configuration::default()
    };

    let (sender, receiver): (Sender<AppEvent>, Receiver<AppEvent>) = channel();
    let control_sender = sender.clone();
    //can we use tokio thread here?
    let _handle = thread::spawn(move || listen_for_control(control_sender));
    let namespaces: Vec<Namespace> = fetch_namespaces(&default_tenant, &conf).await?;

    let mut app = App {
        pulsar: PulsarApp {
            receiver,
            sender,
            client: pulsar,
            token,
            active_sub_handle: None,
        },
        confirmation_modal: None,
        error_to_show: None,
        active_resource: Resource::Namespaces { namespaces },
        content_cursor: Some(0),
        last_cursor: None,
        last_tenant: Some(default_tenant),
        last_namespace: None,
        last_topic: None,
        pulsar_admin_cfg: conf,
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
    Up,
    Down,
    Terminate,
    Delete,
    Subscribe,
    Accept,
    Refuse,
    ResetSubscription(ResetLength),
}

pub enum ResetLength {
    OneHour,
    TwentyFourHours,
    Week,
}

pub enum AppEvent {
    Control(ControlEvent),
    Command(ConfirmedCommand),
    SubscriptionEvent(TopicEvent),
}

fn listen_for_control(sender: Sender<AppEvent>) {
    loop {
        let event = match event::read().unwrap() {
            Event::Key(key) => match key.code {
                KeyCode::Char('a')
                    if key.modifiers == KeyModifiers::CONTROL =>
                {
                    Some(AppEvent::Control(ControlEvent::Accept))
                }
                KeyCode::Char('n') => {
                    Some(AppEvent::Control(ControlEvent::Refuse))
                }
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
                KeyCode::Char('u') => Some(AppEvent::Control(ControlEvent::ResetSubscription(
                    ResetLength::OneHour,
                ))),
                KeyCode::Char('i') => Some(AppEvent::Control(ControlEvent::ResetSubscription(
                    ResetLength::TwentyFourHours,
                ))),
                KeyCode::Char('o') => Some(AppEvent::Control(ControlEvent::ResetSubscription(
                    ResetLength::Week,
                ))),
                KeyCode::Tab => Some(AppEvent::Control(ControlEvent::CycleSide)),
                KeyCode::Enter => Some(AppEvent::Control(ControlEvent::Enter)),
                KeyCode::Char('h') | KeyCode::Left | KeyCode::Esc => {
                    Some(AppEvent::Control(ControlEvent::Back))
                }
                KeyCode::Backspace => Some(AppEvent::Control(ControlEvent::Back)),
                KeyCode::Char('j') | KeyCode::Down => Some(AppEvent::Control(ControlEvent::Down)),
                KeyCode::Char('y') => Some(AppEvent::Control(ControlEvent::Yank)),
                KeyCode::Char('k') | KeyCode::Up => Some(AppEvent::Control(ControlEvent::Up)),
                _ => None,
            },
            _ => None,
        };

        if let Some(event) = event {
            sender.send(event).unwrap()
        }
    }
}
