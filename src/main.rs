pub mod auth;
mod pulsar_admin;

use anyhow::{anyhow, Result};
use auth::{auth, read_config};
use clipboard::{ClipboardContext, ClipboardProvider};
use core::fmt;
use futures::TryStreamExt;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::consumer::InitialPosition;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use pulsar_admin::{fetch_namespaces, fetch_subscriptions, fetch_tenants, fetch_topics};
use pulsar_admin_sdk::apis::configuration::Configuration;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Wrap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Stdout;
use std::panic;
use std::{
    io,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread,
    time::Duration,
};
use tokio::sync::{oneshot, Mutex};

use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    prelude::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, BorderType, Borders, List, ListState, Padding, Paragraph},
    Frame, Terminal,
};

#[derive(Debug)]
enum Resource {
    Tenants,
    Namespaces,
    Topics,
    Subscriptions,
    Listening,
}

impl std::fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

struct ErrorToShow {
    message: String,
}

impl ErrorToShow {
    fn new(message: String) -> Self {
        ErrorToShow { message }
    }
}

#[derive(Clone)]
pub enum Content {
    Tenant {
        name: String,
    },
    Topic {
        name: String,
        fqn: String,
    },
    Namespace {
        name: String,
    },
    Subscription {
        name: String,
    },
    SubMessage {
        body: String,
        properties: Vec<String>,
    },
}

fn get_topic_fqn(content: &Content) -> Option<String> {
    match content {
        Content::Tenant { .. } => None,
        Content::Topic { name: _, fqn } => Some(fqn.to_string()),
        Content::Namespace { name: _ } => None,
        Content::Subscription { .. } => None,
        Content::SubMessage { .. } => None,
    }
}
fn get_name(content: Content) -> Option<String> {
    match content {
        Content::Tenant { name } => Some(name),
        Content::Topic { name, .. } => Some(name),
        Content::Namespace { name } => Some(name),
        Content::Subscription { name } => Some(name),
        Content::SubMessage { .. } => None,
    }
}

fn get_show(content: Content) -> Option<String> {
    match content {
        Content::Tenant { name } => Some(name),
        Content::Topic { name, .. } => Some(name),
        Content::Namespace { name } => Some(name),
        Content::Subscription { name } => Some(name),
        Content::SubMessage { body, .. } => Some(body),
    }
}

fn get_show_alternative(content: Content) -> Option<String> {
    match content {
        Content::Tenant { name } => Some(name),
        Content::Topic { name, .. } => Some(name),
        Content::Namespace { name } => Some(name),
        Content::Subscription { name } => Some(name),
        Content::SubMessage {
            body: _,
            properties,
        } => Some(properties.join("\n")),
    }
}

struct App {
    error_to_show: Option<ErrorToShow>,
    active_resource: Resource,
    contents: Vec<Content>,
    content_cursor: Option<usize>,
    last_cursor: Option<usize>,
    last_tenant: Option<String>,
    last_namespace: Option<String>,
    last_topic: Option<String>,
    active_sub_handle: Option<tokio::sync::oneshot::Sender<()>>,
    pulsar_admin_cfg: Configuration,
    selected_side: Side,
}

#[derive(Serialize, Deserialize)]
struct TopicEvent {
    body: Value,
    properties: Vec<String>,
}

impl DeserializeMessage for TopicEvent {
    type Output = Result<TopicEvent, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        let props = payload
            .metadata
            .properties
            .iter()
            .map(|keyvalue| format!("{}:{}", keyvalue.key, keyvalue.value))
            .collect::<Vec<String>>();
        serde_json::from_slice::<Value>(&payload.data).map(|content| TopicEvent {
            body: content,
            properties: props,
        })
    }
}

enum ControlEvent {
    Enter,
    CycleSide,
    Yank,
    Back,
    Up,
    Down,
    Terminate,
    Subscribe,
}

enum AppEvent {
    Control(ControlEvent),
    SubscriptionEvent(TopicEvent),
}

async fn listen_to_topic(
    topic_fqn: String,
    event_sender: Sender<AppEvent>,
    pulsar: Arc<Mutex<Pulsar<TokioExecutor>>>,
    mut control_channel: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let mut consumer: Consumer<TopicEvent, TokioExecutor> = pulsar
        .lock()
        .await
        .consumer()
        .with_options(
            pulsar::ConsumerOptions::default()
                .durable(false)
                .with_initial_position(InitialPosition::Latest),
        )
        .with_topic(topic_fqn)
        .with_consumer_name("lgm")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("lgm_subscription")
        .build()
        .await?;

    loop {
        tokio::select! {
            msg = consumer.try_next() => {
                match msg {
                    Ok(Some(message)) => {
                        let topic_event = message.deserialize()?;
                        let _ = event_sender.send(AppEvent::SubscriptionEvent(topic_event));

                        consumer.ack(&message).await?;

                    },
                    Ok(None) => break,
                    Err(e) => {
                        println!("Error in topic consumer, {:?}", e);
                        break;
                    }
                }
            },
            _ = &mut control_channel => {
                // cancel!
                break;
            }
        }
    }

    consumer.close().await?;

    Ok({})
}

fn listen_for_control(sender: Sender<AppEvent>) {
    loop {
        let event = match event::read().unwrap() {
            Event::Key(key) => match key.code {
                KeyCode::Char('c') | KeyCode::Char('q')
                    if key.modifiers == KeyModifiers::CONTROL =>
                {
                    Some(AppEvent::Control(ControlEvent::Terminate))
                }
                KeyCode::Char('s') if key.modifiers == KeyModifiers::CONTROL => {
                    Some(AppEvent::Control(ControlEvent::Subscribe))
                }
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

#[tokio::main]
async fn main() -> () {
    env_logger::init();

    match run().await {
        Ok(_) => println!("bye!"),
        Err(error) => eprintln!("Failed unexpectedlly. Reason: {:?}", error.source()),
    }
}

async fn run() -> anyhow::Result<()> {
    let mut stdout = io::stdout();

    execute!(stdout, EnterAlternateScreen)?;
    enable_raw_mode()?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    let result = update(&mut terminal).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

enum Side {
    Left,
    Right { scroll_offset: u16 },
}

async fn update(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> anyhow::Result<()> {
    let app_config = read_config("config/staging.toml")?;

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
    let namespaces: Vec<Content> = fetch_namespaces(&default_tenant, &conf).await?;

    let mut app = App {
        error_to_show: None,
        active_resource: Resource::Namespaces,
        contents: namespaces,
        content_cursor: Some(0),
        last_cursor: None,
        last_tenant: Some(default_tenant),
        last_namespace: None,
        last_topic: None,
        active_sub_handle: None,
        pulsar_admin_cfg: conf,
        selected_side: Side::Left,
    };

    loop {
        terminal.draw(|f| draw(f, &app).expect("Failed to draw"))?;
        if let Ok(event) = receiver.recv_timeout(Duration::from_millis(100)) {
            app.error_to_show = None;
            match event {
                AppEvent::Control(ControlEvent::CycleSide) => {
                    if let Resource::Listening = app.active_resource {
                        match app.selected_side {
                            Side::Left => app.selected_side = Side::Right { scroll_offset: 0 },
                            Side::Right { .. } => app.selected_side = Side::Left,
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Yank) => {
                    if let Resource::Listening = app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            if let Some(content) = app.contents.get(cursor) {
                                let content = get_show(content.clone()).unwrap();
                                //TODO: Create this once and pass it around
                                let mut ctx: ClipboardContext = ClipboardProvider::new().unwrap();
                                ctx.set_contents(content).unwrap();
                            }
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Subscribe) => {
                    if let Resource::Topics = app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            let topic = app.contents[cursor].clone();
                            app.active_resource = Resource::Listening;
                            app.content_cursor = None;
                            app.contents = Vec::new();
                            let new_pulsar = pulsar.clone();
                            let new_sender = sender.clone();
                            let (tx, rx) = oneshot::channel::<()>();
                            app.active_sub_handle = Some(tx);
                            let _sub_handle = tokio::task::spawn(async move {
                                listen_to_topic(
                                    get_topic_fqn(&topic).unwrap(),
                                    new_sender,
                                    new_pulsar,
                                    rx,
                                )
                                .await
                            });
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Up) => {
                    if let (Resource::Listening, Side::Right { scroll_offset }) =
                        (&app.active_resource, &app.selected_side)
                    {
                        app.selected_side = Side::Right {
                            scroll_offset: scroll_offset.saturating_sub(1),
                        }
                    } else {
                        app.content_cursor = match app.content_cursor {
                            Some(cursor) => {
                                if cursor == 0 {
                                    Some(app.contents.len().saturating_sub(1))
                                } else {
                                    Some(cursor - 1)
                                }
                            }
                            None => None,
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Down) => {
                    if let (Resource::Listening, Side::Right { scroll_offset }) =
                        (&app.active_resource, &app.selected_side)
                    {
                        app.selected_side = Side::Right {
                            scroll_offset: scroll_offset + 1,
                        }
                    } else {
                        app.content_cursor = match app.content_cursor {
                            Some(cursor) => {
                                if cursor == app.contents.len().saturating_sub(1) {
                                    Some(0)
                                } else {
                                    Some(cursor + 1)
                                }
                            }
                            None => Some(0),
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Back) => {
                    match app.active_resource {
                        Resource::Tenants => {}
                        Resource::Namespaces => {
                            let tenants = fetch_tenants(&token).await;
                            match tenants {
                                Ok(tenants) => {
                                    if !tenants.is_empty() {
                                        app.content_cursor = app.last_cursor.or(Some(0))
                                    } else {
                                        app.content_cursor = None;
                                    };
                                    app.contents = tenants;
                                    app.active_resource = Resource::Tenants;
                                }
                                Err(err) => {
                                    app.error_to_show = Some(ErrorToShow::new(format!(
                                        "Failed to fetch tenants :[\n {:?}",
                                        err
                                    )));
                                }
                            }
                        }
                        Resource::Topics => {
                            if let Some(last_tenant) = &app.last_tenant {
                                let namespaces =
                                    fetch_namespaces(last_tenant, &app.pulsar_admin_cfg).await;

                                match namespaces {
                                    Ok(namespaces) => {
                                        if !namespaces.is_empty() {
                                            app.content_cursor = app.last_cursor.or(Some(0))
                                        } else {
                                            app.content_cursor = None;
                                        };
                                        app.contents = namespaces;
                                        app.active_resource = Resource::Namespaces;
                                    }
                                    Err(err) => {
                                        app.error_to_show = Some(ErrorToShow::new(format!(
                                            "Failed to fetch namespaces :[\n {:?}",
                                            err
                                        )));
                                    }
                                }
                            }
                        }
                        Resource::Subscriptions => {
                            if let Some(last_namespace) = &app.last_namespace {
                                let topics = fetch_topics(
                                    &app.last_tenant.clone().unwrap(),
                                    last_namespace,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match topics {
                                    Ok(topics) => {
                                        if !topics.is_empty() {
                                            app.content_cursor = app.last_cursor.or(Some(0))
                                        } else {
                                            app.content_cursor = None;
                                        };

                                        app.contents = topics;
                                        app.active_resource = Resource::Topics;
                                    }
                                    Err(err) => {
                                        app.error_to_show = Some(ErrorToShow::new(format!(
                                            "Failed to fetch topics :[\n {:?}",
                                            err
                                        )));
                                    }
                                }
                            }
                        }
                        Resource::Listening => {
                            if let Some(last_namespace) = &app.last_namespace {
                                let topics = fetch_topics(
                                    &app.last_tenant.clone().unwrap(),
                                    last_namespace,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match topics {
                                    Ok(topics) => {
                                        if !topics.is_empty() {
                                            app.content_cursor = app.last_cursor.or(Some(0))
                                        } else {
                                            app.content_cursor = None;
                                        };

                                        app.contents = topics;
                                        app.active_resource = Resource::Topics;
                                        if let Some(sub) = app.active_sub_handle {
                                            sub.send({}).map_err(|()| {
                                    anyhow!("Failed to send termination singal to subscription")
                                })?
                                        }
                                        app.active_sub_handle = None
                                    }
                                    Err(err) => {
                                        app.error_to_show = Some(ErrorToShow::new(format!(
                                            "Failed to fetch topics :[\n {:?}",
                                            err
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
                AppEvent::SubscriptionEvent(event) => {
                    if let Resource::Listening = app.active_resource {
                        app.contents.push(Content::SubMessage {
                            body: serde_json::to_string(&event.body)?,
                            properties: event.properties,
                        });
                        if app.content_cursor.is_none() {
                            app.content_cursor = Some(0)
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Terminate) => break,
                AppEvent::Control(ControlEvent::Enter) => match app.active_resource {
                    Resource::Tenants => {
                        if let Some(cursor) = app.content_cursor {
                            if !app.contents.is_empty() {
                                let tenant = get_name(app.contents[cursor].clone()).unwrap();
                                let namespaces =
                                    fetch_namespaces(&tenant, &app.pulsar_admin_cfg).await;

                                match namespaces {
                                    Ok(namespaces) => {
                                        app.last_cursor = app.content_cursor;
                                        if !namespaces.is_empty() {
                                            app.content_cursor = Some(0);
                                        } else {
                                            app.content_cursor = None;
                                        };
                                        app.contents = namespaces;
                                        app.active_resource = Resource::Namespaces;
                                        app.last_tenant = Some(tenant)
                                    }
                                    Err(err) => {
                                        app.error_to_show = Some(ErrorToShow::new(format!(
                                            "Failed to fetch namespaces :[\n {:?}",
                                            err
                                        )));
                                    }
                                }
                            }
                        }
                    }
                    Resource::Namespaces => {
                        if let Some(cursor) = app.content_cursor {
                            let namespace = get_name(app.contents[cursor].clone()).unwrap();
                            let topics = fetch_topics(
                                &app.last_tenant.clone().unwrap(),
                                &namespace,
                                &app.pulsar_admin_cfg,
                            )
                            .await;

                            match topics {
                                Ok(topics) => {
                                    app.last_cursor = app.content_cursor;
                                    if !topics.is_empty() {
                                        app.content_cursor = Some(0);
                                    } else {
                                        app.content_cursor = None;
                                    };
                                    app.content_cursor = Some(0);
                                    app.contents = topics;
                                    app.active_resource = Resource::Topics;
                                    app.last_namespace = Some(namespace)
                                }
                                Err(err) => {
                                    app.error_to_show = Some(ErrorToShow::new(format!(
                                        "Failed to fetch topics :[\n {:?}",
                                        err
                                    )));
                                }
                            }
                        }
                    }
                    Resource::Topics => {
                        if let Some(cursor) = app.content_cursor {
                            if !app.contents.is_empty() {
                                let topic = get_name(app.contents[cursor].clone()).unwrap();
                                let subscriptions = fetch_subscriptions(
                                    &app.last_tenant.clone().unwrap(),
                                    app.last_namespace.as_ref().unwrap(),
                                    &topic,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match subscriptions {
                                    Ok(subscriptions) => {
                                        app.last_cursor = app.content_cursor;
                                        if !subscriptions.is_empty() {
                                            app.content_cursor = Some(0);
                                        } else {
                                            app.content_cursor = None;
                                        };
                                        app.contents = subscriptions;
                                        app.active_resource = Resource::Subscriptions;
                                        app.last_topic = Some(topic)
                                    }
                                    Err(err) => {
                                        app.error_to_show = Some(ErrorToShow::new(format!(
                                            "Failed to fetch subscriptions :[\n {:?}",
                                            err
                                        )));
                                    }
                                }
                            }
                        }
                    }
                    Resource::Subscriptions => {}
                    Resource::Listening => {}
                },
            }
        }
    }

    Ok({})
}

struct HeaderLayout {
    help: Rect,
    logo: Rect,
}

struct MainLayout {
    left: Rect,
    right: Option<Rect>,
}

struct LayoutChunks {
    header: HeaderLayout,
    message: Option<Rect>,
    main: MainLayout,
}

fn prep_main_layout(rect: Rect, is_split: bool) -> MainLayout {
    if is_split {
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
            .split(rect);

        MainLayout {
            left: main_chunks[0],
            right: Some(main_chunks[1]),
        }
    } else {
        let main_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(100)])
            .split(rect);

        MainLayout {
            left: main_chunks[0],
            right: None,
        }
    }
}

fn draw(frame: &mut Frame, app: &App) -> anyhow::Result<()> {
    let layout_chunks = match app.error_to_show {
        Some(_) => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(7),
                    Constraint::Length(4),
                    Constraint::Percentage(100),
                ])
                .split(frame.size());

            let header_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[0]);

            let main = prep_main_layout(
                chunks[2],
                matches!(app.active_resource, Resource::Listening),
            );

            LayoutChunks {
                header: HeaderLayout {
                    help: header_chunks[0],
                    logo: header_chunks[1],
                },
                message: Some(chunks[1].into()),
                main,
            }
        }
        None => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(7), Constraint::Percentage(100)])
                .split(frame.size());

            let header_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[0]);

            let main = prep_main_layout(
                chunks[1],
                matches!(app.active_resource, Resource::Listening),
            );

            LayoutChunks {
                header: HeaderLayout {
                    help: header_chunks[0],
                    logo: header_chunks[1],
                },
                message: None,
                main,
            }
        }
    };

    let header = Paragraph::new(
        r#"
  .-.    .-.    .-.    _     ____ __  __,
 /   \  /   \  /   \  | |   / ___|  \/  |
| o o || o o || o o | | |  | |  _| |\/| |
|  ^  ||  ^  ||  ^  | | |__| |_| | |  | |
 \___/  \___/  \___/  |_____\____|_|  |_|
"#,
    )
    .alignment(Alignment::Right)
    .style(Style::default().fg(Color::LightGreen));

    let content_borders = match app.active_resource {
        Resource::Listening => match app.selected_side {
            Side::Left => BorderType::Double,
            Side::Right { .. } => BorderType::Plain,
        },
        _ => BorderType::Plain,
    };

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(content_borders)
        .title(app.active_resource.to_string())
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(
        app.contents
            .iter()
            .flat_map(|content| get_show(content.clone()))
            .map(|content| {
                if content.len() > 300 {
                    format!("{}...", &content[..300])
                } else {
                    content
                }
            }),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(app.content_cursor);

    let help_block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(1, 1, 1, 1));

    let help_item_bac = HelpItem::new("<esc>", "back");
    let help_item_listen = HelpItem::new("<c-s>", "listen");
    let help_item_yank = HelpItem::new("y", "copy to clipboard");
    let help_cycle_side = HelpItem::new("<tab>", "cycle selected side");

    let help: Vec<HelpItem> = match &app.active_resource {
        Resource::Tenants => vec![HelpItem::new("<enter>", "namespaces")],
        Resource::Namespaces => vec![help_item_bac, HelpItem::new("<enter>", "topics")],
        Resource::Topics => vec![
            help_item_bac,
            help_item_listen,
            HelpItem::new("<enter>", "subscriptions"),
        ],
        Resource::Subscriptions => vec![help_item_bac],
        Resource::Listening => vec![help_item_bac, help_cycle_side, help_item_yank],
    };
    let help_list = List::new(help).block(help_block);

    frame.render_widget(help_list, layout_chunks.header.help);
    frame.render_widget(header, layout_chunks.header.logo);

    if let Some(error_to_show) = app.error_to_show.as_ref() {
        if let Some(rect_for_error) = layout_chunks.message {
            let block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Thick);
            let error_message_paragraph = Paragraph::new(error_to_show.message.clone())
                .alignment(Alignment::Center)
                .style(Style::default().fg(Color::Red))
                .block(block);

            frame.render_widget(error_message_paragraph, rect_for_error)
        }
    }

    match app.active_resource {
        Resource::Listening => {
            let full = app
                .content_cursor
                .and_then(|cursor| app.contents.get(cursor))
                .and_then(|content| get_show(content.clone()))
                .map(|content| serde_json::from_str::<serde_json::Value>(&content).unwrap())
                .map(|content| serde_json::to_string_pretty(&content).unwrap());

            let selected_alternative = app
                .content_cursor
                .and_then(|cursor| app.contents.get(cursor))
                .and_then(|content| get_show_alternative(content.clone()));

            let right_block = Block::default()
                .borders(Borders::ALL)
                .border_type(if matches!(app.selected_side, Side::Right { .. }) {
                    BorderType::Double
                } else {
                    BorderType::Plain
                })
                .title("Preview")
                .title_alignment(Alignment::Center)
                .title_style(Style::default().fg(Color::Green))
                .padding(Padding::new(2, 2, 1, 1));

            let content =
                selected_alternative.and_then(|alt| full.map(|full| alt + "\n\n" + &full.clone()));

            let scroll_offset = match app.selected_side {
                Side::Left => (0, 0),
                Side::Right { scroll_offset } => (scroll_offset, 0),
            };

            let right_block_content =
                Paragraph::new(content.unwrap_or(String::from("nothing to show")))
                    .block(right_block)
                    .wrap(Wrap { trim: false })
                    .scroll(scroll_offset);

            frame.render_widget(right_block_content, layout_chunks.main.right.unwrap());
        }
        _ => {}
    };

    frame.render_stateful_widget(content_list, layout_chunks.main.left, &mut state);

    Ok({})
}

#[derive(Clone)]
struct HelpItem {
    keybind: String,
    description: String,
}

impl HelpItem {
    fn new(key: &str, desc: &str) -> HelpItem {
        HelpItem {
            keybind: key.to_string(),
            description: desc.to_string(),
        }
    }
}

impl From<HelpItem> for Line<'_> {
    fn from(value: HelpItem) -> Self {
        Line::from(vec![
            Span::raw(value.keybind).style(Style::default().fg(Color::Green)),
            Span::raw(" "),
            Span::raw(value.description),
        ])
    }
}

impl From<HelpItem> for Text<'_> {
    fn from(value: HelpItem) -> Self {
        Text::from(Into::<Line>::into(value))
    }
}
