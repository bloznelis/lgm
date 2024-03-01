pub mod auth;

use anyhow::{anyhow, Result};
use auth::{auth, read_config, Token};
use core::fmt;
use futures::TryStreamExt;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::consumer::InitialPosition;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use pulsar_admin_sdk::apis::configuration::Configuration;
use pulsar_admin_sdk::apis::namespaces_api::namespaces_get_tenant_namespaces;
use pulsar_admin_sdk::apis::namespaces_api::namespaces_get_topics;
use pulsar_admin_sdk::apis::persistent_topic_api::persistent_topics_get_subscriptions;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Wrap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Stdout;
use std::panic;
use std::rc::Rc;
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

#[derive(PartialEq, Debug)]
enum Element {
    ResourceSelector,
    ContentView,
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
enum Content {
    Tenant { name: String },
    Topic { name: String, fqn: String },
    Namespace { name: String },
    Subscription { name: String },
    SubMessage { value: String },
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
        Content::SubMessage { value } => Some(value),
    }
}

struct App {
    error_to_show: Option<ErrorToShow>,
    active_element: Element,
    active_resource: Resource,
    contents: Vec<Content>,
    content_cursor: Option<usize>,
    last_cursor: Option<usize>,
    input: String,
    last_tenant: Option<String>,
    last_namespace: Option<String>,
    last_topic: Option<String>,
    active_sub_handle: Option<tokio::sync::oneshot::Sender<()>>,
    pulsar_admin_cfg: Configuration,
}

#[derive(Serialize, Deserialize)]
struct TopicEvent {
    content: Value,
}

impl DeserializeMessage for TopicEvent {
    type Output = Result<TopicEvent, serde_json::Error>;
    // type Output = Result<TopicEvent, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice::<Value>(&payload.data).map(|content| TopicEvent { content })
    }
}

enum ControlEvent {
    Enter,
    Back,
    Up,
    Down,
    Cmd,
    Terminate,
    Subscribe,
}

enum AppEvent {
    Control(ControlEvent),
    Input(String),
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
                        // let props = &message.metadata().properties.iter().map(|keyvalue| format!("{}:{}", keyvalue.key, keyvalue.value)).collect::<Vec<String>>();
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
                KeyCode::Enter => Some(AppEvent::Control(ControlEvent::Enter)),
                KeyCode::Char('h') | KeyCode::Left | KeyCode::Esc => {
                    Some(AppEvent::Control(ControlEvent::Back))
                }
                KeyCode::Backspace => Some(AppEvent::Control(ControlEvent::Back)),
                KeyCode::Char(':') => Some(AppEvent::Control(ControlEvent::Cmd)),
                KeyCode::Char('j') | KeyCode::Down => Some(AppEvent::Control(ControlEvent::Down)),
                KeyCode::Char('k') | KeyCode::Up => Some(AppEvent::Control(ControlEvent::Up)),
                KeyCode::Char(any) => Some(AppEvent::Input(any.to_string())),
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

async fn update(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> anyhow::Result<()> {
    let auth_cfg = read_config()?;

    let token = auth(auth_cfg).await?;

    //TODO: unify pulsar and pulsar admin client auth configuration
    //Get the sensitive info from developers key
    let builder = Pulsar::builder(
        "pulsar+ssl://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud:6651".to_string(),
        TokioExecutor,
    )
    .with_auth_provider(OAuth2Authentication::client_credentials(OAuth2Params {
        issuer_url: "https://auth.streamnative.cloud/".to_string(),
        credentials_url: "file:///home/lukas/.streamnative-developers-key.json".to_string(), // Absolute path of your downloaded key file
        audience: Some("urn:sn:pulsar:o-w0y8l:staging".to_string()),
        scope: None,
    }));
    let pulsar = builder.build().await?;
    let pulsar = Arc::new(Mutex::new(pulsar));

    let conf = Configuration {
        base_path: "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud/admin/v2".to_string(),
        bearer_access_token: Some(token.access_token.clone()),
        ..Configuration::default()
    };

    let (sender, receiver): (Sender<AppEvent>, Receiver<AppEvent>) = channel();
    let control_sender = sender.clone();
    //can we use tokio thread here?
    let _handle = thread::spawn(move || listen_for_control(control_sender));
    let namespaces: Vec<Content> = fetch_namespaces("flowie", &conf).await?;

    let mut app = App {
        error_to_show: None,
        active_element: Element::ContentView,
        active_resource: Resource::Namespaces,
        contents: namespaces,
        content_cursor: Some(0),
        last_cursor: None,
        input: String::from(""),
        last_tenant: Some("flowie".to_string()),
        last_namespace: None,
        last_topic: None,
        active_sub_handle: None,
        pulsar_admin_cfg: conf,
    };

    loop {
        terminal.draw(|f| draw(f, &app).expect("Failed to draw"))?;
        if let Ok(event) = receiver.recv_timeout(Duration::from_millis(100)) {
            app.error_to_show = None;
            match event {
                AppEvent::Control(ControlEvent::Subscribe) => {
                    if let Resource::Topics = app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            let topic = app.contents[cursor].clone();
                            app.active_resource = Resource::Listening;
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
                AppEvent::Control(ControlEvent::Down) => {
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
                AppEvent::Control(ControlEvent::Back) => match app.active_resource {
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
                                fetch_namespaces(last_tenant, &app.pulsar_admin_cfg).await?;
                            if !namespaces.is_empty() {
                                app.content_cursor = app.last_cursor.or(Some(0))
                            } else {
                                app.content_cursor = None;
                            };
                            app.contents = namespaces;
                            app.active_resource = Resource::Namespaces;
                        }
                    }
                    Resource::Subscriptions => {
                        if let Some(last_namespace) = &app.last_namespace {
                            let topics =
                                fetch_topics(last_namespace, &app.pulsar_admin_cfg).await?;
                            if !topics.is_empty() {
                                app.content_cursor = app.last_cursor.or(Some(0))
                            } else {
                                app.content_cursor = None;
                            };

                            app.contents = topics;
                            app.active_resource = Resource::Topics;
                        }
                    }
                    Resource::Listening => {
                        if let Some(last_namespace) = &app.last_namespace {
                            let topics =
                                fetch_topics(last_namespace, &app.pulsar_admin_cfg).await?;
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
                    }
                },
                AppEvent::SubscriptionEvent(event) => {
                    if let Resource::Listening = app.active_resource {
                        app.contents.push(Content::SubMessage {
                            value: serde_json::to_string_pretty(&event.content)?,
                        });
                        if app.content_cursor.is_none() {
                            app.content_cursor = Some(0)
                        }
                    }
                }
                AppEvent::Input(input) => {
                    if app.active_element == Element::ResourceSelector {
                        //todo: this is very stupid, use char array maybe?
                        let mut str = app.input.to_owned();
                        str.push_str(&input.to_string());

                        app.input = str
                    }
                }
                AppEvent::Control(ControlEvent::Terminate) => break,
                AppEvent::Control(ControlEvent::Cmd) => {
                    app.active_element = Element::ResourceSelector;
                    app.input = String::new()
                }
                AppEvent::Control(ControlEvent::Enter) => match app.active_element {
                    Element::ContentView => match app.active_resource {
                        Resource::Tenants => {
                            if let Some(cursor) = app.content_cursor {
                                if !app.contents.is_empty() {
                                    let tenant = get_name(app.contents[cursor].clone()).unwrap();
                                    let namespaces =
                                        fetch_namespaces(&tenant, &app.pulsar_admin_cfg).await?;
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
                            }
                        }
                        Resource::Namespaces => {
                            if let Some(cursor) = app.content_cursor {
                                let namespace = get_name(app.contents[cursor].clone()).unwrap();
                                let topics =
                                    fetch_topics(&namespace, &app.pulsar_admin_cfg).await?;
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
                        }
                        Resource::Topics => {
                            if let Some(cursor) = app.content_cursor {
                                if !app.contents.is_empty() {
                                    let topic = get_name(app.contents[cursor].clone()).unwrap();
                                    let subscriptions = fetch_subscriptions(
                                        app.last_namespace.as_ref().unwrap(),
                                        &topic,
                                        &app.pulsar_admin_cfg,
                                    )
                                    .await?;
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
                            }
                        }
                        Resource::Subscriptions => {}
                        Resource::Listening => {}
                    },
                    Element::ResourceSelector => match app.input.as_str() {
                        "tenants" => {
                            app.input = String::new();
                            app.active_element = Element::ContentView;
                            app.active_resource = Resource::Tenants;
                            app.contents = fetch_tenants(&token).await?;
                        }
                        "namespaces" => {
                            if let Some(cursor) = app.content_cursor {
                                let tenant = get_name(app.contents[cursor].clone()).unwrap();
                                let topics =
                                    fetch_namespaces(&tenant, &app.pulsar_admin_cfg).await?;
                                app.content_cursor = None;
                                app.contents = topics;
                                app.active_resource = Resource::Namespaces;
                                app.last_tenant = Some(tenant.to_string())
                            }
                        }
                        "topics" => {
                            app.input = String::new();
                            app.active_element = Element::ContentView;
                            app.active_resource = Resource::Topics;
                        }
                        _ => {}
                    },
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

struct LayoutChunks {
    header: HeaderLayout,
    message: Option<Rect>,
    main: Rect,
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

            let main_chunks = match app.active_resource {
                Resource::Listening => Layout::default()
                    .direction(Direction::Horizontal)
                    // .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .constraints([Constraint::Percentage(100)])
                    .split(chunks[2]),
                _ => Rc::new([chunks[2]]),
            };

            LayoutChunks {
                header: HeaderLayout {
                    help: header_chunks[0],
                    logo: header_chunks[1],
                },
                message: Some(chunks[1].into()),
                main: main_chunks[0],
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

            let main_chunks = match app.active_resource {
                Resource::Listening => Layout::default()
                    .direction(Direction::Horizontal)
                    // .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                    .constraints([Constraint::Percentage(100)])
                    .split(chunks[1]),
                _ => Rc::new([chunks[1]]),
            };

            LayoutChunks {
                header: HeaderLayout {
                    help: header_chunks[0],
                    logo: header_chunks[1],
                },
                message: None,
                main: main_chunks[0],
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

    let content_borders = match app.active_element {
        Element::ResourceSelector => BorderType::Plain,
        Element::ContentView => BorderType::Plain,
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
            .flat_map(|content| get_show(content.clone())),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(app.content_cursor);

    let help_block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(1, 1, 1, 1));

    let help_item_bac = HelpItem::new("<esc>", "back");
    let help_item_listen = HelpItem::new("<c-s>", "listen");

    let help: Vec<HelpItem> = match &app.active_resource {
        Resource::Tenants => vec![HelpItem::new("<enter>", "namespaces")],
        Resource::Namespaces => vec![help_item_bac, HelpItem::new("<enter>", "topics")],
        Resource::Topics => vec![
            help_item_bac,
            help_item_listen,
            HelpItem::new("<enter>", "subscriptions"),
        ],
        Resource::Subscriptions => vec![help_item_bac],
        Resource::Listening => vec![help_item_bac],
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
            let selected = app
                .content_cursor
                .and_then(|cursor| app.contents.get(cursor))
                //TODO: probably nothing that can fail should be done in the draw function, as we
                //can't propagate errors up. So consider making the serialization during update
                //function.
                .and_then(|content| get_show(content.clone()))
                .map(|content| serde_json::from_str::<serde_json::Value>(&content).unwrap())
                .map(|content| serde_json::to_string_pretty(&content).unwrap());

            let json_block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Plain)
                .title("Preview")
                .title_alignment(Alignment::Center)
                .title_style(Style::default().fg(Color::Green))
                .padding(Padding::new(2, 2, 1, 1));

            let _json_preview = Paragraph::new(selected.unwrap_or(String::from("nothing to show")))
                .block(json_block)
                .wrap(Wrap { trim: false });

            frame.render_stateful_widget(content_list, layout_chunks.main, &mut state);
            // frame.render_widget(json_preview, main_chunks[1]);
        }
        _ => frame.render_stateful_widget(content_list, layout_chunks.main, &mut state),
    };

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

async fn fetch_anything(url: String, token: &Token) -> Result<Vec<String>, reqwest::Error> {
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .bearer_auth(&token.access_token)
        .send()
        .await?;

    let array: Vec<String> = response.json().await?;

    Ok(array)
}

async fn fetch_tenants(token: &Token) -> Result<Vec<Content>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";
    let tenant = fetch_anything(format!("{}/admin/v2/tenants", addr), token).await?;
    let content = tenant
        .iter()
        .map(|tenant| Content::Tenant {
            name: tenant.to_string(),
        })
        .collect();

    Ok(content)
}

async fn fetch_namespaces(tenant: &str, cfg: &Configuration) -> anyhow::Result<Vec<Content>> {
    let result = namespaces_get_tenant_namespaces(&cfg, tenant)
        .await
        .map_err(|err| anyhow!("Failed to fetch namespaces {}", err))?;

    let perfix_dropped = result
        .iter()
        .map(|namespace| Content::Namespace {
            name: namespace
                .strip_prefix("flowie/")
                .map(|stripped| stripped.to_string())
                .unwrap_or(namespace.clone()),
        })
        .collect();

    Ok(perfix_dropped)
}

async fn fetch_topics(namespace: &str, cfg: &Configuration) -> anyhow::Result<Vec<Content>> {
    let result = namespaces_get_topics(&cfg, "flowie", namespace, None, None)
        .await
        .map_err(|err| anyhow!("Failed to fetch topics {}", err))?;

    let perfix_dropped = result
        .iter()
        .map(|topic| Content::Topic {
            name: topic
                .split('/')
                .last()
                .map(|stripped| stripped.to_string())
                .unwrap_or(topic.clone()),
            fqn: topic.to_string(),
        })
        .collect();

    Ok(perfix_dropped)
}

async fn fetch_subscriptions(
    namespace: &str,
    topic: &str,
    cfg: &Configuration,
) -> anyhow::Result<Vec<Content>> {
    let result = persistent_topics_get_subscriptions(&cfg, "flowie", namespace, topic, None)
        .await
        .map_err(|err| anyhow!("Failed to fetch subscriptions {}", err))?
        .iter()
        .map(|sub| Content::Subscription {
            name: sub.to_string(),
        })
        .collect();

    Ok(result)
}
