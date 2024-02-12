pub mod auth;

use core::fmt;
use auth::{auth, read_config, Token};
use futures::TryStreamExt;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::consumer::InitialPosition;
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Wrap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

struct App {
    active_element: Element,
    active_resource: Resource,
    contents: Vec<String>,
    content_cursor: Option<usize>,
    last_cursor: Option<usize>,
    input: String,
    last_tenant: Option<String>,
    last_namespace: Option<String>,
    last_topic: Option<String>,
    active_sub_handle: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(Serialize, Deserialize)]
struct TopicEvent {
    content: Value,
}

impl DeserializeMessage for TopicEvent {
    type Output = Result<TopicEvent, serde_json::Error>;
    // type Output = Result<TopicEvent, FromUtf8Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice::<Value>(&payload.data)
            .map(|content| TopicEvent { content: content })
    }
}

struct Topic {
    value: String,
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
    topic: Topic,
    event_sender: Sender<AppEvent>,
    pulsar: Arc<Mutex<Pulsar<TokioExecutor>>>,
    mut control_channel: tokio::sync::oneshot::Receiver<()>,
) {
    let mut consumer: Consumer<TopicEvent, TokioExecutor> = pulsar
        .lock()
        .await
        .consumer()
        .with_options(
            pulsar::ConsumerOptions::default()
                .durable(false)
                .with_initial_position(InitialPosition::Latest),
        )
        .with_topic(topic.value)
        .with_consumer_name("lgm")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("lgm_subscription")
        .build()
        .await
        .unwrap();

    loop {
        tokio::select! {
            msg = consumer.try_next() => {
                match msg {
                    Ok(Some(message)) => {
                        let a = &message.metadata().properties.iter().map(|keyvalue| format!("{}:{}", keyvalue.key, keyvalue.value)).collect::<Vec<String>>();
                        let topic_event = message.deserialize().unwrap();
                        let _ = event_sender.send(AppEvent::SubscriptionEvent(topic_event));

                        consumer.ack(&message).await.unwrap();

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

    consumer.close().await.unwrap();
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
async fn main() -> Result<(), reqwest::Error> {
    let mut stdout = io::stdout();

    execute!(stdout, EnterAlternateScreen).unwrap();
    enable_raw_mode().unwrap();

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).unwrap();

    env_logger::init();
    let auth_cfg = read_config().unwrap();

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
    let pulsar = builder.build().await.unwrap();
    let pulsar = Arc::new(Mutex::new(pulsar));

    let (sender, receiver): (Sender<AppEvent>, Receiver<AppEvent>) = channel();
    let control_sender = sender.clone();
    let _handle = thread::spawn(move || listen_for_control(control_sender)); //can we use tokio thread here?

    let mut app = App {
        active_element: Element::ContentView,
        active_resource: Resource::Namespaces,
        contents: fetch_namespaces(&"flowie".to_string(), &token).await?,
        content_cursor: Some(0),
        last_cursor: None,
        input: String::from(""),
        last_tenant: Some("flowie".to_string()),
        last_namespace: None,
        last_topic: None,
        active_sub_handle: None,
    };

    loop {
        terminal.draw(|f| draw(f, &app)).unwrap();
        if let Ok(event) = receiver.recv_timeout(Duration::from_millis(100)) {
            match event {
                AppEvent::Control(ControlEvent::Subscribe) => {
                    if let Resource::Topics = app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            let topic = Topic {
                                value: app.contents[cursor].clone(),
                            };
                            app.active_resource = Resource::Listening;
                            app.contents = Vec::new();
                            let new_pulsar = pulsar.clone();
                            let new_sender = sender.clone();
                            let (tx, rx) = oneshot::channel::<()>();
                            app.active_sub_handle = Some(tx);
                            let _sub_handle = tokio::task::spawn(async move {
                                listen_to_topic(topic, new_sender, new_pulsar, rx).await
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
                        let tenants = fetch_tenants(&token).await?;
                        if !tenants.is_empty() {
                            app.content_cursor = app.last_cursor.or(Some(0))
                        } else {
                            app.content_cursor = None;
                        };
                        app.contents = tenants;
                        app.active_resource = Resource::Tenants;
                    }
                    Resource::Topics => {
                        if let Some(last_tenant) = &app.last_tenant {
                            let namespaces = fetch_namespaces(last_tenant, &token).await?;
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
                            let topics = fetch_topics(last_namespace, &token).await?;
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
                            let topics = fetch_topics(last_namespace, &token).await?;
                            if !topics.is_empty() {
                                app.content_cursor = app.last_cursor.or(Some(0))
                            } else {
                                app.content_cursor = None;
                            };

                            app.contents = topics;
                            app.active_resource = Resource::Topics;
                            if let Some(sub) = app.active_sub_handle {
                                sub.send(()).unwrap()
                            }
                            app.active_sub_handle = None
                        }
                    }
                },
                AppEvent::SubscriptionEvent(event) => {
                    if let Resource::Listening = app.active_resource {
                        app.contents
                            .push(serde_json::to_string_pretty(&event.content).unwrap());
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
                                let tenant = app.contents[cursor].clone();
                                let namespaces = fetch_namespaces(&tenant, &token).await?;
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
                        Resource::Namespaces => {
                            if let Some(cursor) = app.content_cursor {
                                let namespace = app.contents[cursor].clone();
                                let topics = fetch_topics(&namespace, &token).await?;
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
                                    let topic = app.contents[cursor].clone();
                                    let subscriptions = fetch_subscriptions(&topic, &token).await?;
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
                                let tenant = app.contents[cursor].clone();
                                let topics = fetch_namespaces(&tenant, &token).await?;
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

    disable_raw_mode().unwrap();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).unwrap();
    terminal.show_cursor().unwrap();

    Ok(())
}

fn draw(frame: &mut Frame, app: &App) {
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

    let content_list = List::new(app.contents.clone())
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

    frame.render_widget(help_list, header_chunks[0]);
    frame.render_widget(header, header_chunks[1]);

    match app.active_resource {
        Resource::Listening => {
            let selected = app
                .content_cursor
                .and_then(|cursor| app.contents.get(cursor))
                .map(|content| serde_json::from_str::<serde_json::Value>(content).unwrap())
                .map(|content| serde_json::to_string_pretty(&content).unwrap());

            let json_block = Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Plain)
                .title("Preview")
                .title_alignment(Alignment::Center)
                .title_style(Style::default().fg(Color::Green))
                .padding(Padding::new(2, 2, 1, 1));

            let json_preview = Paragraph::new(selected.unwrap_or(String::from("nothing to show")))
                .block(json_block)
                .wrap(Wrap { trim: false });

            frame.render_stateful_widget(content_list, main_chunks[0], &mut state);
            // frame.render_widget(json_preview, main_chunks[1]);
        }
        _ => frame.render_stateful_widget(content_list, main_chunks[0], &mut state),
    };
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

async fn fetch_tenants(token: &Token) -> Result<Vec<String>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";

    fetch_anything(format!("{}/admin/v2/tenants", addr), token).await
}

async fn fetch_namespaces(tenant: &String, token: &Token) -> Result<Vec<String>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";

    fetch_anything(format!("{}/admin/v2/namespaces/{}", addr, tenant), token).await
}

async fn fetch_topics(namespace: &String, token: &Token) -> Result<Vec<String>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";

    fetch_anything(
        format!("{}/admin/v2/namespaces/{}/topics", addr, namespace),
        token,
    )
    .await
}

async fn fetch_subscriptions(topic: &String, token: &Token) -> Result<Vec<String>, reqwest::Error> {
    let addr = "https://pc-2c213b69.euw1-turtle.streamnative.g.snio.cloud";
    let topic = topic.strip_prefix("persistent://").unwrap_or(topic);

    fetch_anything(
        format!("{}/admin/v2/persistent/{}/subscriptions", addr, topic),
        token,
    )
    .await
}
