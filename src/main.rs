use core::fmt;
use futures::{StreamExt, TryStreamExt};
use pulsar::{Consumer, DeserializeMessage, Payload, Pulsar, SubType, TokioExecutor};
use reqwest;
use serde::{Deserialize, Serialize};
use std::{
    fmt::Display,
    io,
    sync::{mpsc::{channel, Receiver, Sender}, Arc, Mutex},
};
use tokio;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::Rect,
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
    input: String,
    last_tenant: Option<String>,
    last_namespace: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct TopicEvent {
    content: String,
}

impl DeserializeMessage for TopicEvent {
    type Output = Result<TopicEvent, serde_json::Error>;

    fn deserialize_message(payload: &Payload) -> Self::Output {
        serde_json::from_slice(&payload.data)
    }
}

struct Topic {
    value: String,
}

// async fn listen_to_topics(
//     event_sender: Sender<TopicEvent>,
//     topic_receiver: Receiver<Topic>,
//     pulsar: Pulsar<TokioExecutor>,
// ) {
//     loop {
//         let topic = topic_receiver.recv().unwrap();
//         let mut consumer: Consumer<TopicEvent, TokioExecutor> = pulsar
//             .consumer()
//             .with_topic(topic.value)
//             .with_consumer_name("lgm")
//             .with_subscription_type(SubType::Exclusive)
//             .with_subscription("lgm_subscription")
//             .build()
//             .await
//             .unwrap();
//     }
// }

async fn listen_to_topic(
    topic: Topic,
    event_sender: Sender<TopicEvent>,
    pulsar: Arc<Mutex<Pulsar<TokioExecutor>>>,
) {
    let mut consumer: Consumer<TopicEvent, TokioExecutor> = pulsar.lock().unwrap()
        .consumer()
        .with_topic(topic.value)
        .with_consumer_name("lgm")
        .with_subscription_type(SubType::Exclusive)
        .with_subscription("lgm_subscription")
        .build()
        .await
        .unwrap();

    // let _ = tokio::task::spawn(async move { transmit_events(consumer, event_sender) }).await;
    transmit_events(consumer, event_sender).await;
}

async fn transmit_events(
    mut consumer: Consumer<TopicEvent, TokioExecutor>,
    sender: Sender<TopicEvent>,
) {
    while let Some(msg) = consumer.try_next().await.unwrap() {
        let message = msg.deserialize().unwrap();
        let _ = sender.send(message);

        consumer.ack(&msg).await.unwrap();
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

    let initial_tenants = fetch_tenants().await?;

    let mut builder = Pulsar::builder("http://127.0.0.1:6650".to_string(), TokioExecutor);
    let pulsar = builder.build().await.unwrap();
    // let pulsar = Arc::new(Mutex::new(builder.build().await.unwrap()));

    let (event_sender, event_receiver): (Sender<TopicEvent>, Receiver<TopicEvent>) = channel();
    let (topic_sender, topic_receiver): (Sender<Topic>, Receiver<Topic>) = channel();

    let mut app = App {
        active_element: Element::ContentView,
        active_resource: Resource::Tenants,
        contents: initial_tenants,
        content_cursor: None,
        input: String::from(""),
        last_tenant: None,
        last_namespace: None,
    };

    loop {
        terminal.draw(|f| draw(f, &mut app)).unwrap();

        if let Event::Key(key) = event::read().unwrap() {
            if key.kind == KeyEventKind::Press {
                match key.code {
                    KeyCode::Char('q') => break,
                    KeyCode::Char('h') => match app.active_resource {
                        Resource::Tenants => {}
                        Resource::Namespaces => {
                            app.content_cursor = Some(0);
                            app.contents = fetch_tenants().await?;
                            app.active_resource = Resource::Tenants;
                        }
                        Resource::Topics => {
                            if let Some(last_tenant) = &app.last_tenant {
                                app.content_cursor = Some(0);
                                app.contents = fetch_namespaces(last_tenant).await?;
                                app.active_resource = Resource::Namespaces;
                            }
                        }
                    },
                    KeyCode::Char(':') => {
                        app.active_element = Element::ResourceSelector;
                        app.input = String::new()
                    }
                    KeyCode::Backspace => app.input = String::new(),
                    KeyCode::Enter => match app.active_element {
                        Element::ContentView => match app.active_resource {
                            Resource::Tenants => {
                                if let Some(cursor) = app.content_cursor {
                                    let tenant = app.contents[cursor].clone();
                                    let topics = fetch_namespaces(&tenant).await?;
                                    app.content_cursor = Some(0);
                                    app.contents = topics;
                                    app.active_resource = Resource::Namespaces;
                                    app.last_tenant = Some(tenant.to_string())
                                }
                            }
                            Resource::Namespaces => {
                                if let Some(cursor) = app.content_cursor {
                                    let topics = fetch_topics(&app.contents[cursor]).await?;
                                    app.content_cursor = Some(0);
                                    app.contents = topics;
                                    app.active_resource = Resource::Topics;
                                }
                            }
                            Resource::Topics => {
                                if let Some(cursor) = app.content_cursor {
                                    let topic = Topic {
                                        value: app.contents[cursor].clone(),
                                    };
                                    listen_to_topic(topic, event_sender, pulsar);
                                }
                            }
                        },
                        Element::ResourceSelector => match app.input.as_str() {
                            "tenants" => {
                                app.input = String::new();
                                app.active_element = Element::ContentView;
                                app.active_resource = Resource::Tenants;
                                app.contents = fetch_tenants().await?;
                            }
                            "namespaces" => {
                                if let Some(cursor) = app.content_cursor {
                                    let tenant = app.contents[cursor].clone();
                                    let topics = fetch_namespaces(&tenant).await?;
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
                    KeyCode::Char('j') | KeyCode::Down => {
                        app.content_cursor = match app.content_cursor {
                            Some(cursor) => {
                                if cursor == app.contents.len() - 1 {
                                    Some(0)
                                } else {
                                    Some(cursor + 1)
                                }
                            }
                            None => Some(0),
                        }
                    }
                    KeyCode::Char('k') | KeyCode::Up => {
                        app.content_cursor = match app.content_cursor {
                            Some(cursor) => {
                                if cursor == 0 {
                                    Some(app.contents.len() - 1)
                                } else {
                                    Some(cursor - 1)
                                }
                            }
                            None => None,
                        }
                    }
                    KeyCode::Char(any) => {
                        if app.active_element == Element::ResourceSelector {
                            //todo: this is very stupid, use char array maybe?
                            let mut str = app.input.to_owned();
                            str.push_str(&any.to_string());

                            app.input = String::from(str)
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    disable_raw_mode().unwrap();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).unwrap();
    terminal.show_cursor().unwrap();

    Ok(())
}

fn draw(frame: &mut Frame, state: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Percentage(90)])
        .split(frame.size());

    let input_borders = match state.active_element {
        Element::ResourceSelector => BorderType::Double,
        Element::ContentView => BorderType::Plain,
    };

    let input_block = Block::default()
        .borders(Borders::ALL)
        .border_type(input_borders);

    let input_paragraph = Paragraph::new(format!("> {}", state.input))
        .alignment(Alignment::Left)
        .block(input_block);

    let content_borders = match state.active_element {
        Element::ResourceSelector => BorderType::Plain,
        Element::ContentView => BorderType::Double,
    };

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(content_borders)
        .title(state.active_resource.to_string())
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(state.contents.clone())
        .block(content_block)
        .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(state.content_cursor);

    frame.render_widget(input_paragraph, chunks[0]);
    frame.render_stateful_widget(content_list, chunks[1], &mut state);
}

async fn fetch_tenants() -> Result<Vec<String>, reqwest::Error> {
    let addr = "http://127.0.0.1:8080";
    // Make the GET request to list tenants
    let response = reqwest::get(format!("{}/admin/v2/tenants", addr)).await?;

    let tenants: Vec<String> = response.json().await?;

    Ok(tenants)
}

async fn fetch_namespaces(tenant: &String) -> Result<Vec<String>, reqwest::Error> {
    let addr = "http://127.0.0.1:8080";
    // Make the GET request to list tenants
    let response = reqwest::get(format!("{}/admin/v2/namespaces/{}", addr, tenant)).await?;

    let namespaces: Vec<String> = response.json().await?;

    Ok(namespaces)
}

async fn fetch_topics(namespace: &String) -> Result<Vec<String>, reqwest::Error> {
    let addr = "http://127.0.0.1:8080";
    // Make the GET request to list tenants
    let response =
        reqwest::get(format!("{}/admin/v2/namespaces/{}/topics", addr, namespace)).await?;
    // println!("resp {:?}", response);

    let topics: Vec<String> = response.json().await?;

    Ok(topics)
}
