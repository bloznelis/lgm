use crate::pulsar_admin;
use anyhow::anyhow;
use chrono::TimeDelta;
use clipboard::{ClipboardContext, ClipboardProvider};
use core::fmt;
use pulsar::{Pulsar, TokioExecutor};
use pulsar_admin_sdk::apis::configuration::Configuration;
use std::io::Stdout;
use std::{
    sync::{
        mpsc::{Receiver, Sender},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{oneshot, Mutex};

use ratatui::{backend::CrosstermBackend, Terminal};

use crate::auth::Token;
use crate::{draw, pulsar_listener, AppEvent, ControlEvent};

#[derive(Debug)]
pub enum Resource {
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

pub enum Side {
    Left,
    Right { scroll_offset: u16 },
}

pub struct ErrorToShow {
    pub message: String,
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

pub fn get_show(content: Content) -> Option<String> {
    match content {
        Content::Tenant { name } => Some(name),
        Content::Topic { name, .. } => Some(name),
        Content::Namespace { name } => Some(name),
        Content::Subscription { name } => Some(name),
        Content::SubMessage { body, .. } => Some(body),
    }
}

pub fn get_show_alternative(content: Content) -> Option<String> {
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

pub struct App {
    pub pulsar: PulsarApp,
    pub error_to_show: Option<ErrorToShow>,
    pub active_resource: Resource,
    pub contents: Vec<Content>,
    pub content_cursor: Option<usize>,
    pub last_cursor: Option<usize>,
    pub last_tenant: Option<String>,
    pub last_namespace: Option<String>,
    pub last_topic: Option<String>,
    pub pulsar_admin_cfg: Configuration,
    pub selected_side: Side,
}

pub struct PulsarApp {
    pub receiver: Receiver<AppEvent>,
    pub sender: Sender<AppEvent>,
    pub client: Arc<Mutex<Pulsar<TokioExecutor>>>,
    pub token: Token,
    pub active_sub_handle: Option<tokio::sync::oneshot::Sender<()>>,
}

pub async fn update(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
) -> anyhow::Result<()> {
    loop {
        terminal.draw(|f| draw::draw(f, &app).expect("Failed to draw"))?;
        if let Ok(event) = app.pulsar.receiver.recv_timeout(Duration::from_millis(100)) {
            app.error_to_show = None;
            match event {
                AppEvent::Control(ControlEvent::ResetSubscription(length)) => {
                    if let Resource::Listening = app.active_resource {
                        let length = match length {
                            crate::ResetLength::OneHour => TimeDelta::try_hours(1).expect("Expecting hours"),
                            crate::ResetLength::TwentyFourHours => TimeDelta::try_hours(24).expect("Expecting hours"),
                            crate::ResetLength::Week => TimeDelta::try_days(7).expect("Expecting days"),
                        };

                        let result = pulsar_admin::reset_subscription(
                            &app.last_tenant.as_ref().expect("Tenant must be set"),
                            &app.last_namespace.as_ref().expect("Namespace must be set"),
                            &app.last_topic.as_ref().expect("Topic must be set"),
                            "lgm_subscription",
                            &app.pulsar_admin_cfg,
                            length
                        )
                        .await;

                        match result {
                            Err(err) => app.error_to_show = Some(ErrorToShow::new(err.to_string())),
                            Ok(_) => (),
                        }
                    }
                }
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
                            app.last_topic = get_name(topic.clone());
                            app.active_resource = Resource::Listening;
                            app.content_cursor = None;
                            app.contents = Vec::new();
                            let new_pulsar = app.pulsar.client.clone();
                            let new_sender = app.pulsar.sender.clone();
                            let (tx, rx) = oneshot::channel::<()>();
                            app.pulsar.active_sub_handle = Some(tx);
                            let _sub_handle = tokio::task::spawn(async move {
                                pulsar_listener::listen_to_topic(
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
                            let tenants = pulsar_admin::fetch_tenants(&app.pulsar.token).await;
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
                                let namespaces = pulsar_admin::fetch_namespaces(
                                    last_tenant,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

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
                                let topics = pulsar_admin::fetch_topics(
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
                                let topics = pulsar_admin::fetch_topics(
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
                                        if let Some(sender) = app.pulsar.active_sub_handle.take() {
                                            sender.send({}).map_err(|()| { anyhow!("Failed to send termination singal to subscription") })?
                                        };
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
                                    pulsar_admin::fetch_namespaces(&tenant, &app.pulsar_admin_cfg)
                                        .await;

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
                            let topics = pulsar_admin::fetch_topics(
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
                                let subscriptions = pulsar_admin::fetch_subscriptions(
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
