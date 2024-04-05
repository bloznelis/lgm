use crate::pulsar_admin;
use anyhow::anyhow;
use chrono::TimeDelta;
use clipboard::{ClipboardContext, ClipboardProvider};
use core::fmt;
use pulsar::{Pulsar, TokioExecutor};
use pulsar_admin_sdk::apis::configuration::Configuration;
use std::io::Stdout;
use std::usize;
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

#[derive(Debug, Clone)]
pub enum Resource {
    Tenants {
        tenants: Vec<Tenant>,
    },
    Namespaces {
        namespaces: Vec<Namespace>,
    },
    Topics {
        topics: Vec<Topic>,
    },
    Subscriptions {
        subscriptions: Vec<Subscription>,
    },
    Listening {
        messages: Vec<SubMessage>,
        selected_side: Side,
    },
}

impl Resource {
    pub fn list_element_count(&self) -> usize {
        match self {
            Resource::Tenants { tenants } => tenants.len(),
            Resource::Namespaces { namespaces } => namespaces.len(),
            Resource::Topics { topics } => topics.len(),
            Resource::Subscriptions { subscriptions } => subscriptions.len(),
            Resource::Listening { messages, .. } => messages.len(),
        }
    }
}

impl std::fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone)]
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

#[derive(Clone, Debug)]
pub struct Namespace {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct Tenant {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct Topic {
    pub name: String,
    pub fqn: String,
}

#[derive(Clone, Debug)]
pub struct Subscription {
    pub name: String,
    pub sub_type: String,
    pub backlog_size: i64,
}

#[derive(Clone, Debug)]
pub struct SubMessage {
    pub body: String,
    pub properties: Vec<String>,
}

pub struct ConfirmationModal {
    pub message: String,
    pub command: ConfirmedCommand,
}

pub enum ConfirmedCommand {
    DeleteSubscription {
        tenant: String,
        namespace: String,
        topic: String,
        sub_name: String,
        cfg: Configuration,
    },
}

pub struct App {
    pub pulsar: PulsarApp,
    pub error_to_show: Option<ErrorToShow>,
    pub confirmation_modal: Option<ConfirmationModal>,
    pub active_resource: Resource,
    pub content_cursor: Option<usize>,
    pub last_cursor: Option<usize>,
    pub last_tenant: Option<String>,
    pub last_namespace: Option<String>,
    pub last_topic: Option<String>,
    pub pulsar_admin_cfg: Configuration,
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
        terminal.draw(|f| draw::draw_new(f, &app))?;
        if let Ok(event) = app
            .pulsar
            .receiver
            .recv_timeout(Duration::from_millis(100))
        {
            app.error_to_show = None;
            match event {
                AppEvent::Command(ConfirmedCommand::DeleteSubscription {
                    tenant,
                    namespace,
                    topic,
                    sub_name,
                    cfg,
                }) => {
                    pulsar_admin::delete_subscription(&tenant, &namespace, &topic, &sub_name, &cfg)
                        .await?;
                    app.confirmation_modal = None;

                    let subscriptions = pulsar_admin::fetch_subs(
                        &app.last_tenant.clone().unwrap(),
                        app.last_namespace.as_ref().unwrap(),
                        &app.last_topic.as_ref().unwrap(),
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
                            app.active_resource = Resource::Subscriptions { subscriptions };
                            app.last_topic = Some(topic.to_string())
                        }
                        Err(err) => {
                            app.error_to_show = Some(ErrorToShow::new(format!(
                                "Failed to fetch subscriptions :[\n {:?}",
                                err
                            )));
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Accept) => {
                    if let Some(confirmation) = app.confirmation_modal.take() {
                        app.pulsar
                            .sender
                            .send(AppEvent::Command(confirmation.command))?
                    }
                }
                AppEvent::Control(ControlEvent::Refuse) => {
                    app.confirmation_modal = None;
                }
                AppEvent::Control(ControlEvent::Delete) => {
                    if let Resource::Subscriptions { subscriptions } = &mut app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            let subscription = &subscriptions[cursor].name;

                            app.confirmation_modal = Some(ConfirmationModal {
                                message: format!("Delete '{}' subscription?", subscription),
                                command: ConfirmedCommand::DeleteSubscription {
                                    tenant: app
                                        .last_tenant
                                        .as_ref()
                                        .expect("Tenant must be set")
                                        .clone(),
                                    namespace: app
                                        .last_namespace
                                        .as_ref()
                                        .expect("Namespace must be set")
                                        .clone(),
                                    topic: app
                                        .last_topic
                                        .as_ref()
                                        .expect("Topic must be set")
                                        .clone(),
                                    sub_name: subscription.clone(),
                                    cfg: app.pulsar_admin_cfg.clone(),
                                },
                            })
                        }
                    }
                }
                AppEvent::Control(ControlEvent::ResetSubscription(length)) => {
                    if let Resource::Listening { .. } = &app.active_resource {
                        let length = match length {
                            crate::ResetLength::OneHour => {
                                TimeDelta::try_hours(1).expect("Expecting hours")
                            }
                            crate::ResetLength::TwentyFourHours => {
                                TimeDelta::try_hours(24).expect("Expecting hours")
                            }
                            crate::ResetLength::Week => {
                                TimeDelta::try_days(7).expect("Expecting days")
                            }
                        };

                        let result = pulsar_admin::reset_subscription(
                            &app.last_tenant
                                .as_ref()
                                .expect("Tenant must be set"),
                            &app.last_namespace
                                .as_ref()
                                .expect("Namespace must be set"),
                            &app.last_topic
                                .as_ref()
                                .expect("Topic must be set"),
                            "lgm_subscription",
                            &app.pulsar_admin_cfg,
                            length,
                        )
                        .await;

                        match result {
                            Err(err) => app.error_to_show = Some(ErrorToShow::new(err.to_string())),
                            Ok(_) => (),
                        }
                    }
                }
                AppEvent::Control(ControlEvent::CycleSide) => {
                    if let Resource::Listening { selected_side, .. } = &mut app.active_resource {
                        *selected_side = match *selected_side {
                            Side::Left => Side::Right { scroll_offset: 0 },
                            Side::Right { .. } => Side::Left,
                        };
                    }
                }
                AppEvent::Control(ControlEvent::Yank) => {
                    if let Resource::Listening { messages, .. } = &app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            if let Some(content) = messages.get(cursor) {
                                let content = &content.body;
                                //TODO: Create this once and pass it around
                                let mut ctx: ClipboardContext = ClipboardProvider::new().unwrap();
                                ctx.set_contents(content.to_string()).unwrap();
                            }
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Subscribe) => {
                    if let Resource::Topics { topics } = &app.active_resource {
                        if let Some(cursor) = app.content_cursor {
                            let topic = topics[cursor].clone();
                            app.last_topic = Some(topic.name);
                            app.active_resource = Resource::Listening {
                                messages: Vec::new(),
                                selected_side: Side::Left,
                            };
                            app.content_cursor = None;
                            let new_pulsar = app.pulsar.client.clone();
                            let new_sender = app.pulsar.sender.clone();
                            let (tx, rx) = oneshot::channel::<()>();
                            app.pulsar.active_sub_handle = Some(tx);
                            let _sub_handle = tokio::task::spawn(async move {
                                pulsar_listener::listen_to_topic(
                                    topic.fqn, new_sender, new_pulsar, rx,
                                )
                                .await
                            });
                        }
                    }
                }

                AppEvent::Control(ControlEvent::Up) => {
                    app.confirmation_modal = None;
                    if let Resource::Listening {
                        selected_side: Side::Right { scroll_offset },
                        ..
                    } = &mut app.active_resource
                    {
                        *scroll_offset = scroll_offset.saturating_sub(1)
                    } else {
                        app.content_cursor = match app.content_cursor {
                            Some(cursor) => {
                                if cursor <= 0 {
                                    Some(
                                        app.active_resource
                                            .list_element_count()
                                            .saturating_sub(1),
                                    )
                                } else {
                                    Some(cursor - 1)
                                }
                            }
                            None => Some(0),
                        }
                    }
                }

                AppEvent::Control(ControlEvent::Down) => {
                    app.confirmation_modal = None;
                    if let Resource::Listening {
                        selected_side: Side::Right { scroll_offset },
                        ..
                    } = &mut app.active_resource
                    {
                        *scroll_offset = scroll_offset.saturating_add(1)
                    } else {
                        app.content_cursor = match app.content_cursor {
                            Some(cursor) => {
                                if cursor
                                    == app
                                        .active_resource
                                        .list_element_count()
                                        .saturating_sub(1)
                                {
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
                    app.confirmation_modal = None;
                    match &app.active_resource {
                        Resource::Tenants { .. } => {}
                        Resource::Namespaces { .. } => {
                            let tenants = pulsar_admin::fetch_tenants(&app.pulsar.token).await;
                            match tenants {
                                Ok(tenants) => {
                                    if !tenants.is_empty() {
                                        app.content_cursor = app.last_cursor.or(Some(0))
                                    } else {
                                        app.content_cursor = None;
                                    };
                                    app.active_resource = Resource::Tenants { tenants };
                                }
                                Err(err) => {
                                    app.error_to_show = Some(ErrorToShow::new(format!(
                                        "Failed to fetch tenants :[\n {:?}",
                                        err
                                    )));
                                }
                            }
                        }
                        Resource::Topics { .. } => {
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
                                        app.active_resource = Resource::Namespaces { namespaces };
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

                        Resource::Subscriptions { .. } => {
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

                                        app.active_resource = Resource::Topics { topics };
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
                        Resource::Listening { .. } => {
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

                                        app.active_resource = Resource::Topics { topics };
                                        if let Some(sender) = app.pulsar.active_sub_handle.take() {
                                            sender.send({}).map_err(|()| {
                                                anyhow!(
                                                "Failed to send termination singal to subscription"
                                            )
                                            })?
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
                    if let Resource::Listening { messages, .. } = &mut app.active_resource {
                        messages.push(SubMessage {
                            body: serde_json::to_string(&event.body)?,
                            properties: event.properties,
                        });
                        if app.content_cursor.is_none() {
                            app.content_cursor = Some(0)
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Terminate) => break,
                AppEvent::Control(ControlEvent::Enter) => {
                    app.confirmation_modal = None;
                    match app.active_resource.clone() {
                        Resource::Tenants { tenants } => {
                            if let Some(cursor) = app.content_cursor {
                                if !tenants.is_empty() {
                                    let tenant = &tenants[cursor].name;
                                    let namespaces = pulsar_admin::fetch_namespaces(
                                        &tenant,
                                        &app.pulsar_admin_cfg,
                                    )
                                    .await;

                                    match namespaces {
                                        Ok(namespaces) => {
                                            app.last_cursor = app.content_cursor;
                                            if !namespaces.is_empty() {
                                                app.content_cursor = Some(0);
                                            } else {
                                                app.content_cursor = None;
                                            };
                                            app.active_resource =
                                                Resource::Namespaces { namespaces };
                                            app.last_tenant = Some(tenant.clone())
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
                        Resource::Namespaces { namespaces } => {
                            if let Some(cursor) = app.content_cursor {
                                let namespace = &namespaces[cursor].name;
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
                                        app.active_resource = Resource::Topics { topics };
                                        app.last_namespace = Some(namespace.to_string())
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
                        Resource::Topics { topics } => {
                            if let Some(cursor) = app.content_cursor {
                                if !topics.is_empty() {
                                    let topic = &topics[cursor].name;
                                    let subscriptions = pulsar_admin::fetch_subs(
                                        &app.last_tenant.clone().unwrap(),
                                        app.last_namespace.as_ref().unwrap(),
                                        topic,
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
                                            app.active_resource =
                                                Resource::Subscriptions { subscriptions };
                                            app.last_topic = Some(topic.to_string())
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
                        Resource::Subscriptions { .. } => {}
                        Resource::Listening { .. } => {}
                    }
                }
            }
        }
    }

    Ok({})
}
