use crate::pulsar_admin;
use anyhow::anyhow;
use chrono::TimeDelta;
use clipboard::{ClipboardContext, ClipboardProvider};
use core::fmt;
use crossterm::event::KeyCode;
use itertools::Itertools;
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
use uuid::Uuid;

use ratatui::{backend::CrosstermBackend, Terminal};

use crate::auth::Token;
use crate::{draw, pulsar_listener, AppEvent, ControlEvent};

#[derive(Clone)]
pub struct Tenants {
    pub tenants: Vec<Tenant>,
    pub cursor: Option<usize>,
}

#[derive(Clone)]
pub struct Namespaces {
    pub namespaces: Vec<Namespace>,
    pub cursor: Option<usize>,
}

#[derive(Clone)]
pub struct Topics {
    pub topics: Vec<Topic>,
    pub cursor: Option<usize>,
}

#[derive(Clone)]
pub struct Subscriptions {
    pub subscriptions: Vec<Subscription>,
    pub cursor: Option<usize>,
}

#[derive(Clone)]
pub struct Consumers {
    pub consumers: Vec<Consumer>,
    pub cursor: Option<usize>,
}

#[derive(Clone)]
pub struct Listening {
    pub messages: Vec<SubMessage>,
    pub filtered_messages: Vec<SubMessage>,
    pub panel: SelectedPanel,
    pub cursor: Option<usize>,
    pub search: Option<String>,
}

impl Listening {
    pub fn filter_messages(&mut self) {
        let messages = self.messages.clone();
        self.filtered_messages = match &self.search {
            Some(search) => {
                let search = search.replace(' ', "");

                messages
                    .into_iter()
                    .filter(|message| {
                        message.body.contains(&search)
                            || message
                                .properties
                                .iter()
                                .any(|prop| prop.contains(&search))
                    })
                    .collect_vec()
            }
            None => messages,
        };
    }
}

#[derive(Debug, Clone)]
pub enum Resource {
    Tenants,
    Namespaces,
    Topics,
    Subscriptions,
    Consumers,
    Listening { sub_name: String },
}

impl std::fmt::Display for Resource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub enum SelectedPanel {
    Left,
    Right { scroll_offset: u16 },
    Search,
}

pub struct InfoToShow {
    pub message: String,
    pub is_error: bool,
}

impl InfoToShow {
    fn error(message: String) -> Self {
        InfoToShow { message, is_error: true }
    }

    fn info(message: String) -> Self {
        InfoToShow { message, is_error: false }
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
    pub consumer_count: usize,
}

#[derive(Clone, Debug)]
pub struct Consumer {
    pub name: String,
    pub unacked_messages: i32,
    pub connected_since: String,
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
    CloseInfoMessage,
    DeleteSubscription {
        tenant: String,
        namespace: String,
        topic: String,
        sub_name: String,
        cfg: Configuration,
    },
    SeekSubscription {
        tenant: String,
        namespace: String,
        topic: String,
        sub_name: String,
        time_delta: TimeDelta,
        cfg: Configuration,
    },
}

#[derive(Clone)]
pub struct Resources {
    pub tenants: Tenants,
    pub namespaces: Namespaces,
    pub topics: Topics,
    pub subscriptions: Subscriptions,
    pub consumers: Consumers,
    pub listening: Listening,
}

impl Resources {
    fn cursor_up(&mut self, active_resource: &Resource) {
        match active_resource {
            Resource::Tenants => {
                self.tenants.cursor = cursor_up(self.tenants.cursor, self.tenants.tenants.len())
            }

            Resource::Namespaces => {
                self.namespaces.cursor =
                    cursor_up(self.namespaces.cursor, self.namespaces.namespaces.len())
            }

            Resource::Topics => {
                self.topics.cursor = cursor_up(self.topics.cursor, self.topics.topics.len())
            }

            Resource::Subscriptions => {
                self.subscriptions.cursor = cursor_up(
                    self.subscriptions.cursor,
                    self.subscriptions.subscriptions.len(),
                )
            }

            Resource::Consumers => {
                self.consumers.cursor =
                    cursor_up(self.consumers.cursor, self.consumers.consumers.len())
            }

            Resource::Listening { .. } => {
                self.listening.cursor = cursor_up(
                    self.listening.cursor,
                    self.listening.filtered_messages.len(),
                )
            }
        }
    }

    fn cursor_down(&mut self, active_resource: &Resource) {
        match active_resource {
            Resource::Tenants => {
                self.tenants.cursor = cursor_down(self.tenants.cursor, self.tenants.tenants.len())
            }
            Resource::Namespaces => {
                self.namespaces.cursor =
                    cursor_down(self.namespaces.cursor, self.namespaces.namespaces.len())
            }
            Resource::Topics => {
                self.topics.cursor = cursor_down(self.topics.cursor, self.topics.topics.len())
            }

            Resource::Subscriptions => {
                self.subscriptions.cursor = cursor_down(
                    self.subscriptions.cursor,
                    self.subscriptions.subscriptions.len(),
                )
            }

            Resource::Consumers => {
                self.consumers.cursor =
                    cursor_down(self.consumers.cursor, self.consumers.consumers.len())
            }

            Resource::Listening { .. } => {
                self.listening.cursor = cursor_down(
                    self.listening.cursor,
                    self.listening.filtered_messages.len(),
                )
            }
        }
    }

    pub fn selected_tenant(&self) -> Option<&Tenant> {
        self.tenants
            .cursor
            .and_then(|cursor| self.tenants.tenants.get(cursor))
    }

    pub fn selected_tenant_name(&self) -> Option<&str> {
        self.selected_tenant()
            .map(|tenant| tenant.name.as_ref())
    }

    pub fn selected_namespace(&self) -> Option<&Namespace> {
        self.namespaces
            .cursor
            .and_then(|cursor| self.namespaces.namespaces.get(cursor))
    }

    pub fn selected_namespace_name(&self) -> Option<&str> {
        self.selected_namespace()
            .map(|ns| ns.name.as_ref())
    }

    pub fn selected_topic(&self) -> Option<&Topic> {
        self.topics
            .cursor
            .and_then(|cursor| self.topics.topics.get(cursor))
    }

    pub fn selected_topic_name(&self) -> Option<&str> {
        self.selected_topic()
            .map(|topic| topic.name.as_ref())
    }

    pub fn selected_subscription(&self) -> Option<&Subscription> {
        self.subscriptions
            .cursor
            .and_then(|cursor| self.subscriptions.subscriptions.get(cursor))
    }
    pub fn selected_message(&self) -> Option<&SubMessage> {
        self.listening
            .cursor
            .and_then(|cursor| self.listening.messages.get(cursor))
    }
}

pub fn selected_topic(resources: &Resources) -> Option<Topic> {
    resources
        .topics
        .cursor
        .and_then(|cursor| resources.topics.topics.get(cursor).cloned())
}

fn cursor_up(current: Option<usize>, col_size: usize) -> Option<usize> {
    match current {
        Some(cursor) => {
            if cursor == 0 {
                Some(col_size.saturating_sub(1))
            } else {
                Some(cursor - 1)
            }
        }
        None => Some(0),
    }
}

fn cursor_down(current: Option<usize>, col_size: usize) -> Option<usize> {
    match current {
        Some(cursor) => {
            if cursor == col_size.saturating_sub(1) {
                Some(0)
            } else {
                Some(cursor + 1)
            }
        }
        None => Some(0),
    }
}

pub struct App {
    pub pulsar: PulsarApp,
    pub info_to_show: Option<InfoToShow>,
    pub confirmation_modal: Option<ConfirmationModal>,
    pub active_resource: Resource,
    pub resources: Resources,
    pub pulsar_admin_cfg: Configuration,
    pub cluster_name: String,
}

pub struct PulsarApp {
    pub receiver: Receiver<AppEvent>,
    pub sender: Sender<AppEvent>,
    pub client: Arc<Mutex<Pulsar<TokioExecutor>>>,
    pub token: Token,
    pub active_sub_handle: Option<tokio::sync::oneshot::Sender<()>>,
}

pub async fn update<'a>(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
) -> anyhow::Result<()> {
    loop {
        terminal.draw(|f| draw::draw_new(f, app))?;
        if let Ok(event) = app
            .pulsar
            .receiver
            .recv_timeout(Duration::from_millis(100))
        {
            match event {
                // XXX: Allow only a subset of events if search is enabled
                AppEvent::Control(control_event)
                    if matches!(app.resources.listening.panel, SelectedPanel::Search)
                        && matches!(
                            control_event,
                            ControlEvent::Yank
                                | ControlEvent::Back
                                | ControlEvent::Up
                                | ControlEvent::Down
                                | ControlEvent::Delete
                                | ControlEvent::ResetSubscription(..)
                        ) => {}
                AppEvent::Control(ControlEvent::ClearInput) => {
                    if matches!(app.resources.listening.panel, SelectedPanel::Search) {
                        app.resources.listening.search = Some(String::new());
                    }
                }
                AppEvent::Input(input) => {
                    if matches!(app.resources.listening.panel, SelectedPanel::Search) {
                        if let Resource::Listening { .. } = &app.active_resource {
                            let char = match input {
                                KeyCode::Char(char) if char != '/' => Some(char),
                                _ => None,
                            };

                            if let Some(char) = char {
                                app.resources.listening.search = app
                                    .resources
                                    .listening
                                    .search
                                    .as_ref()
                                    .map(|current_search| format!("{}{}", current_search, char));
                            }

                            app.resources.listening.filter_messages();
                        }
                    }
                }
                AppEvent::Command(ConfirmedCommand::CloseInfoMessage) => app.info_to_show = None,
                AppEvent::Command(ConfirmedCommand::SeekSubscription {
                    tenant,
                    namespace,
                    topic,
                    sub_name,
                    time_delta,
                    cfg,
                }) => {
                    let result = pulsar_admin::reset_subscription(
                        &tenant, &namespace, &topic, &sub_name, &cfg, time_delta,
                    )
                    .await;
                    app.confirmation_modal = None;

                    if let Err(err) = result {
                        app.info_to_show = Some(InfoToShow::error(err.to_string()))
                    }

                    //TODO: this is getting duplicated, move out to refresh_subscriptions function
                    let subscriptions = pulsar_admin::fetch_subs(
                        app.resources
                            .selected_tenant_name()
                            .expect("tenant must be set"),
                        app.resources
                            .selected_namespace_name()
                            .expect("namespace must be set"),
                        app.resources
                            .selected_topic_name()
                            .expect("namespace must be set"),
                        &app.pulsar_admin_cfg,
                    )
                    .await;

                    match subscriptions {
                        Ok(subscriptions) => {
                            app.resources.subscriptions.subscriptions = subscriptions;
                            app.active_resource = Resource::Subscriptions;

                            show_info_msg(app, "Seeked successfully.");
                        }
                        Err(err) => {
                            show_error_msg(
                                app,
                                format!("Failed to fetch subscriptions :[ {:?}", err),
                            );
                        }
                    }
                }
                AppEvent::Command(ConfirmedCommand::DeleteSubscription {
                    tenant,
                    namespace,
                    topic,
                    sub_name,
                    cfg,
                }) => {
                    match pulsar_admin::delete_subscription(
                        &tenant, &namespace, &topic, &sub_name, &cfg,
                    )
                    .await
                    {
                        Ok(_) => {
                            let subscriptions = pulsar_admin::fetch_subs(
                                app.resources
                                    .selected_tenant_name()
                                    .expect("tenant must be set"),
                                app.resources
                                    .selected_namespace_name()
                                    .expect("namespace must be set"),
                                app.resources
                                    .selected_topic_name()
                                    .expect("namespace must be set"),
                                &app.pulsar_admin_cfg,
                            )
                            .await;

                            match subscriptions {
                                Ok(subscriptions) => {
                                    if !subscriptions.is_empty() {
                                        app.resources.subscriptions.cursor = Some(0);
                                    } else {
                                        app.resources.subscriptions.cursor = None;
                                    };
                                    app.active_resource = Resource::Subscriptions;

                                    show_info_msg(app, "Subscription deleted.");
                                }
                                Err(err) => {
                                    show_error_msg(
                                        app,
                                        format!("Failed to fetch subscriptions :[ {:?}", err),
                                    );
                                }
                            }
                        }
                        Err(err) => {
                            show_error_msg(
                                app,
                                format!("Failed to delete subscription :[ {:?}", err),
                            );
                        }
                    }

                    app.confirmation_modal = None;
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
                AppEvent::Control(ControlEvent::Search) => {
                    if let Resource::Listening { .. } = &mut app.active_resource {
                        match &app.resources.listening.panel {
                            SelectedPanel::Left => match &app.resources.listening.search {
                                Some(_) => app.resources.listening.panel = SelectedPanel::Search,
                                None => {
                                    app.resources.listening.panel = SelectedPanel::Search;
                                    app.resources.listening.search = Some(String::new());
                                }
                            },
                            SelectedPanel::Right { .. } => match &app.resources.listening.search {
                                Some(_) => app.resources.listening.panel = SelectedPanel::Search,
                                None => {
                                    app.resources.listening.panel = SelectedPanel::Search;
                                    app.resources.listening.search = Some(String::new());
                                }
                            },
                            SelectedPanel::Search => match &app.resources.listening.search {
                                Some(_) => {
                                    app.resources.listening.panel = SelectedPanel::Left;
                                    app.resources.listening.search = None
                                }
                                None => {
                                    app.resources.listening.search = Some(String::new());
                                }
                            },
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Delete) => {
                    if let Resource::Subscriptions = &mut app.active_resource {
                        if let Some(subscription) = app.resources.selected_subscription() {
                            app.confirmation_modal = Some(ConfirmationModal {
                                message: format!("Delete '{}' subscription?", subscription.name),
                                command: ConfirmedCommand::DeleteSubscription {
                                    tenant: app
                                        .resources
                                        .selected_tenant_name()
                                        .expect("tenant must be set")
                                        .to_string(),
                                    namespace: app
                                        .resources
                                        .selected_namespace_name()
                                        .expect("namespace must be set")
                                        .to_string(),
                                    topic: app
                                        .resources
                                        .selected_topic_name()
                                        .expect("namespace must be set")
                                        .to_string(),
                                    sub_name: subscription.name.clone(),
                                    cfg: app.pulsar_admin_cfg.clone(),
                                },
                            })
                        }
                    }
                }
                AppEvent::Control(ControlEvent::ResetSubscription(length)) => {
                    if let Resource::Listening { sub_name } = &app.active_resource {
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
                            app.resources
                                .selected_tenant_name()
                                .expect("tenant must be set"),
                            app.resources
                                .selected_namespace_name()
                                .expect("namespace must be set"),
                            app.resources
                                .selected_topic_name()
                                .expect("namespace must be set"),
                            sub_name,
                            &app.pulsar_admin_cfg,
                            length,
                        )
                        .await;

                        if let Err(err) = result {
                            show_error_msg(app, err.to_string());
                        } else {
                            show_info_msg(app, "Seeked successfully.");
                        };
                    }
                    if let Resource::Subscriptions = &app.active_resource {
                        if let Some(subscription) = app.resources.selected_subscription() {
                            let (time_delta, time_str) = match length {
                                crate::ResetLength::OneHour => {
                                    (TimeDelta::try_hours(1).expect("Expecting hours"), "1h")
                                }
                                crate::ResetLength::TwentyFourHours => {
                                    (TimeDelta::try_hours(24).expect("Expecting hours"), "24h")
                                }
                                crate::ResetLength::Week => {
                                    (TimeDelta::try_days(7).expect("Expecting days"), "7days")
                                }
                            };

                            app.confirmation_modal = Some(ConfirmationModal {
                                message: format!(
                                    "Seek '{}' subscription for {time_str}?",
                                    subscription.name
                                ),
                                command: ConfirmedCommand::SeekSubscription {
                                    tenant: app
                                        .resources
                                        .selected_tenant_name()
                                        .expect("tenant must be set")
                                        .to_string(),
                                    namespace: app
                                        .resources
                                        .selected_namespace_name()
                                        .expect("namespace must be set")
                                        .to_string(),
                                    topic: app
                                        .resources
                                        .selected_topic_name()
                                        .expect("namespace must be set")
                                        .to_string(),
                                    sub_name: subscription.name.clone(),
                                    time_delta,
                                    cfg: app.pulsar_admin_cfg.clone(),
                                },
                            })
                        }
                    }
                }
                AppEvent::Control(ControlEvent::CycleSide) => {
                    if let Resource::Listening { .. } = &app.active_resource {
                        app.resources.listening.panel = match &app.resources.listening.panel {
                            SelectedPanel::Search => SelectedPanel::Left,
                            SelectedPanel::Left => SelectedPanel::Right { scroll_offset: 0 },
                            SelectedPanel::Right { .. } => {
                                if app.resources.listening.search.is_some() {
                                    SelectedPanel::Search
                                } else {
                                    SelectedPanel::Left
                                }
                            }
                        };
                    }
                }
                AppEvent::Control(ControlEvent::Yank) => {
                    if let Resource::Listening { .. } = &app.active_resource {
                        if let Some(sub_message) = app.resources.selected_message() {
                            let content = &sub_message.body;
                            let res = ClipboardContext::new()
                                .map_err(|_| anyhow!("Failed to get the clipboard."))
                                .and_then(|mut ctx| {
                                    ctx.set_contents(content.to_owned())
                                        .map_err(|_| anyhow!("Failed to copy to clipboard."))
                                });

                            match res {
                                Ok(_) => show_info_msg(app, "Message copied to clipboard."),
                                Err(err) => show_error_msg(app, err.to_string()),
                            }
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Subscribe) => {
                    if let Resource::Topics = &app.active_resource {
                        if let Some(topic) = app.resources.selected_topic().cloned() {
                            let sub_name = format!("lgm_subscription_{}", Uuid::new_v4());
                            app.active_resource =
                                Resource::Listening { sub_name: sub_name.clone() };
                            app.resources.listening.cursor = None;
                            app.resources.listening.messages = vec![];
                            app.resources.listening.filtered_messages = vec![];
                            app.resources.listening.search = None;
                            let new_pulsar = app.pulsar.client.clone();
                            let new_sender = app.pulsar.sender.clone();
                            let (tx, rx) = oneshot::channel::<()>();
                            app.pulsar.active_sub_handle = Some(tx);
                            let _sub_handle = tokio::task::spawn(async move {
                                pulsar_listener::listen_to_topic(
                                    sub_name,
                                    topic.fqn.clone(),
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
                    app.confirmation_modal = None;
                    if let SelectedPanel::Right { scroll_offset } =
                        &mut app.resources.listening.panel
                    {
                        *scroll_offset = scroll_offset.saturating_sub(1)
                    } else {
                        app.resources.cursor_up(&app.active_resource)
                    }
                }

                AppEvent::Control(ControlEvent::Down) => {
                    app.confirmation_modal = None;
                    if let SelectedPanel::Right { scroll_offset } =
                        &mut app.resources.listening.panel
                    {
                        *scroll_offset = scroll_offset.saturating_add(1)
                    } else {
                        app.resources.cursor_down(&app.active_resource)
                    }
                }

                AppEvent::Control(ControlEvent::BackSpace) => {
                    if let Resource::Listening { .. } = &app.active_resource {
                        if matches!(app.resources.listening.panel, SelectedPanel::Search) {
                            app.resources.listening.search = match &app.resources.listening.search {
                                Some(current_search) => {
                                    let len = current_search.len();
                                    if len > 0 {
                                        Some(current_search[0..len - 1].to_owned())
                                    } else {
                                        Some("".to_string())
                                    }
                                }
                                None => None,
                            };
                            app.resources.listening.filter_messages();
                        }
                    }
                }

                AppEvent::Control(ControlEvent::Back | ControlEvent::Esc) => {
                    if app.confirmation_modal.is_some() {
                        app.confirmation_modal = None;
                    } else {
                        match &app.active_resource {
                            Resource::Tenants { .. } => {}
                            Resource::Namespaces { .. } => {
                                let tenants =
                                    pulsar_admin::fetch_tenants(&app.pulsar_admin_cfg).await;
                                match tenants {
                                    Ok(tenants) => {
                                        app.resources.tenants.tenants = tenants;
                                        app.resources.tenants.tenants.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Tenants;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch tenants :[ {:?}", err),
                                        );
                                    }
                                }
                            }
                            Resource::Topics => {
                                let namespaces = pulsar_admin::fetch_namespaces(
                                    &app.resources.selected_tenant().unwrap().name,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match namespaces {
                                    Ok(namespaces) => {
                                        app.resources.namespaces.namespaces = namespaces;
                                        app.resources.namespaces.namespaces.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Namespaces;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch namespaces :[ {:?}", err),
                                        );
                                    }
                                }
                            }

                            Resource::Subscriptions => {
                                let topics = pulsar_admin::fetch_topics(
                                    &app.resources.selected_tenant().unwrap().name,
                                    &app.resources.selected_namespace().unwrap().name,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match topics {
                                    Ok(topics) => {
                                        app.resources.topics.topics = topics;
                                        app.active_resource = Resource::Topics;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch topics :[ {:?}", err),
                                        );
                                    }
                                }
                            }

                            Resource::Consumers => {
                                let subscriptions = pulsar_admin::fetch_subs(
                                    &app.resources.selected_tenant().unwrap().name,
                                    &app.resources.selected_namespace().unwrap().name,
                                    &app.resources.selected_topic().unwrap().name,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match subscriptions {
                                    Ok(subscriptions) => {
                                        app.resources.subscriptions.subscriptions = subscriptions;
                                        app.resources.subscriptions.subscriptions.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Subscriptions;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch subscriptions :[ {:?}", err),
                                        );
                                    }
                                }
                            }

                            Resource::Listening { .. } => {
                                match &app.resources.listening.panel {
                                    SelectedPanel::Search => {
                                        app.resources.listening.panel = SelectedPanel::Left;
                                        app.resources.listening.search = None;
                                        app.resources.listening.filtered_messages =
                                            app.resources.listening.messages.clone();
                                    }
                                    _ => {
                                        let topics = pulsar_admin::fetch_topics(
                                            &app.resources.selected_tenant().unwrap().name,
                                            &app.resources.selected_namespace().unwrap().name,
                                            &app.pulsar_admin_cfg,
                                        )
                                        .await;

                                        match topics {
                                            Ok(topics) => {
                                                app.resources.topics.topics = topics;
                                                app.resources.topics.topics.sort_by(|a, b| a.name.cmp(&b.name));
                                                app.resources.listening.search = None;
                                                app.resources.listening.panel = SelectedPanel::Left;
                                                app.active_resource = Resource::Topics;

                                                if let Some(sender) =
                                                    app.pulsar.active_sub_handle.take()
                                                {
                                                    sender.send(()).map_err(|()| { anyhow!( "Failed to send termination singal to subscription")
                                                })?
                                                };
                                            }
                                            Err(err) => {
                                                show_error_msg(
                                                    app,
                                                    format!("Failed to fetch topics :[ {:?}", err),
                                                );
                                            }
                                        }
                                    }
                                };
                            }
                        }
                    }
                }
                AppEvent::SubscriptionEvent(event) => {
                    if let Resource::Listening { .. } = &mut app.active_resource {
                        app.resources.listening.messages.push(SubMessage {
                            body: serde_json::to_string(&event.body)?,
                            properties: event.properties,
                        });

                        app.resources.listening.filter_messages();

                        if app.resources.listening.cursor.is_none() {
                            app.resources.listening.cursor = Some(0)
                        }
                    }
                }
                AppEvent::Control(ControlEvent::Terminate) => break,
                AppEvent::Control(ControlEvent::Enter) => {
                    app.confirmation_modal = None;
                    match app.active_resource.clone() {
                        Resource::Tenants => {
                            if let Some(tenant) = app.resources.selected_tenant() {
                                let namespaces = pulsar_admin::fetch_namespaces(
                                    &tenant.name,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match namespaces {
                                    Ok(namespaces) => {
                                        app.resources.namespaces.cursor = get_new_cursor(
                                            &namespaces,
                                            app.resources.namespaces.cursor,
                                        );
                                        app.resources.namespaces.namespaces = namespaces;
                                        app.resources.namespaces.namespaces.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Namespaces;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch namespaces :[ {:?}", err),
                                        );
                                    }
                                }
                            }
                        }
                        Resource::Namespaces => {
                            if let Some(namespace) = app.resources.selected_namespace() {
                                let topics = pulsar_admin::fetch_topics(
                                    &app.resources.selected_tenant().unwrap().name,
                                    &namespace.name,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match topics {
                                    Ok(topics) => {
                                        app.resources.topics.cursor =
                                            get_new_cursor(&topics, app.resources.topics.cursor);
                                        app.resources.topics.topics = topics;
                                        app.resources.topics.topics.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Topics;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch topics :[ {:?}", err),
                                        );
                                    }
                                }
                            }
                        }
                        Resource::Topics => {
                            if let Some(topic) = app.resources.selected_topic() {
                                let subscriptions = pulsar_admin::fetch_subs(
                                    app.resources
                                        .selected_tenant_name()
                                        .expect("tenant must be set"),
                                    app.resources
                                        .selected_namespace_name()
                                        .expect("namespace must be set"),
                                    &topic.name,
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match subscriptions {
                                    Ok(subscriptions) => {
                                        app.resources.subscriptions.cursor = get_new_cursor(
                                            &subscriptions,
                                            app.resources.subscriptions.cursor,
                                        );
                                        app.resources.subscriptions.subscriptions = subscriptions;
                                        app.resources.subscriptions.subscriptions.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Subscriptions;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch subscriptions :[ {:?}", err),
                                        );
                                    }
                                }
                            }
                        }
                        Resource::Subscriptions { .. } => {
                            if let Some(subscription) = app.resources.selected_subscription() {
                                let consumers = pulsar_admin::fetch_consumers(
                                    app.resources
                                        .selected_tenant_name()
                                        .expect("tenant must be set"),
                                    app.resources
                                        .selected_namespace_name()
                                        .expect("namespace must be set"),
                                    app.resources
                                        .selected_topic_name()
                                        .expect("namespace must be set"),
                                    subscription.name.as_ref(),
                                    &app.pulsar_admin_cfg,
                                )
                                .await;

                                match consumers {
                                    Ok(consumers) => {
                                        app.resources.consumers.cursor = get_new_cursor(
                                            &consumers,
                                            app.resources.consumers.cursor,
                                        );
                                        app.resources.consumers.consumers = consumers;
                                        app.resources.consumers.consumers.sort_by(|a, b| a.name.cmp(&b.name));
                                        app.active_resource = Resource::Consumers;
                                    }
                                    Err(err) => {
                                        show_error_msg(
                                            app,
                                            format!("Failed to fetch consumers :[ {:?}", err),
                                        );
                                    }
                                }
                            }
                        }
                        Resource::Listening { .. } => {
                            if let SelectedPanel::Search = &app.resources.listening.panel {
                                app.resources.listening.panel = SelectedPanel::Left
                            }
                        }
                        Resource::Consumers => {}
                    }
                }
            }
        }
    }

    Ok(())
}

fn get_new_cursor<A>(col: &[A], old_cursor: Option<usize>) -> Option<usize> {
    if col.is_empty() {
        None
    } else {
        match old_cursor {
            Some(old_cursor) => {
                if col.get(old_cursor).is_some() {
                    Some(old_cursor)
                } else {
                    Some(0)
                }
            }
            None => Some(0),
        }
    }
}

fn show_info_msg(app: &mut App, msg: &str) {
    app.info_to_show = Some(InfoToShow::info(msg.to_string()));

    let sender = app.pulsar.sender.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        sender.send(AppEvent::Command(ConfirmedCommand::CloseInfoMessage))
    });
}

fn show_error_msg(app: &mut App, msg: String) {
    app.info_to_show = Some(InfoToShow::error(msg.to_string()));

    let sender = app.pulsar.sender.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        sender.send(AppEvent::Command(ConfirmedCommand::CloseInfoMessage))
    });
}
