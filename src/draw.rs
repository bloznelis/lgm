use std::usize;

use ratatui::layout::Rect;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Wrap;

use ratatui::{
    prelude::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, BorderType, Borders, List, ListState, Padding, Paragraph},
    Frame,
};

use crate::update::{Namespace, SubMessage, Subscription, Tenant, Topic};
use crate::{App, Resource, Side};

struct HeaderLayout {
    help: Rect,
    logo: Rect,
}

struct LayoutChunks {
    header: HeaderLayout,
    message: Option<Rect>,
    main: Rect,
}

pub fn draw_new(frame: &mut Frame, app: &App) -> () {
    let layout = &make_layout(frame, app);
    draw_logo(frame, layout);
    draw_error(frame, app, layout);

    match &app.active_resource {
        Resource::Tenants { tenants } => draw_tenants(frame, layout, tenants, app.content_cursor),

        Resource::Namespaces { namespaces } => {
            draw_namespaces(frame, layout, namespaces, app.content_cursor)
        }

        Resource::Topics { topics } => draw_topics(frame, layout, topics, app.content_cursor),

        Resource::Subscriptions { subscriptions } => {
            draw_subscriptions(frame, layout, subscriptions, app.content_cursor)
        }

        Resource::Listening { messages, selected_side } => {
            draw_listening(frame, layout, messages, selected_side, app.content_cursor)
        }
    }
}

fn draw_tenants(
    frame: &mut Frame,
    layout: &LayoutChunks,
    tenants: &Vec<Tenant>,
    cursor: Option<usize>,
) -> () {
    let tenants_help = vec![HelpItem::new("<enter>", "namespaces")];
    draw_help(frame, layout, tenants_help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title("Tenants".to_string())
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(
        tenants
            .iter()
            .map(|tenant| tenant.name.to_string()),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_namespaces(
    frame: &mut Frame,
    layout: &LayoutChunks,
    namespaces: &Vec<Namespace>,
    cursor: Option<usize>,
) -> () {
    let help = vec![
        HelpItem::new("<esc>", "back"),
        HelpItem::new("<enter>", "topics"),
    ];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title("Namespaces".to_string())
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(
        namespaces
            .iter()
            .map(|namespace| namespace.name.to_string()),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_topics(
    frame: &mut Frame,
    layout: &LayoutChunks,
    topics: &Vec<Topic>,
    cursor: Option<usize>,
) -> () {
    let help = vec![
        HelpItem::new("<esc>", "back"),
        HelpItem::new("<enter>", "topics"),
    ];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title("Topics".to_string())
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(topics.iter().map(|topic| topic.name.to_string()))
        .block(content_block)
        .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_subscriptions(
    frame: &mut Frame,
    layout: &LayoutChunks,
    subscriptions: &Vec<Subscription>,
    cursor: Option<usize>,
) -> () {
    let help = vec![
        HelpItem::new("<esc>", "back"),
        HelpItem::new("<c-s>", "listen"),
        HelpItem::new("<enter>", "subscriptions"),
    ];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title("Subscriptions".to_string())
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(
        subscriptions
            .iter()
            .map(|subscription| subscription.name.to_string()),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_listening(
    frame: &mut Frame,
    layout: &LayoutChunks,
    messages: &Vec<SubMessage>,
    side: &Side,
    cursor: Option<usize>,
) -> () {
    let help = vec![
        HelpItem::new("<esc>", "back"),
        HelpItem::new("y", "copy to clipboard"),
        HelpItem::new("u", "seek 1h"),
        HelpItem::new("i", "seek 24h"),
        HelpItem::new("o", "seek 1 week"),
        HelpItem::new("<tab>", "cycle selected side"),
    ];
    draw_help(frame, layout, help);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(layout.main);

    let left_rect: Rect = chunks[0];
    let horizontal_space: usize = (left_rect.width - 10).into();
    let right_rect = chunks[1];

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title("Subscriptions".to_string())
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(messages.iter().map(|message| {
        if message.body.len() > horizontal_space {
            format!("{}...", &message.body[..horizontal_space])
        } else {
            message.body.to_string()
        }
    }))
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(cursor);

    let message_body = cursor
        .and_then(|cursor| messages.get(cursor))
        .and_then(|message| serde_json::from_str::<serde_json::Value>(&message.body).ok())
        .and_then(|body_as_json| serde_json::to_string_pretty(&body_as_json).ok());

    let message_properties = cursor
        .and_then(|cursor| messages.get(cursor))
        .map(|message| message.properties.join("\n"));

    let preview_block = Block::default()
        .borders(Borders::ALL)
        .border_type(if matches!(side, Side::Right { .. }) {
            BorderType::Double
        } else {
            BorderType::Plain
        })
        .title("Preview")
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let preview_content = message_properties.and_then(|properties| {
        message_body
            .as_ref()
            .map(|body| properties + "\n\n" + body)
    });

    let scroll_offset = match side {
        Side::Left => (0, 0),
        Side::Right { scroll_offset } => (scroll_offset.clone(), 0),
    };

    let preview_paragraph =
        Paragraph::new(preview_content.unwrap_or(String::from("nothing to show")))
            .block(preview_block)
            .wrap(Wrap { trim: false })
            .scroll(scroll_offset);

    frame.render_stateful_widget(content_list, left_rect, &mut state);
    frame.render_widget(preview_paragraph, right_rect);
}

fn make_layout(frame: &mut Frame, app: &App) -> LayoutChunks {
    match app.error_to_show {
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

            let main = chunks[2];

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

            let main = chunks[1];

            LayoutChunks {
                header: HeaderLayout {
                    help: header_chunks[0],
                    logo: header_chunks[1],
                },
                message: None,
                main,
            }
        }
    }
}

fn draw_logo(frame: &mut Frame, layout: &LayoutChunks) -> () {
    let logo = Paragraph::new(
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

    frame.render_widget(logo, layout.header.logo);
}

fn draw_help(frame: &mut Frame, layout: &LayoutChunks, help_items: Vec<HelpItem>) -> () {
    let help_block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(1, 1, 1, 1));
    let help_list = List::new(help_items).block(help_block);

    frame.render_widget(help_list, layout.header.help);
}

fn draw_error(frame: &mut Frame, app: &App, layout: &LayoutChunks) -> () {
    if let Some((error_to_show, rect_for_error)) = app.error_to_show.as_ref().zip(layout.message) {
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

impl From<HelpItem> for String {
    fn from(value: HelpItem) -> Self {
        Line::from(vec![
            Span::raw(value.keybind).style(Style::default().fg(Color::Green)),
            Span::raw(" "),
            Span::raw(value.description),
        ])
        .into()
    }
}

impl From<HelpItem> for Text<'_> {
    fn from(value: HelpItem) -> Self {
        Text::from(Into::<Line>::into(value))
    }
}
