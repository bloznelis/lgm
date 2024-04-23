use std::usize;

use ratatui::layout::Rect;
use ratatui::style::Modifier;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Cell, Row, Table, TableState, Wrap};

use ratatui::{
    prelude::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, BorderType, Borders, Clear, List, ListState, Padding, Paragraph},
    Frame,
};

use crate::update::{ConfirmationModal, Namespace, SubMessage, Subscription, Tenant, Topic};
use crate::{App, Resource, Side};

struct HeaderLayout {
    help_rects: Vec<Rect>,
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

        Resource::Namespaces { namespaces } => draw_namespaces(
            frame,
            layout,
            clone_or_empty(&app.last_tenant),
            namespaces,
            app.content_cursor,
        ),

        Resource::Topics { topics } => draw_topics(
            frame,
            layout,
            clone_or_empty(&app.last_namespace),
            topics,
            app.content_cursor,
        ),

        Resource::Subscriptions { subscriptions } => draw_subscriptions(
            frame,
            layout,
            clone_or_empty(&app.last_topic),
            subscriptions,
            app.content_cursor,
        ),

        Resource::Listening { messages, selected_side } => {
            draw_listening(frame, layout, messages, selected_side, app.content_cursor)
        }
    }

    app.confirmation_modal
        .as_ref()
        .map(|modal| draw_confirmation_modal(frame, modal));
}

fn clone_or_empty(str: &Option<String>) -> String {
    str.clone().unwrap_or("".to_string())
}

fn draw_confirmation_modal(frame: &mut Frame, modal: &ConfirmationModal) -> () {
    let message = format!(
        "{}\n\n n to cancel | <c-a> to accept",
        modal.message.clone()
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .border_style(Style::new().fg(Color::Red));
    let paragraph = Paragraph::new(message)
        .centered()
        .wrap(Wrap { trim: false })
        .block(block)
        .style(Style::new());
    let rect = centered_rect(35, 12, frame.size());

    frame.render_widget(Clear, rect);
    frame.render_widget(paragraph, rect)
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::vertical([
        Constraint::Percentage((100 - percent_y) / 2),
        Constraint::Percentage(percent_y),
        Constraint::Percentage((100 - percent_y) / 2),
    ])
    .split(r);

    Layout::horizontal([
        Constraint::Percentage((100 - percent_x) / 2),
        Constraint::Percentage(percent_x),
        Constraint::Percentage((100 - percent_x) / 2),
    ])
    .split(popup_layout[1])[1]
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
    tenant: String,
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
        .title(format!("Namespaces of {}", tenant))
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
    namespace: String,
    topics: &Vec<Topic>,
    cursor: Option<usize>,
) -> () {
    let help = vec![
        HelpItem::new("<esc>", "back"),
        HelpItem::new("<enter>", "subs"),
        HelpItem::new("<c-s>", "listen"),
    ];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title(format!("Topics of {}", namespace))
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
    topic: String,
    subscriptions: &Vec<Subscription>,
    cursor: Option<usize>,
) -> () {
    let help = vec![
        HelpItem::new("<esc>", "back"),
        HelpItem::new("<c-d>", "delete"),
        HelpItem::new("u", "seek 1h"),
        HelpItem::new("i", "seek 24h"),
        HelpItem::new("o", "seek 1 week"),
    ];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title(format!("Subscriptions of {}", topic))
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let widths = [
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
    ];

    let table = Table::new(
        subscriptions.iter().map(|sub| {
            Row::new(vec![
                Cell::new(sub.name.clone()),
                Cell::new(sub.sub_type.clone()),
                style_backlog_cell(sub.backlog_size),
            ])
        }),
        widths,
    )
    .header(Row::new(vec![
        "name".to_string(),
        "type".to_string(),
        "backlog".to_string(),
    ]))
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = TableState::default().with_selected(cursor);

    frame.render_stateful_widget(table, layout.main, &mut state);
}

fn style_backlog_cell(backlog: i64) -> Cell<'static> {
    let style = match backlog {
        backlog if backlog > 100 => Style::default()
            .fg(Color::Red)
            .add_modifier(Modifier::BOLD),
        backlog if backlog > 10 => Style::default().fg(Color::Yellow),
        _ => Style::default(),
    };

    Cell::new(format!("{}", backlog)).style(style)
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
        HelpItem::new("<tab>", "cycle side"),
        HelpItem::new("u", "seek 1h"),
        HelpItem::new("i", "seek 24h"),
        HelpItem::new("o", "seek 1 week"),
        HelpItem::new("y", "copy to clipboard"),
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
        .border_type(if matches!(side, Side::Left { .. }) {
            BorderType::Double
        } else {
            BorderType::Plain
        })
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
                .constraints([
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(40),
                ])
                .split(chunks[0]);

            let main = chunks[2];

            LayoutChunks {
                header: HeaderLayout {
                    help_rects: vec![header_chunks[0], header_chunks[1], header_chunks[2]],
                    logo: header_chunks[3],
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
                .constraints([
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(20),
                    Constraint::Percentage(40),
                ])
                .split(chunks[0]);

            let main = chunks[1];

            LayoutChunks {
                header: HeaderLayout {
                    help_rects: vec![header_chunks[0], header_chunks[1], header_chunks[2]],
                    logo: header_chunks[3],
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

    let lines: Vec<Line> = help_items
        .into_iter()
        .map(|help| Line::from(help))
        .collect();

    lines
        .chunks(5)
        .into_iter()
        .map(|lines| Paragraph::new(Text::from(lines.to_vec())).block(help_block.clone()))
        .enumerate()
        .for_each(|(i, p)| frame.render_widget(p, layout.header.help_rects[i]));
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
            Span::raw(value.keybind).style(
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::raw(value.description),
        ])
    }
}

impl From<HelpItem> for String {
    fn from(value: HelpItem) -> Self {
        String::from(Line::from(vec![
            Span::raw(value.keybind).style(Style::default().fg(Color::Green)),
            Span::raw(" "),
            Span::raw(value.description),
        ]))
    }
}

impl From<HelpItem> for Text<'_> {
    fn from(value: HelpItem) -> Self {
        Text::from(Into::<Line>::into(value))
    }
}

impl From<Subscription> for Row<'_> {
    fn from(value: Subscription) -> Self {
        Row::new(vec![
            Cell::new(value.name),
            Cell::new(format!("{}", value.backlog_size)),
        ])
    }
}
