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

use crate::update::{
    ConfirmationModal, Consumers, Listening, Namespaces, Subscription, Subscriptions, Tenants,
    Topics,
};
use crate::{App, Resource, SelectedPanel};

struct HeaderLayout {
    info_rect: Rect,
    help_rects: Vec<Rect>,
    logo: Rect,
}

struct LayoutChunks {
    header: HeaderLayout,
    message: Option<Rect>,
    main: Rect,
}

pub fn draw_new(frame: &mut Frame, app: &App) {
    let layout = &make_layout(frame, app);
    draw_logo(frame, layout);
    draw_notification(frame, app, layout);
    draw_info(
        frame,
        layout,
        Info {
            cluster_name: LabeledItem::info("cluster:", &app.cluster_name),
        },
    );

    match &app.active_resource {
        Resource::Tenants => draw_tenants(frame, layout, &app.resources.tenants),

        Resource::Namespaces => draw_namespaces(
            frame,
            layout,
            app.resources
                .selected_tenant()
                .map(|tenant| tenant.name.clone())
                .unwrap_or("".to_string()),
            &app.resources.namespaces,
        ),

        Resource::Topics => draw_topics(
            frame,
            layout,
            app.resources
                .selected_namespace()
                .map(|tenant| tenant.name.clone())
                .unwrap_or("".to_string()),
            &app.resources.topics,
        ),

        Resource::Subscriptions => draw_subscriptions(
            frame,
            layout,
            app.resources
                .selected_topic()
                .map(|tenant| tenant.name.clone())
                .unwrap_or("".to_string()),
            &app.resources.subscriptions,
        ),

        Resource::Consumers => draw_consumers(
            frame,
            layout,
            app.resources
                .selected_subscription()
                .map(|sub| sub.name.clone())
                .unwrap_or("".to_string()),
            &app.resources.consumers,
        ),

        Resource::Listening { .. } => draw_listening(
            frame,
            layout,
            &app.resources.listening,
            app.resources
                .selected_topic()
                .map(|topic| topic.name.clone())
                .unwrap_or("".to_string()),
        ),
    }

    if let Some(modal) = app.confirmation_modal.as_ref() {
        draw_confirmation_modal(frame, modal)
    }
}

fn draw_confirmation_modal(frame: &mut Frame, modal: &ConfirmationModal) {
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

fn draw_tenants(frame: &mut Frame, layout: &LayoutChunks, tenants: &Tenants) {
    let tenants_help = vec![LabeledItem::help("<enter>", "namespaces")];
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
            .tenants
            .iter()
            .map(|tenant| tenant.name.to_string()),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(tenants.cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_namespaces(
    frame: &mut Frame,
    layout: &LayoutChunks,
    tenant: String,
    namespaces: &Namespaces,
) {
    let help = vec![
        LabeledItem::help("<esc>", "back"),
        LabeledItem::help("<enter>", "topics"),
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
            .namespaces
            .iter()
            .map(|namespace| namespace.name.to_string()),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(namespaces.cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_topics(frame: &mut Frame, layout: &LayoutChunks, namespace: String, topics: &Topics) {
    let help = vec![
        LabeledItem::help("<esc>", "back"),
        LabeledItem::help("<enter>", "subs"),
        LabeledItem::help("<c-s>", "listen"),
    ];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title(format!("Topics of {}", namespace))
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content_list = List::new(
        topics
            .topics
            .iter()
            .map(|topic| topic.name.to_string()),
    )
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(topics.cursor);

    frame.render_stateful_widget(content_list, layout.main, &mut state);
}

fn draw_subscriptions(
    frame: &mut Frame,
    layout: &LayoutChunks,
    topic: String,
    subscriptions: &Subscriptions,
) {
    let help = vec![
        LabeledItem::help("<esc>", "back"),
        LabeledItem::help("<enter>", "consumers"),
        LabeledItem::help("<c-d>", "delete"),
        LabeledItem::help("<c-p>", "skip backlog"),
        LabeledItem::help("u", "seek 1h"),
        LabeledItem::help("i", "seek 24h"),
        LabeledItem::help("o", "seek 1 week"),
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
        Constraint::Ratio(1, 4),
        Constraint::Ratio(1, 4),
        Constraint::Ratio(1, 4),
        Constraint::Ratio(1, 4),
    ];

    let table = Table::new(
        subscriptions.subscriptions.iter().map(|sub| {
            Row::new(vec![
                Cell::new(sub.name.clone()),
                Cell::new(sub.sub_type.clone()),
                Cell::new(sub.consumer_count.to_string()),
                style_backlog_cell(sub.backlog_size),
            ])
        }),
        widths,
    )
    .header(Row::new(vec![
        "name".to_string(),
        "type".to_string(),
        "consumers".to_string(),
        "backlog".to_string(),
    ]))
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = TableState::default().with_selected(subscriptions.cursor);

    frame.render_stateful_widget(table, layout.main, &mut state);
}

fn draw_consumers(
    frame: &mut Frame,
    layout: &LayoutChunks,
    subscription: String,
    consumers: &Consumers,
) {
    let help = vec![LabeledItem::help("<esc>", "back")];
    draw_help(frame, layout, help);

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Plain)
        .title(format!("Consumers of {subscription}"))
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let widths = [
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
    ];

    let table = Table::new(
        consumers
            .consumers
            .clone()
            .into_iter()
            .map(|consumer| {
                Row::new(vec![
                    Cell::new(consumer.name),
                    Cell::new(consumer.connected_since),
                    Cell::new(consumer.unacked_messages.to_string()),
                ])
            }),
        widths,
    )
    .header(Row::new(vec![
        "name".to_string(),
        "connected since".to_string(),
        "unacked messages".to_string(),
    ]))
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = TableState::default().with_selected(consumers.cursor);

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

fn draw_search(frame: &mut Frame, listening: &Listening, rect: Option<Rect>) {
    if let (Some(search), Some(rect)) = (listening.search.clone(), rect) {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(if matches!(listening.panel, SelectedPanel::Search) {
                BorderType::Double
            } else {
                BorderType::Plain
            })
            .title("Search".to_string())
            .title_alignment(Alignment::Center)
            .title_style(Style::default().fg(Color::Green));

        let paragraph = Paragraph::new(search).block(block);

        frame.render_widget(paragraph, rect);
    }
}

fn draw_listening(
    frame: &mut Frame,
    layout: &LayoutChunks,
    listening: &Listening,
    topic_name: String,
) {
    let help = vec![
        LabeledItem::help("<esc>", "back"),
        LabeledItem::help("<tab>", "cycle panels"),
        LabeledItem::help("u", "seek 1h"),
        LabeledItem::help("i", "seek 24h"),
        LabeledItem::help("o", "seek 1 week"),
        LabeledItem::help("y", "copy to clipboard"),
        LabeledItem::help("/", "toggle search"),
    ];
    draw_help(frame, layout, help);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(layout.main);

    let (left_rect, search_rect) = match listening.search {
        Some(_) => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Percentage(100)])
                .split(chunks[0]);

            (chunks[1], Some(chunks[0]))
        }
        None => (chunks[0], None),
    };

    let horizontal_space: usize = (left_rect.width - 10).into();
    let right_rect = chunks[1];

    let content_block = Block::default()
        .borders(Borders::ALL)
        .border_type(if matches!(listening.panel, SelectedPanel::Left { .. }) {
            BorderType::Double
        } else {
            BorderType::Plain
        })
        .title(format!("Messages of {topic_name}"))
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let filtered_messages = listening.filtered_messages.clone();

    let content_list = List::new(filtered_messages.iter().map(|message| {
        if message.body.len() > horizontal_space {
            format!(
                "{}...",
                &message
                    .body
                    .chars()
                    .take(horizontal_space)
                    .collect::<String>()
            )
        } else {
            message.body.to_string()
        }
    }))
    .block(content_block)
    .highlight_style(Style::default().bg(Color::Green).fg(Color::Black));

    let mut state = ListState::default().with_selected(listening.cursor);

    let message_body = listening
        .cursor
        .and_then(|cursor| filtered_messages.get(cursor))
        .and_then(|message| serde_json::from_str::<serde_json::Value>(&message.body).ok())
        .and_then(|body_as_json| serde_json::to_string_pretty(&body_as_json).ok());

    let message_properties = listening
        .cursor
        .and_then(|cursor| filtered_messages.get(cursor))
        .map(|message| message.properties.join("\n"));

    let preview_block = Block::default()
        .borders(Borders::ALL)
        .border_type(if matches!(listening.panel, SelectedPanel::Right { .. }) {
            BorderType::Double
        } else {
            BorderType::Plain
        })
        .title("Preview")
        .title_alignment(Alignment::Center)
        .title_style(Style::default().fg(Color::Green))
        .padding(Padding::new(2, 2, 1, 1));

    let content = message_properties
        .and_then(|properties| {
            message_body
                .as_ref()
                .map(|body| properties + "\n\n" + body)
        })
        .unwrap_or(String::from("nothing to show"));

    let text: Vec<Line<'_>> = content
        .lines()
        .map(|line| {
            if let Some(search) = listening.search.clone() {
                if line.contains(&search) && !search.is_empty() {
                    if let Some((first_half, second_half)) = line.split_once(&search) {
                        Line::from(vec![
                            Span::raw(first_half),
                            Span::raw(search.clone())
                                .style(Style::default().fg(Color::Black).bg(Color::Green)),
                            Span::raw(second_half),
                        ])
                    //TODO: Fix this yikes
                    } else {
                        Line::from(line)
                    }
                } else {
                    Line::from(line)
                }
            } else {
                Line::from(line)
            }
        })
        .collect();

    let scroll_offset = match listening.panel {
        SelectedPanel::Left => (0, 0),
        SelectedPanel::Search => (0, 0),
        SelectedPanel::Right { scroll_offset } => (scroll_offset, 0),
    };

    let preview_paragraph = Paragraph::new(text)
        .block(preview_block)
        .wrap(Wrap { trim: false })
        .scroll(scroll_offset);

    draw_search(frame, listening, search_rect);
    frame.render_stateful_widget(content_list, left_rect, &mut state);
    frame.render_widget(preview_paragraph, right_rect);
}

fn make_layout(frame: &mut Frame, app: &App) -> LayoutChunks {
    match app.info_to_show {
        Some(_) => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(7),
                    Constraint::Percentage(100),
                    Constraint::Length(1),
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

            LayoutChunks {
                header: HeaderLayout {
                    info_rect: header_chunks[0],
                    help_rects: vec![header_chunks[1], header_chunks[2]],
                    logo: header_chunks[3],
                },
                message: Some(chunks[2]),
                main: chunks[1],
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
                    info_rect: header_chunks[0],
                    help_rects: vec![header_chunks[1], header_chunks[2]],
                    logo: header_chunks[3],
                },
                message: None,
                main,
            }
        }
    }
}

fn draw_logo(frame: &mut Frame, layout: &LayoutChunks) {
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
    .style(Style::default().fg(Color::Green));

    frame.render_widget(logo, layout.header.logo);
}

fn draw_info(frame: &mut Frame, layout: &LayoutChunks, info: Info) {
    let help_block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(1, 1, 1, 1));
    //TODO: add more info lines to a vector, once we have more than just cluster name to show
    let items = vec![Line::from(info.cluster_name)];
    let paragraph = Paragraph::new(Text::from(items)).block(help_block.clone());

    frame.render_widget(paragraph, layout.header.info_rect);
}

fn draw_help(frame: &mut Frame, layout: &LayoutChunks, help_items: Vec<LabeledItem>) {
    let help_block = Block::default()
        .borders(Borders::NONE)
        .padding(Padding::new(1, 1, 1, 1));

    let lines: Vec<Line> = help_items.into_iter().map(Line::from).collect();

    lines
        .chunks(5)
        .map(|lines| Paragraph::new(Text::from(lines.to_vec())).block(help_block.clone()))
        .enumerate()
        .for_each(|(i, p)| frame.render_widget(p, layout.header.help_rects[i]));
}

fn draw_notification(frame: &mut Frame, app: &App, layout: &LayoutChunks) {
    if let Some((info, rect)) = app.info_to_show.as_ref().zip(layout.message) {
        let block = Block::default()
            .borders(Borders::NONE)
            .padding(Padding::horizontal(1));
        let color = if info.is_error {
            Color::Red
        } else {
            Color::Green
        };

        let paragraph = Paragraph::new(info.message.clone())
            .alignment(Alignment::Left)
            .style(Style::default().fg(color))
            .block(block);

        frame.render_widget(paragraph, rect)
    }
}

#[derive(Clone)]
struct Info {
    cluster_name: LabeledItem,
}

#[derive(Clone)]
struct LabeledItem {
    keybind: String,
    description: String,
    color: Color,
}

impl LabeledItem {
    fn help(key: &str, desc: &str) -> LabeledItem {
        LabeledItem {
            keybind: key.to_string(),
            description: desc.to_string(),
            color: Color::Green,
        }
    }

    fn info(key: &str, desc: &str) -> LabeledItem {
        LabeledItem {
            keybind: key.to_string(),
            description: desc.to_string(),
            color: Color::Black,
        }
    }
}

impl From<LabeledItem> for Line<'_> {
    fn from(value: LabeledItem) -> Self {
        Line::from(vec![
            Span::raw(value.keybind).style(
                Style::default()
                    .fg(value.color)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::raw(value.description),
        ])
    }
}

impl From<LabeledItem> for String {
    fn from(value: LabeledItem) -> Self {
        String::from(Line::from(vec![
            Span::raw(value.keybind).style(Style::default().fg(Color::Green)),
            Span::raw(" "),
            Span::raw(value.description),
        ]))
    }
}

impl From<LabeledItem> for Text<'_> {
    fn from(value: LabeledItem) -> Self {
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
