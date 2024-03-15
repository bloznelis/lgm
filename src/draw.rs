use ratatui::layout::Rect;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::Wrap;

use ratatui::{
    prelude::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, BorderType, Borders, List, ListState, Padding, Paragraph},
    Frame,
};

use crate::update::{get_show, get_show_alternative};
use crate::{App, Resource, Side, update};

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

pub fn draw(frame: &mut Frame, app: &App) -> anyhow::Result<()> {
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
