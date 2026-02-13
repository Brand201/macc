use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::state::UiStatusLevel;

pub struct Theme {
    pub accent: Color,
    pub accent_dim: Color,
    pub muted: Color,
    pub good: Color,
    pub warn: Color,
    pub bad: Color,
    pub highlight_bg: Color,
}

pub fn theme() -> Theme {
    Theme {
        accent: Color::Cyan,
        accent_dim: Color::LightCyan,
        muted: Color::Gray,
        good: Color::Green,
        warn: Color::Yellow,
        bad: Color::Red,
        highlight_bg: Color::DarkGray,
    }
}

pub fn panel<'a>(title: &'a str) -> Block<'a> {
    Block::default().borders(Borders::ALL).title(title)
}

pub fn wrapped_paragraph<'a>(text: impl Into<String>, title: &'a str) -> Paragraph<'a> {
    Paragraph::new(text.into())
        .block(panel(title))
        .wrap(Wrap { trim: false })
}

pub fn truncate_middle(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    if max_chars < 8 {
        return input.chars().take(max_chars).collect();
    }
    let keep = (max_chars - 3) / 2;
    let start: String = input.chars().take(keep).collect();
    let end: String = input
        .chars()
        .rev()
        .take(keep)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{}...{}", start, end)
}

pub fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

pub fn status_style(level: UiStatusLevel, t: &Theme) -> Style {
    match level {
        UiStatusLevel::Info => Style::default().fg(t.accent_dim),
        UiStatusLevel::Success => Style::default().fg(t.good),
        UiStatusLevel::Warning => Style::default().fg(t.warn),
        UiStatusLevel::Error => Style::default().fg(t.bad).add_modifier(Modifier::BOLD),
    }
}

pub fn compact_help_line(items: Vec<(&str, &str)>, max_chars: usize) -> String {
    let mut out = String::new();
    for (k, d) in items {
        let chunk = if out.is_empty() {
            format!("{}: {}", k, d)
        } else {
            format!(" | {}: {}", k, d)
        };
        if out.chars().count() + chunk.chars().count() > max_chars {
            if out.is_empty() {
                return truncate_middle(&chunk, max_chars);
            }
            break;
        }
        out.push_str(&chunk);
    }
    out
}

pub struct HeaderContext<'a> {
    pub app_name: &'a str,
    pub screen_title: &'a str,
    pub mode: &'a str,
    pub project: &'a str,
    pub config_label: &'a str,
    pub errors: usize,
    pub status: Option<(UiStatusLevel, String)>,
    pub width: u16,
}

pub fn header_lines(ctx: &HeaderContext<'_>, t: &Theme) -> Vec<Line<'static>> {
    let max = ctx.width.saturating_sub(6) as usize;
    let project_short = truncate_middle(ctx.project, max.saturating_sub(12));
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                ctx.app_name.to_string(),
                Style::default().fg(t.accent).add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(
                ctx.screen_title.to_string(),
                Style::default().fg(t.accent_dim),
            ),
            Span::raw("  "),
            Span::styled("mode", Style::default().fg(t.muted)),
            Span::raw(": "),
            Span::styled(ctx.mode.to_string(), Style::default().fg(t.accent)),
        ]),
        Line::from(vec![
            Span::styled("project", Style::default().fg(t.muted)),
            Span::raw(": "),
            Span::raw(project_short),
            Span::raw("  "),
            Span::styled("config", Style::default().fg(t.muted)),
            Span::raw(": "),
            Span::raw(ctx.config_label.to_string()),
            Span::raw("  "),
            Span::styled("errors", Style::default().fg(t.muted)),
            Span::raw(": "),
            Span::styled(
                ctx.errors.to_string(),
                Style::default().fg(if ctx.errors == 0 { t.good } else { t.bad }),
            ),
        ]),
    ];

    if let Some((lvl, msg)) = &ctx.status {
        lines.push(Line::from(vec![
            Span::styled("status", Style::default().fg(t.muted)),
            Span::raw(": "),
            Span::styled(truncate_middle(msg, max), status_style(*lvl, t)),
        ]));
    } else {
        lines.push(Line::from(vec![
            Span::styled("status", Style::default().fg(t.muted)),
            Span::raw(": idle"),
        ]));
    }
    lines
}
