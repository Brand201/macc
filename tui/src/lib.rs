use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use std::{collections::BTreeMap, io, time::Duration};

pub mod screen;
pub mod state;
pub mod ui;

use macc_core::plan::{PlannedOpKind, Scope};
use macc_core::tool::{FieldDefault, FieldKind};
use screen::Screen;
use state::AppState;
use ui::{compact_help_line, header_lines, panel, theme, wrapped_paragraph};

/// RAII guard to ensure terminal state is restored on drop.
struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<io::Stdout>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen,);
        let _ = self.terminal.show_cursor();
    }
}

pub fn run_tui() -> Result<()> {
    let mut guard = TerminalGuard::new()?;
    let registry = macc_registry::default_registry();
    let engine = std::sync::Arc::new(macc_core::MaccEngine::new(registry));
    let mut state = AppState::new(engine);

    run_app(&mut guard.terminal, &mut state)?;

    Ok(())
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, state: &mut AppState) -> io::Result<()> {
    loop {
        state.tick();
        terminal.draw(|f| ui(f, state, true))?;

        if event::poll(Duration::from_millis(16))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    handle_key(state, key.code);
                }
            }
        }
        if state.should_quit {
            return Ok(());
        }
    }
}

fn handle_key(state: &mut AppState, key: KeyCode) {
    if state.help_open {
        match key {
            KeyCode::Char('?') | KeyCode::Esc | KeyCode::Char('q') => {
                state.toggle_help();
                return;
            }
            _ => return,
        }
    }

    if state.search_editing {
        match key {
            KeyCode::Enter => state.commit_search(),
            KeyCode::Esc => state.cancel_search(),
            KeyCode::Backspace => state.pop_search_char(),
            KeyCode::Char(c) => state.append_search_char(c),
            _ => {}
        }
        return;
    }

    let current_screen = state.current_screen();

    if current_screen == Screen::ToolSettings && state.is_tool_field_editing() {
        match key {
            KeyCode::Enter => state.commit_tool_field_edit(),
            KeyCode::Esc => state.cancel_tool_field_edit(),
            KeyCode::Backspace => state.pop_tool_field_char(),
            KeyCode::Char(c) => state.append_tool_field_char(c),
            _ => {}
        }
        return;
    }
    if current_screen == Screen::Automation && state.is_automation_field_editing() {
        match key {
            KeyCode::Enter => state.commit_automation_field_edit(),
            KeyCode::Esc => state.cancel_automation_field_edit(),
            KeyCode::Backspace => state.pop_automation_field_char(),
            KeyCode::Char(c) => state.append_automation_field_char(c),
            _ => {}
        }
        return;
    }
    if current_screen == Screen::Tools && state.is_tool_install_confirmation_open() {
        match key {
            KeyCode::Char('y') | KeyCode::Enter => state.confirm_tool_install(),
            KeyCode::Char('n') | KeyCode::Esc => state.cancel_tool_install_confirmation(),
            _ => {}
        }
        return;
    }

    match key {
        KeyCode::Char('?') => {
            state.toggle_help();
        }
        KeyCode::Char('/') => {
            state.begin_search();
        }
        KeyCode::Enter => {
            state.navigate_enter();
        }
        KeyCode::Backspace if current_screen == Screen::Apply => {
            state.pop_apply_consent_char();
        }
        KeyCode::Char(c) if current_screen == Screen::Apply && !matches!(c, 'q' | 'Q') => {
            state.append_apply_consent_char(c);
        }
        // Navigation: Esc/q to quit (or pop if in a sub-screen)
        KeyCode::Esc | KeyCode::Char('q') => {
            if state.screen_stack.len() > 1 {
                state.pop_screen();
            } else {
                state.should_quit = true;
            }
        }
        // Navigation: 'h' for Home, 't' for Tools
        KeyCode::Char('h') => state.goto_screen(Screen::Home),
        KeyCode::Char('t') => state.push_screen(Screen::Tools),
        KeyCode::Char('o') => state.push_screen(Screen::Automation),
        KeyCode::Char('m') => state.push_screen(Screen::Mcp),
        KeyCode::Char('g') => state.push_screen(Screen::Logs),
        KeyCode::Char('p') => state.open_preview(),
        KeyCode::Char('x') if current_screen != Screen::Apply => {
            state.open_apply_screen();
        }

        // Navigation: Backspace to go back
        KeyCode::Backspace => state.pop_screen(),

        // Actions: 's' to Save Config
        KeyCode::Char('s') if current_screen != Screen::Apply => {
            state.save_config();
        }
        KeyCode::Char('u') => {
            state.undo_config_change();
        }
        KeyCode::Char('U') => {
            state.redo_config_change();
        }

        // Screen-specific controls
        KeyCode::Up => {
            state.navigate_prev();
        }
        KeyCode::Down => {
            state.navigate_next();
        }
        KeyCode::PageUp => {
            if current_screen == Screen::Preview {
                state.scroll_preview_diff(-10);
            } else if current_screen == Screen::Logs {
                state.scroll_log_content(-10);
            }
        }
        KeyCode::PageDown => {
            if current_screen == Screen::Preview {
                state.scroll_preview_diff(10);
            } else if current_screen == Screen::Logs {
                state.scroll_log_content(10);
            }
        }
        KeyCode::Char(' ') | KeyCode::Right => {
            state.navigate_toggle();
        }
        KeyCode::Char('a') => {
            if current_screen == Screen::Skills {
                state.select_all_skills();
            } else if current_screen == Screen::Agents {
                state.select_all_agents();
            } else if current_screen == Screen::Mcp {
                state.select_all_mcp();
            } else {
                state.push_screen(Screen::About);
            }
        }
        KeyCode::Char('n') => {
            if current_screen == Screen::Skills {
                state.select_no_skills();
            } else if current_screen == Screen::Agents {
                state.select_no_agents();
            } else if current_screen == Screen::Mcp {
                state.select_no_mcp();
            }
        }
        KeyCode::Char('r') => {
            if current_screen == Screen::Automation {
                state.start_coordinator_action("run");
            } else if current_screen == Screen::Logs {
                state.refresh_logs();
            } else if current_screen == Screen::Preview {
                state.refresh_preview_plan();
            }
        }
        KeyCode::Char('y') => {
            if current_screen == Screen::Automation {
                state.start_coordinator_action("sync");
            }
        }
        KeyCode::Char('c') => {
            if current_screen == Screen::Automation {
                state.start_coordinator_action("reconcile");
            }
        }
        KeyCode::Char('k') => {
            if current_screen == Screen::Automation {
                state.stop_coordinator_action();
            }
        }
        KeyCode::Char('l') => {
            if current_screen == Screen::Automation {
                state.refresh_coordinator_snapshot();
            }
        }
        KeyCode::Char('d') => {
            if current_screen == Screen::Tools {
                state.refresh_tool_checks();
            }
        }
        KeyCode::Char('i') => {
            if current_screen == Screen::Tools {
                state.begin_tool_install_confirmation();
            }
        }
        _ => {}
    }
}

fn ui(f: &mut Frame, state: &AppState, full_clear: bool) {
    let theme = theme();
    if full_clear {
        f.render_widget(Clear, f.size());
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(6), // Title + status badges
            Constraint::Min(0),    // Body
            Constraint::Length(4), // Footer / Navigation help
        ])
        .split(f.size());

    let current_screen = state.current_screen();

    // Header
    let project_label = state
        .project_paths
        .as_ref()
        .map(|p| p.root.display().to_string())
        .unwrap_or_else(|| "(no project)".to_string());
    let (config_label, config_color) = if state.working_copy.is_some() {
        ("loaded", theme.good)
    } else {
        ("missing", theme.warn)
    };
    let config_status = format!(
        "{} ({})",
        config_label,
        if config_color == theme.good {
            "ok"
        } else {
            "warn"
        }
    );
    let title = Paragraph::new(header_lines(
        "[M][A][C][C]",
        current_screen.title(),
        state.interaction_mode_label(),
        &project_label,
        &config_status,
        state.errors.len(),
        state.status_line(),
        chunks[0].width,
        &theme,
    ))
    .block(panel("MACC"));
    f.render_widget(title, chunks[0]);

    // Body
    match current_screen {
        Screen::Skills => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[1]);
            let selected_skills = state.selected_skills();

            let mut list_state = ListState::default();
            let visible = state.filtered_skill_indices();
            if !visible.is_empty() {
                let idx = if visible.contains(&state.skill_selection_index) {
                    state.skill_selection_index
                } else {
                    visible[0]
                };
                let selected_pos = visible.iter().position(|v| *v == idx).unwrap_or(0);
                list_state.select(Some(selected_pos));
            }

            let items: Vec<ListItem> = state
                .filtered_skill_indices()
                .iter()
                .map(|index| {
                    let skill = &state.skills[*index];
                    let is_enabled = selected_skills.contains(&skill.id.to_string());
                    let enabled_marker = if is_enabled { "[x]" } else { "[ ]" };
                    ListItem::new(Line::from(vec![
                        Span::raw(enabled_marker),
                        Span::raw(" "),
                        Span::raw(skill.name.clone()),
                    ]))
                })
                .collect();

            let title = format!(
                "Skills ({}/{}, shown {})",
                selected_skills.len(),
                state.skills.len(),
                visible.len()
            );
            let list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title(title))
                .highlight_symbol("› ")
                .highlight_style(Style::default().bg(theme.highlight_bg));
            f.render_stateful_widget(list, body_chunks[0], &mut list_state);

            if visible.is_empty() {
                f.render_widget(
                    Paragraph::new("No matching skills. Press '/' to edit search.")
                        .block(Block::default().borders(Borders::ALL)),
                    body_chunks[1],
                );
            } else {
                let selected = if visible.contains(&state.skill_selection_index) {
                    state.skill_selection_index
                } else {
                    visible[0]
                };
                let current_skill = &state.skills[selected];
                let mut desc_text = format!("ID: {}\n\n", current_skill.id);
                desc_text.push_str("Description:\n");
                desc_text.push_str(&current_skill.description);
                desc_text.push_str("\n\n---\nShortcuts:\n'a' - Select All\n'n' - Select None");

                let desc_para = Paragraph::new(desc_text).block(panel("Details"));
                f.render_widget(desc_para, body_chunks[1]);
            }
        }
        Screen::Mcp => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[1]);
            let selected_ids = state
                .working_copy
                .as_ref()
                .and_then(|c| c.selections.as_ref())
                .map(|s| s.mcp.clone())
                .unwrap_or_default();

            let mut list_state = ListState::default();
            let visible = state.filtered_mcp_indices();
            if !visible.is_empty() {
                let idx = if visible.contains(&state.mcp_selection_index) {
                    state.mcp_selection_index
                } else {
                    visible[0]
                };
                let selected_pos = visible.iter().position(|v| *v == idx).unwrap_or(0);
                list_state.select(Some(selected_pos));
            }

            let items: Vec<ListItem> = state
                .filtered_mcp_indices()
                .iter()
                .map(|index| {
                    let entry = &state.mcp_entries[*index];
                    let is_enabled = selected_ids.contains(&entry.id.to_string());
                    let enabled_marker = if is_enabled { "[x]" } else { "[ ]" };
                    ListItem::new(Line::from(vec![
                        Span::raw(enabled_marker),
                        Span::raw(" "),
                        Span::raw(entry.name.clone()),
                    ]))
                })
                .collect();

            let title = format!(
                "MCP Servers ({}/{}, shown {})",
                selected_ids.len(),
                state.mcp_entries.len(),
                visible.len()
            );
            let list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title(title))
                .highlight_symbol("› ")
                .highlight_style(Style::default().bg(theme.highlight_bg));
            f.render_stateful_widget(list, body_chunks[0], &mut list_state);

            if visible.is_empty() {
                f.render_widget(
                    Paragraph::new("No matching MCP entries. Press '/' to edit search.")
                        .block(Block::default().borders(Borders::ALL)),
                    body_chunks[1],
                );
            } else {
                let selected = if visible.contains(&state.mcp_selection_index) {
                    state.mcp_selection_index
                } else {
                    visible[0]
                };
                let current = &state.mcp_entries[selected];
                let kind = match current.source.kind {
                    macc_core::catalog::SourceKind::Git => "git",
                    macc_core::catalog::SourceKind::Http => "http",
                    macc_core::catalog::SourceKind::Local => "local",
                };
                let mut detail = format!(
                    "ID: {}\nName: {}\nKind: {}\n",
                    current.id, current.name, kind
                );
                detail.push_str("\nDescription:\n");
                detail.push_str(&current.description);
                detail.push_str("\n\nTags:\n");
                if current.tags.is_empty() {
                    detail.push_str("(none)");
                } else {
                    detail.push_str(&current.tags.join(", "));
                }
                detail.push_str("\n\nNotes:\n- MCP packages are merged into .mcp.json on apply.\n- Secrets are never stored by MACC.\n\nShortcuts:\n'a' - Select All\n'n' - Select None");

                let desc_para = Paragraph::new(detail).block(panel("Details"));
                f.render_widget(desc_para, body_chunks[1]);
            }
        }
        Screen::Logs => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(38), Constraint::Percentage(62)])
                .split(chunks[1]);

            let mut list_state = ListState::default();
            let visible = state.filtered_log_indices();
            if !visible.is_empty() {
                let idx = if visible.contains(&state.log_selection_index) {
                    state.log_selection_index
                } else {
                    visible[0]
                };
                let selected_pos = visible.iter().position(|v| *v == idx).unwrap_or(0);
                list_state.select(Some(selected_pos));
            }
            let items: Vec<ListItem> = state
                .filtered_log_indices()
                .iter()
                .map(|index| {
                    let entry = &state.log_entries[*index];
                    ListItem::new(Line::from(vec![Span::raw(entry.relative.clone())]))
                })
                .collect();
            let list_title = format!(
                "Log Files (shown {}/{})",
                visible.len(),
                state.log_entries.len()
            );
            let list = List::new(items)
                .block(panel(&list_title))
                .highlight_symbol("› ")
                .highlight_style(Style::default().bg(theme.highlight_bg));
            f.render_stateful_widget(list, body_chunks[0], &mut list_state);

            let selected = state
                .log_entries
                .get(state.log_selection_index)
                .map(|e| e.path.display().to_string())
                .unwrap_or_else(|| "(none)".to_string());
            let content_title = format!("Content: {}", selected);
            let content = Paragraph::new(state.log_view_content.clone())
                .block(panel(&content_title))
                .scroll((state.log_content_scroll as u16, 0))
                .wrap(Wrap { trim: false });
            f.render_widget(content, body_chunks[1]);
        }
        Screen::Agents => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(chunks[1]);

            let selected_agents = state.selected_agents();

            let mut list_state = ListState::default();
            let visible = state.filtered_agent_indices();
            if !visible.is_empty() {
                let idx = if visible.contains(&state.agent_selection_index) {
                    state.agent_selection_index
                } else {
                    visible[0]
                };
                let selected_pos = visible.iter().position(|v| *v == idx).unwrap_or(0);
                list_state.select(Some(selected_pos));
            }

            let items: Vec<ListItem> = state
                .filtered_agent_indices()
                .iter()
                .map(|index| {
                    let agent = &state.agents[*index];
                    let is_enabled = selected_agents.contains(&agent.id.to_string());
                    let enabled_marker = if is_enabled { "[x]" } else { "[ ]" };
                    ListItem::new(Line::from(vec![
                        Span::raw(enabled_marker),
                        Span::raw(" "),
                        Span::raw(agent.name.clone()),
                    ]))
                })
                .collect();

            let title = format!(
                "Agents ({}/{}, shown {})",
                selected_agents.len(),
                state.agents.len(),
                visible.len()
            );
            let list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title(title))
                .highlight_symbol("› ")
                .highlight_style(Style::default().bg(theme.highlight_bg));
            f.render_stateful_widget(list, body_chunks[0], &mut list_state);

            if visible.is_empty() {
                f.render_widget(
                    Paragraph::new("No matching agents. Press '/' to edit search.")
                        .block(Block::default().borders(Borders::ALL)),
                    body_chunks[1],
                );
            } else {
                let selected = if visible.contains(&state.agent_selection_index) {
                    state.agent_selection_index
                } else {
                    visible[0]
                };
                let current_agent = &state.agents[selected];
                let mut desc_text = format!("ID: {}\n\n", current_agent.id);
                desc_text.push_str("Purpose:\n");
                desc_text.push_str(&current_agent.description);
                desc_text.push_str("\n\n---\nShortcuts:\n'a' - Select All\n'n' - Select None");

                let desc_para = Paragraph::new(desc_text).block(panel("Details"));
                f.render_widget(desc_para, body_chunks[1]);
            }
        }
        Screen::Preview => {
            let mut preview_constraints = Vec::new();
            if state.preview_error.is_some() {
                preview_constraints.push(Constraint::Length(3));
            }
            preview_constraints.push(Constraint::Length(4));
            preview_constraints.push(Constraint::Min(0));

            let preview_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(&preview_constraints)
                .split(chunks[1]);

            let mut chunk_index = 0;
            if let Some(error) = &state.preview_error {
                let error_para = Paragraph::new(error.clone())
                    .style(Style::default().fg(Color::Red))
                    .block(
                        Block::default()
                            .borders(Borders::ALL)
                            .title("Preview Error"),
                    );
                f.render_widget(error_para, preview_chunks[chunk_index]);
                chunk_index += 1;
            }

            let summary_rect = preview_chunks[chunk_index];
            chunk_index += 1;

            let mut kind_counts: BTreeMap<&str, usize> = BTreeMap::new();
            let mut project_ops = 0;
            let mut user_ops = 0;
            for op in &state.preview_ops {
                let kind = kind_label(op.kind);
                *kind_counts.entry(kind).or_insert(0) += 1;
                match op.scope {
                    Scope::Project => project_ops += 1,
                    Scope::User => user_ops += 1,
                }
            }

            let kind_summary = if kind_counts.is_empty() {
                "(none)".to_string()
            } else {
                kind_counts
                    .iter()
                    .map(|(kind, count)| format!("{} {}", kind, count))
                    .collect::<Vec<_>>()
                    .join(", ")
            };

            let mut summary_text = format!(
                "Planned operations: {}\nKinds: {}\nScopes: project {} | user {}",
                state.preview_ops.len(),
                kind_summary,
                project_ops,
                user_ops
            );
            summary_text.push_str(
                "\nPress 'x' to open Apply (consent required for any user-level operations).",
            );

            let summary_para = Paragraph::new(summary_text).block(panel("Summary"));
            f.render_widget(summary_para, summary_rect);

            let content_rect = preview_chunks[chunk_index];
            let columns = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                .split(content_rect);

            let mut list_state = ListState::default();
            if !state.preview_ops.is_empty() {
                let idx = state
                    .preview_selection_index
                    .min(state.preview_ops.len() - 1);
                list_state.select(Some(idx));
            }

            let list_items = if state.preview_ops.is_empty() {
                vec![ListItem::new(
                    "No planned operations yet. Press 'r' to recompute.",
                )]
            } else {
                state
                    .preview_ops
                    .iter()
                    .map(|op| {
                        let line = format!(
                            "{:<7} {:<8} {}",
                            kind_label(op.kind),
                            scope_label(op.scope),
                            op.path
                        );
                        ListItem::new(line)
                    })
                    .collect()
            };

            let operations_list = List::new(list_items)
                .block(panel("Planned Operations"))
                .highlight_symbol("▶ ")
                .highlight_style(Style::default().fg(Color::Yellow));
            f.render_stateful_widget(operations_list, columns[0], &mut list_state);

            let detail_text = if let Some(op) = state.selected_preview_op() {
                let mut text = format!("Path: {}\n", op.path);
                text.push_str(&format!("Action: {}\n", kind_label(op.kind)));
                text.push_str(&format!("Scope: {}\n", scope_label(op.scope)));
                text.push_str(&format!(
                    "Backup required: {}\n",
                    if op.metadata.backup_required {
                        "yes"
                    } else {
                        "no"
                    }
                ));
                text.push_str(&format!(
                    "Consent required: {}\n",
                    if op.consent_required { "yes" } else { "no" }
                ));
                text.push_str(&format!(
                    "Before data: {}\n",
                    if op.before.is_some() {
                        "available"
                    } else {
                        "empty"
                    }
                ));
                text.push_str(&format!(
                    "After data: {}\n",
                    if op.after.is_some() {
                        "available"
                    } else {
                        "empty"
                    }
                ));
                text
            } else {
                "Select an operation to see metadata.".to_string()
            };

            let detail_column = columns[1];
            let diff_view = state.preview_diff_for_selected();
            let diff_truncated = diff_view.map(|view| view.truncated).unwrap_or(false);

            let mut detail_constraints = vec![Constraint::Length(10)];
            if diff_truncated {
                detail_constraints.push(Constraint::Length(2));
            }
            detail_constraints.push(Constraint::Min(0));

            let detail_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(&detail_constraints)
                .split(detail_column);

            let metadata_rect = detail_chunks[0];
            let mut detail_index = 1;
            let trunc_rect = if diff_truncated {
                let rect = detail_chunks[detail_index];
                detail_index += 1;
                Some(rect)
            } else {
                None
            };
            let diff_rect = detail_chunks[detail_index];

            let detail_para = Paragraph::new(detail_text).block(panel("Details"));
            f.render_widget(detail_para, metadata_rect);

            if let Some(rect) = trunc_rect {
                let notice = Paragraph::new(
                    "Large diff truncated; scroll with PageUp/PageDown to view more.",
                )
                .style(Style::default().fg(Color::Yellow))
                .block(panel("Truncation Notice"));
                f.render_widget(notice, rect);
            }

            let diff_text = if let Some(view) = diff_view {
                let start = state.preview_diff_scroll_position();
                let window_height = diff_rect.height as usize;
                let window_rows = window_height.saturating_sub(2).max(1);
                let line_count = view.diff.lines().count();
                let slice = view
                    .diff
                    .lines()
                    .skip(start)
                    .take(window_rows)
                    .collect::<Vec<_>>();

                if slice.is_empty() {
                    if line_count == 0 {
                        "Diff unavailable for this operation.".to_string()
                    } else if start >= line_count {
                        "(End of diff)".to_string()
                    } else {
                        "".to_string()
                    }
                } else {
                    slice.join("\n")
                }
            } else {
                "Select an operation to view its diff.".to_string()
            };

            let diff_para = Paragraph::new(diff_text)
                .block(panel("Diff (PgUp/PgDn to scroll)"))
                .wrap(Wrap { trim: false });
            f.render_widget(diff_para, diff_rect);
        }
        Screen::Apply => {
            let apply_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(20),
                    Constraint::Percentage(40),
                    Constraint::Percentage(15),
                    Constraint::Percentage(25),
                ])
                .split(chunks[1]);

            let summary_rect = apply_chunks[0];
            let operations_rect = apply_chunks[1];
            let consent_rect = apply_chunks[2];
            let result_rect = apply_chunks[3];

            let progress_text = if let Some(progress) = &state.apply_progress {
                let path = progress.path.as_deref().unwrap_or("(pending)");
                format!(
                    "Progress: {}/{} operations (current: {})",
                    progress.current, progress.total, path
                )
            } else {
                "Progress: not started".to_string()
            };

            let summary_base = if let Some(ctx) = &state.apply_context {
                let consent_required = if ctx.needs_user_consent() {
                    "yes"
                } else {
                    "no"
                };
                format!(
                    "Total operations: {}\nProject: {}\nUser: {}\nBackup preview: {}\nUser consent required: {}",
                    ctx.operations.len(),
                    ctx.project_ops,
                    ctx.user_ops,
                    ctx.backup_preview,
                    consent_required
                )
            } else {
                state
                    .apply_error
                    .as_ref()
                    .map(|e| format!("Unable to compute apply plan:\n{}", e))
                    .unwrap_or_else(|| {
                        "Plan unavailable. Try refreshing the preview first.".to_string()
                    })
            };

            let apply_stage = if state.apply_error.is_some() {
                "failed"
            } else if state.apply_feedback.is_some() {
                "done"
            } else if state.apply_progress.is_some() {
                "running"
            } else {
                "ready"
            };
            let summary_text = format!(
                "Pipeline: plan -> consent -> apply -> verify\nStage: {}\n\n{}\n{}",
                apply_stage, summary_base, progress_text
            );

            let summary_para = Paragraph::new(summary_text)
                .block(panel("Apply Summary"))
                .wrap(Wrap { trim: false });
            f.render_widget(summary_para, summary_rect);

            let operations_text = if let Some(ctx) = &state.apply_context {
                if ctx.operations.is_empty() {
                    "No operations planned.".to_string()
                } else {
                    let mut lines: Vec<String> = ctx
                        .operations
                        .iter()
                        .take(10)
                        .map(|op| {
                            format!(
                                "[{}] {:<8} {}",
                                scope_label(op.scope),
                                kind_label(op.kind),
                                op.path
                            )
                        })
                        .collect();
                    if ctx.operations.len() > 10 {
                        lines.push(format!(
                            "...and {} more operations",
                            ctx.operations.len() - 10
                        ));
                    }
                    lines.join("\n")
                }
            } else {
                "Preview the plan first to see operation details.".to_string()
            };

            let operations_para = Paragraph::new(operations_text)
                .block(panel("Operation Snapshot"))
                .wrap(Wrap { trim: false });
            f.render_widget(operations_para, operations_rect);

            let consent_prompt = if let Some(ctx) = &state.apply_context {
                if ctx.needs_user_consent() {
                    "Type YES (case-insensitive) below to allow user-scope operations."
                } else {
                    "No user-scope operations detected; press Enter to apply the project-only changes."
                }
            } else {
                "Consent state unavailable until a plan is computed."
            };
            let consent_status = if state.apply_user_consent_granted {
                "Consent confirmed"
            } else {
                "Consent pending"
            };
            let consent_input = if state.apply_consent_input.is_empty() {
                "<empty>".to_string()
            } else {
                state.apply_consent_input.clone()
            };
            let consent_text = format!(
                "{}\nInput buffer: {}\n{}",
                consent_status, consent_input, consent_prompt
            );
            let consent_para = Paragraph::new(consent_text)
                .block(panel("User Consent"))
                .wrap(Wrap { trim: false });
            f.render_widget(consent_para, consent_rect);

            let result_text = if let Some(err) = &state.apply_error {
                format!("Error applying changes:\n{}", err)
            } else if let Some(feedback) = &state.apply_feedback {
                feedback.clone()
            } else {
                "Awaiting apply. Press Enter to run once consent requirements (if any) are satisfied."
                    .to_string()
            };
            let result_style = if state.apply_error.is_some() {
                Style::default().fg(Color::Red)
            } else if state.apply_feedback.is_some() {
                Style::default().fg(Color::Green)
            } else {
                Style::default()
            };
            let result_para = Paragraph::new(result_text)
                .style(result_style)
                .block(panel("Apply Result"))
                .wrap(Wrap { trim: false });
            f.render_widget(result_para, result_rect);
        }
        Screen::Home => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                .split(chunks[1]);

            let mut overview = String::new();
            if !state.errors.is_empty() {
                overview.push_str("Errors:\n");
                for err in &state.errors {
                    overview.push_str(&format!("- {}\n", err));
                }
                overview.push('\n');
            }
            if !state.notices.is_empty() {
                overview.push_str("Notices:\n");
                for notice in &state.notices {
                    overview.push_str(&format!("- {}\n", notice));
                }
                overview.push('\n');
            }
            if let Some(paths) = &state.project_paths {
                overview.push_str(&format!("Project Root: {}\n", paths.root.display()));
            }
            if let Some(config) = &state.working_copy {
                let titles: Vec<String> = config
                    .tools
                    .enabled
                    .iter()
                    .map(|id| {
                        state
                            .tool_descriptors
                            .iter()
                            .find(|d| &d.id == id)
                            .map(|d| d.title.clone())
                            .unwrap_or_else(|| id.clone())
                    })
                    .collect();
                overview.push_str(&format!("Enabled Tools: {}\n", titles.join(", ")));
                let mcp_selected = config
                    .selections
                    .as_ref()
                    .map(|s| s.mcp.clone())
                    .unwrap_or_default();
                overview.push_str(&format!(
                    "MCP Servers Selected: {}\n",
                    if mcp_selected.is_empty() {
                        "(none)".to_string()
                    } else {
                        mcp_selected.join(", ")
                    }
                ));
            } else if state.errors.is_empty() {
                overview.push_str("No configuration loaded.\n");
            }

            if let Some(status) = &state.worktree_status {
                if let Some(err) = &status.error {
                    overview.push_str(&format!("Worktree Status: unavailable ({})\n", err));
                } else if let Some(current) = &status.current {
                    let branch = current.branch.as_deref().unwrap_or("-");
                    let head = current.head.as_deref().unwrap_or("-");
                    let name = current
                        .path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("-");
                    overview.push_str(&format!(
                        "Worktree: {} (branch: {}, head: {})\n",
                        name, branch, head
                    ));
                    overview.push_str(&format!("Worktrees Total: {}\n", status.total));
                } else {
                    overview.push_str(&format!(
                        "Worktree: (none) | Worktrees Total: {}\n",
                        status.total
                    ));
                }
            }

            let overview_para = wrapped_paragraph(overview, "Overview");
            f.render_widget(overview_para, body_chunks[0]);

            let next_steps = "Quick actions:\n\n- Press 't' to configure tools\n- Press 'o' to configure automation/coordinator\n- Press 'm' to select MCP servers\n- Press 'p' to preview changes\n- Press 'x' to apply changes\n- Press 's' to save\n\nTip: Use '?' anywhere for full keybindings.";
            let steps_para = wrapped_paragraph(next_steps, "Next Steps");
            f.render_widget(steps_para, body_chunks[1]);
        }
        Screen::Automation => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
                .split(chunks[1]);
            let right_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
                .split(body_chunks[1]);

            let mut list_state = ListState::default();
            let automation_count = state.automation_field_count();
            list_state.select(Some(
                state
                    .automation_field_index
                    .min(automation_count.saturating_sub(1)),
            ));
            let items: Vec<ListItem> = (0..automation_count)
                .map(|i| {
                    let label = state.automation_field_label(i);
                    let value = if i == state.automation_field_index
                        && state.is_automation_field_editing()
                    {
                        format!("{}_", state.automation_field_input)
                    } else {
                        state.automation_field_display_value(i)
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(format!("{:<30}", label), Style::default().fg(theme.muted)),
                        Span::raw(" "),
                        Span::raw(value),
                    ]))
                })
                .collect();

            let list = List::new(items)
                .block(panel("Coordinator Settings"))
                .highlight_symbol("› ")
                .highlight_style(Style::default().bg(theme.highlight_bg));
            f.render_stateful_widget(list, body_chunks[0], &mut list_state);

            let idx = state
                .automation_field_index
                .min(automation_count.saturating_sub(1));
            let mut detail = format!(
                "Field: {}\n\n{}\n\nCurrent: {}\n\nShortcuts:\nSpace/Enter - Edit or cycle\nEsc - Cancel edit\ns - Save to .macc/macc.yaml",
                state.automation_field_label(idx),
                state.automation_field_help(idx),
                state.automation_field_display_value(idx),
            );
            if let Some(validation) = state.current_automation_field_validation() {
                detail.push_str(&format!("\n\nValidation:\n{}", validation));
            }
            let detail_para = wrapped_paragraph(detail, "Field Info");
            f.render_widget(detail_para, right_chunks[0]);

            let status_line = if state.is_coordinator_running() {
                format!(
                    "Running: {} ({}s)",
                    state
                        .coordinator_running_action
                        .as_deref()
                        .unwrap_or("unknown"),
                    state.coordinator_elapsed_seconds().unwrap_or(0)
                )
            } else {
                "Idle".to_string()
            };
            let snapshot_line = if let Some(s) = &state.coordinator_snapshot {
                format!(
                    "Tasks: total={} todo={} active={} blocked={} merged={}",
                    s.total, s.todo, s.active, s.blocked, s.merged
                )
            } else {
                "Tasks: unavailable".to_string()
            };
            let refresh_line = state
                .coordinator_last_refresh
                .map(|ts| format!("Last refresh: {}s ago", ts.elapsed().as_secs()))
                .unwrap_or_else(|| "Last refresh: n/a".to_string());
            let result_line = state
                .coordinator_last_result
                .clone()
                .unwrap_or_else(|| "Last result: n/a".to_string());
            let runtime = format!(
                "Coordinator runtime\n\n{}\n{}\n{}\n\n{}\n\nActions:\n- r: run full cycle\n- y: sync registry\n- c: reconcile\n- k: stop (graceful)\n- l: refresh status",
                status_line, snapshot_line, refresh_line, result_line
            );
            let runtime_para = wrapped_paragraph(runtime, "Live Runtime");
            f.render_widget(runtime_para, right_chunks[1]);
        }
        Screen::Tools => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
                .split(chunks[1]);

            let enabled_tools = state
                .working_copy
                .as_ref()
                .map(|c| c.tools.enabled.clone())
                .unwrap_or_default();

            let mut list_state = ListState::default();
            let visible = state.filtered_tool_indices();
            if !visible.is_empty() {
                let idx = if visible.contains(&state.selected_tool_index) {
                    state.selected_tool_index
                } else {
                    visible[0]
                };
                let selected_pos = visible.iter().position(|v| *v == idx).unwrap_or(0);
                list_state.select(Some(selected_pos));
            }

            let items: Vec<ListItem> = state
                .filtered_tool_indices()
                .iter()
                .map(|index| {
                    let tool = &state.tool_descriptors[*index];
                    let is_enabled = enabled_tools.contains(&tool.id.to_string());
                    let enabled_marker = if is_enabled { "[x]" } else { "[ ]" };
                    let status = state
                        .tool_checks
                        .iter()
                        .find(|tc| tc.tool_id.as_deref() == Some(tool.id.as_str()))
                        .map(|tc| tc.status.clone())
                        .unwrap_or(macc_core::doctor::ToolStatus::Missing);
                    let (status_label, status_color) = match status {
                        macc_core::doctor::ToolStatus::Installed => ("installed", theme.good),
                        macc_core::doctor::ToolStatus::Missing => ("missing", theme.warn),
                        macc_core::doctor::ToolStatus::Error(_) => ("error", theme.bad),
                    };
                    let install_hint = if matches!(status, macc_core::doctor::ToolStatus::Missing)
                        && tool.install.is_some()
                    {
                        " install"
                    } else {
                        ""
                    };

                    ListItem::new(Line::from(vec![
                        Span::raw(enabled_marker),
                        Span::raw(" "),
                        Span::styled(
                            format!("{:<9}", status_label),
                            Style::default().fg(status_color),
                        ),
                        Span::raw(" "),
                        Span::raw(format!("{}{}", tool.title, install_hint)),
                    ]))
                })
                .collect();

            let title = format!(
                "Tools ({}/{}, shown {})",
                enabled_tools.len(),
                state.tool_descriptors.len(),
                visible.len()
            );
            let list = List::new(items)
                .block(panel(&title))
                .highlight_symbol("› ")
                .highlight_style(Style::default().bg(theme.highlight_bg));
            f.render_stateful_widget(list, body_chunks[0], &mut list_state);

            if visible.is_empty() {
                f.render_widget(
                    Paragraph::new("No matching tools. Press '/' to edit search.")
                        .block(Block::default().borders(Borders::ALL)),
                    body_chunks[1],
                );
            } else {
                let selected = if visible.contains(&state.selected_tool_index) {
                    state.selected_tool_index
                } else {
                    visible[0]
                };
                let tool = &state.tool_descriptors[selected];
                let status = state
                    .tool_checks
                    .iter()
                    .find(|tc| tc.tool_id.as_deref() == Some(tool.id.as_str()))
                    .map(|tc| tc.status.clone())
                    .unwrap_or(macc_core::doctor::ToolStatus::Missing);
                let status_label = match &status {
                    macc_core::doctor::ToolStatus::Installed => "installed",
                    macc_core::doctor::ToolStatus::Missing => "missing",
                    macc_core::doctor::ToolStatus::Error(_) => "error",
                };
                let mut detail = format!(
                    "ID: {}\nStatus: {}\nFields: {}\n\nDescription:\n{}\n",
                    tool.id,
                    status_label,
                    tool.fields.len(),
                    tool.description
                );
                if let Some(install) = &tool.install {
                    detail.push_str("\nInstall:\n");
                    detail.push_str(&install.confirm_message);
                }
                if let macc_core::doctor::ToolStatus::Error(msg) = status {
                    detail.push_str("\nError:\n");
                    detail.push_str(&msg);
                }
                detail.push_str("\n\nShortcuts:\nSpace - Toggle\nEnter - Configure\n'i' - Install missing tool\n'd' - Refresh checks");
                if state.is_tool_install_confirmation_open() {
                    detail.push_str(
                        "\n\nInstall confirmation pending: press 'y' to install, 'n' to cancel.",
                    );
                }

                let detail_para = Paragraph::new(detail)
                    .block(panel("Details"))
                    .wrap(Wrap { trim: false });
                f.render_widget(detail_para, body_chunks[1]);
            }
        }
        Screen::ToolSettings => {
            let body_chunks = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
                .split(chunks[1]);

            if let Some(desc) = state.current_tool_descriptor() {
                let mut list_state = ListState::default();
                if !desc.fields.is_empty() {
                    let idx = state.tool_field_index.min(desc.fields.len() - 1);
                    list_state.select(Some(idx));
                }

                let items: Vec<ListItem> = desc
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let is_selected = i == state.tool_field_index;
                        let value = if is_selected && state.is_tool_field_editing() {
                            format!("{}_", state.tool_field_input)
                        } else {
                            state.tool_field_display_value(field)
                        };
                        ListItem::new(Line::from(vec![
                            Span::raw(format!("{:<22}", field.label)),
                            Span::raw(" "),
                            Span::raw(value),
                        ]))
                    })
                    .collect();

                let settings_title = format!("{} Settings", desc.title);
                let list = List::new(items)
                    .block(panel(&settings_title))
                    .highlight_symbol("› ")
                    .highlight_style(Style::default().bg(theme.highlight_bg));
                f.render_stateful_widget(list, body_chunks[0], &mut list_state);

                let mut detail = String::new();
                if let Some(field) = state.current_tool_field() {
                    detail.push_str(&format!("Field: {}\n", field.id));
                    detail.push_str(&format!("Pointer: {}\n", field.path));
                    detail.push_str(&format!("Kind: {}\n", field_kind_label(&field.kind)));
                    if let Some(default) = field_default_label(&field.default) {
                        detail.push_str(&format!("Default: {}\n", default));
                    }
                    if let FieldKind::Enum(options) = &field.kind {
                        detail.push_str(&format!("Options: {}\n", options.join(", ")));
                    }
                    detail.push_str("\nHelp:\n");
                    detail.push_str(&field.help);
                } else {
                    detail.push_str("No field selected.");
                }
                detail.push_str("\n\nShortcuts:\nSpace/Enter - Edit\nEsc - Cancel edit");

                if let Some(validation) = state.current_tool_field_validation() {
                    detail.push_str("\n\nValidation:\n");
                    detail.push_str(&validation);
                }
                let detail_para = wrapped_paragraph(detail, "Field Info");
                f.render_widget(detail_para, body_chunks[1]);
            } else {
                let body = wrapped_paragraph("No tool selected. Return to Tools.", "Tool Settings");
                f.render_widget(body, chunks[1]);
            }
        }
        Screen::About => {
            let body = wrapped_paragraph(
                "About MACC\n\nThis is the v0.2 prototype.\n\nUse Backspace or Esc to go back.",
                "About",
            );
            f.render_widget(body, chunks[1]);
        }
    }

    // Footer
    let footer_bindings = current_screen.help_keybindings();
    let help_text = compact_help_line(footer_bindings, chunks[2].width.saturating_sub(20) as usize);
    let badges = state.status_badges().join(" | ");
    let search = if state.search_editing {
        format!("search> {}_", state.search_query)
    } else if !state.search_query.is_empty() {
        format!("search: {}", state.search_query)
    } else {
        "search: (off)".to_string()
    };
    let footer = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Path: ", Style::default().fg(theme.muted)),
            Span::raw(ui::truncate_middle(
                &state.breadcrumbs(),
                chunks[2].width.saturating_sub(8) as usize,
            )),
        ]),
        Line::from(vec![
            Span::styled("State: ", Style::default().fg(theme.muted)),
            Span::raw(ui::truncate_middle(
                &format!("{} | {}", badges, search),
                chunks[2].width.saturating_sub(9) as usize,
            )),
        ]),
        Line::from(vec![
            Span::styled("Hints: ", Style::default().fg(theme.muted)),
            Span::raw(help_text),
            Span::raw("  "),
            Span::styled("Press ?", Style::default().fg(theme.accent)),
            Span::raw(" for help"),
        ]),
    ])
    .block(panel("Navigation"));
    f.render_widget(footer, chunks[2]);

    if state.help_open {
        render_help_overlay(f, state);
    }
}

fn kind_label(kind: PlannedOpKind) -> &'static str {
    match kind {
        PlannedOpKind::Write => "write",
        PlannedOpKind::Merge => "merge",
        PlannedOpKind::Delete => "delete",
        PlannedOpKind::Mkdir => "mkdir",
        PlannedOpKind::Other => "other",
    }
}

fn field_kind_label(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Bool => "bool".to_string(),
        FieldKind::Enum(options) => format!("enum ({} options)", options.len()),
        FieldKind::Text => "text".to_string(),
        FieldKind::Number => "number".to_string(),
        FieldKind::Array => "array".to_string(),
        FieldKind::Action(_) => "action".to_string(),
    }
}

fn field_default_label(default: &Option<FieldDefault>) -> Option<String> {
    match default {
        Some(FieldDefault::Bool(value)) => Some(value.to_string()),
        Some(FieldDefault::Text(value)) => Some(value.clone()),
        Some(FieldDefault::Enum(value)) => Some(value.clone()),
        Some(FieldDefault::Number(value)) => Some(value.to_string()),
        Some(FieldDefault::Array(values)) => Some(values.join(", ")),
        None => None,
    }
}

fn scope_label(scope: Scope) -> &'static str {
    match scope {
        Scope::Project => "project",
        Scope::User => "user",
    }
}

fn render_help_overlay(f: &mut Frame, state: &AppState) {
    let area = ui::centered_rect(60, 60, f.size());
    f.render_widget(Clear, area); // Clear the background

    let current_screen = state.current_screen();
    let help_items = current_screen.help_keybindings();

    let mut text = format!("Help: {}\n\n", current_screen.title());
    for (key, desc) in help_items {
        text.push_str(&format!("{:<15} : {}\n", key, desc));
    }

    let help_para = Paragraph::new(text)
        .block(
            Block::default()
                .title("Keybindings")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: true });

    f.render_widget(help_para, area);
}
