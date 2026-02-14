#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Screen {
    Home,
    Tools,
    Automation,
    Mcp,
    Logs,
    Skills,
    Agents,
    ToolSettings,
    Preview,
    Apply,
    About,
}

impl Screen {
    pub fn title(&self) -> &'static str {
        match self {
            Screen::Home => "Home",
            Screen::Tools => "Tools Configuration",
            Screen::Automation => "Automation / Coordinator",
            Screen::Mcp => "MCP Servers",
            Screen::Logs => "Logs",
            Screen::Skills => "Skills",
            Screen::Agents => "Agents",
            Screen::ToolSettings => "Tool Settings",
            Screen::Preview => "Preview",
            Screen::Apply => "Apply Changes",
            Screen::About => "About",
        }
    }

    pub fn help_keybindings(&self) -> Vec<(&'static str, &'static str)> {
        let mut bindings = vec![
            ("?", "Toggle Help"),
            ("q/Esc", "Back / Quit"),
            ("h", "Go Home"),
            ("t", "Go to Tools"),
            ("o", "Go to Automation"),
            ("m", "Go to MCP"),
            ("g", "Go to Logs"),
            ("p", "Go to Preview"),
            ("x", "Go to Apply"),
            ("s", "Save Config"),
            ("/", "Search / Filter"),
            ("u / U", "Undo / Redo config"),
        ];

        let screen_bindings = match self {
            Screen::Tools => vec![
                ("Up/Down", "Navigate Tools"),
                ("Space", "Toggle Tool"),
                ("Enter", "Configure Tool"),
                ("d", "Refresh Tool Checks"),
                ("f", "Generate Tool Context"),
            ],
            Screen::Automation => vec![
                ("Up/Down", "Navigate Settings"),
                ("Space/Enter", "Edit / Cycle"),
                ("r", "Run Full Cycle"),
                ("y", "Sync Registry"),
                ("c", "Reconcile"),
                ("k", "Stop Coordinator"),
                ("l", "Refresh Live Status"),
            ],
            Screen::ToolSettings => vec![
                ("Up/Down", "Navigate Fields"),
                ("Space/Enter", "Cycle Value / Edit Text/Number/Array"),
            ],
            Screen::Skills => vec![
                ("Up/Down", "Navigate Skills"),
                ("Space/Enter", "Toggle Skill"),
                ("a", "Select All"),
                ("n", "Select None"),
            ],
            Screen::Agents => vec![
                ("Up/Down", "Navigate Agents"),
                ("Space/Enter", "Toggle Agent"),
                ("a", "Select All"),
                ("n", "Select None"),
            ],
            Screen::Mcp => vec![
                ("Up/Down", "Navigate MCP Templates"),
                ("Space/Enter", "Toggle Template"),
                ("a", "Select All"),
                ("n", "Select None"),
            ],
            Screen::Logs => vec![
                ("Up/Down", "Select Log File"),
                ("PgUp/PgDn", "Scroll Log Content"),
                ("r", "Refresh Log List"),
                ("/", "Filter logs"),
            ],
            Screen::Preview => vec![
                ("Up/Down", "Navigate Operations"),
                ("PgUp/PgDn", "Scroll Diff"),
                ("r", "Refresh Plan"),
                ("x", "Go to Apply Screen"),
            ],
            Screen::Apply => vec![
                ("Enter", "Apply Changes"),
                ("Backspace", "Delete last char of 'YES'"),
                ("YES", "Type to consent to user-scope ops"),
            ],
            _ => vec![],
        };

        bindings.extend(screen_bindings);
        bindings
    }
}
