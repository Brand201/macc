use crate::catalog::{
    McpCatalog, McpEntry, Selector, SkillEntry, SkillsCatalog, Source, SourceKind,
};
use crate::engine::Engine;
use crate::service::interaction::InteractionHandler;
use crate::{MaccError, ProjectPaths, Result};

pub trait CatalogUi: InteractionHandler {}
impl<T: InteractionHandler + ?Sized> CatalogUi for T {}

#[derive(Debug, Clone)]
pub struct NormalizedGitInput {
    pub clone_url: String,
    pub reference: String,
    pub subpath: String,
}

pub trait CatalogUrlParser {
    fn normalize_git_input(&self, value: &str) -> Option<NormalizedGitInput>;
    fn validate_http_url(&self, value: &str) -> bool;
}

pub fn run_remote_search(
    engine: &(impl Engine + ?Sized),
    paths: &ProjectPaths,
    provider: &dyn crate::catalog::service::CatalogRemoteSearchProvider,
    api: &str,
    kind: &str,
    query: &str,
    add: bool,
    add_ids: Option<&str>,
    ui: &dyn CatalogUi,
) -> Result<()> {
    let search_kind = crate::catalog::service::parse_search_kind(kind)?;
    ui.info(&format!("Searching {} for '{}' in {}...", kind, query, api));
    let outcome =
        engine.catalog_search_remote(paths, provider, api, search_kind, query, add, add_ids)?;

    if outcome.rows.is_empty() {
        match outcome.kind {
            crate::catalog::service::CatalogSearchKind::Skill => ui.info("No skills found."),
            crate::catalog::service::CatalogSearchKind::Mcp => ui.info("No MCP servers found."),
        }
        return Ok(());
    }

    ui.info(&format!(
        "{:<20} {:<30} {:<10} {:<20}",
        "ID", "NAME", "KIND", "TAGS"
    ));
    ui.info(&format!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", ""));
    for row in &outcome.rows {
        ui.info(&format!(
            "{:<20} {:<30} {:<10} {:<20}",
            row.id, row.name, row.kind, row.tags
        ));
        if row.queued {
            ui.info(&format!("  [+] Queued import for '{}'", row.id));
        }
    }

    if outcome.imported > 0 {
        match outcome.kind {
            crate::catalog::service::CatalogSearchKind::Skill => {
                ui.info("Saved changes to skills catalog.")
            }
            crate::catalog::service::CatalogSearchKind::Mcp => {
                ui.info("Saved changes to MCP catalog.")
            }
        }
    }
    Ok(())
}

pub fn list_skills(catalog: &SkillsCatalog, ui: &dyn CatalogUi) {
    if catalog.entries.is_empty() {
        ui.info("No skills found in catalog.");
        return;
    }
    ui.info(&format!(
        "{:<20} {:<30} {:<10} {:<20}",
        "ID", "NAME", "KIND", "TAGS"
    ));
    ui.info(&format!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", ""));
    for entry in &catalog.entries {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        ui.info(&format!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        ));
    }
}

pub fn search_skills(catalog: &SkillsCatalog, query: &str, ui: &dyn CatalogUi) {
    let filtered = crate::catalog::service::filter_skills(catalog, query);
    if filtered.is_empty() {
        ui.info(&format!("No skills matching '{}' found.", query));
        return;
    }
    ui.info(&format!(
        "{:<20} {:<30} {:<10} {:<20}",
        "ID", "NAME", "KIND", "TAGS"
    ));
    ui.info(&format!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", ""));
    for entry in &filtered {
        let tags = entry.tags.join(", ");
        let kind = crate::catalog::service::source_kind_label(&entry.source.kind);
        ui.info(&format!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        ));
    }
}

#[allow(clippy::too_many_arguments)]
pub fn add_skill(
    paths: &ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
    ui: &dyn CatalogUi,
) -> Result<()> {
    let entry =
        crate::catalog::service::build_skill_entry(crate::catalog::service::CatalogEntryInput {
            id: id.clone(),
            name,
            description,
            tags_csv: tags,
            subpath,
            kind,
            url,
            reference,
            checksum,
        })?;
    crate::catalog::service::upsert_skill(paths, catalog, entry)?;
    ui.info(&format!("Skill '{}' upserted successfully.", id));
    Ok(())
}

pub fn remove_skill(
    paths: &ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
    ui: &dyn CatalogUi,
) -> Result<()> {
    if crate::catalog::service::remove_skill(paths, catalog, &id)? {
        ui.info(&format!("Skill '{}' removed successfully.", id));
    } else {
        ui.info(&format!("Skill '{}' not found in catalog.", id));
    }
    Ok(())
}

pub fn list_mcp(catalog: &McpCatalog, ui: &dyn CatalogUi) {
    if catalog.entries.is_empty() {
        ui.info("No MCP servers found in catalog.");
        return;
    }
    ui.info(&format!(
        "{:<20} {:<30} {:<10} {:<20}",
        "ID", "NAME", "KIND", "TAGS"
    ));
    ui.info(&format!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", ""));
    for entry in &catalog.entries {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        ui.info(&format!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        ));
    }
}

pub fn search_mcp(catalog: &McpCatalog, query: &str, ui: &dyn CatalogUi) {
    let filtered = crate::catalog::service::filter_mcp(catalog, query);
    if filtered.is_empty() {
        ui.info(&format!("No MCP servers matching '{}' found.", query));
        return;
    }
    ui.info(&format!(
        "{:<20} {:<30} {:<10} {:<20}",
        "ID", "NAME", "KIND", "TAGS"
    ));
    ui.info(&format!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", ""));
    for entry in &filtered {
        let tags = entry.tags.join(", ");
        let kind = crate::catalog::service::source_kind_label(&entry.source.kind);
        ui.info(&format!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        ));
    }
}

#[allow(clippy::too_many_arguments)]
pub fn add_mcp(
    paths: &ProjectPaths,
    catalog: &mut McpCatalog,
    id: String,
    name: String,
    description: String,
    tags: Option<String>,
    subpath: String,
    kind: String,
    url: String,
    reference: String,
    checksum: Option<String>,
    ui: &dyn CatalogUi,
) -> Result<()> {
    let entry =
        crate::catalog::service::build_mcp_entry(crate::catalog::service::CatalogEntryInput {
            id: id.clone(),
            name,
            description,
            tags_csv: tags,
            subpath,
            kind,
            url,
            reference,
            checksum,
        })?;
    crate::catalog::service::upsert_mcp(paths, catalog, entry)?;
    ui.info(&format!("MCP server '{}' upserted successfully.", id));
    Ok(())
}

pub fn remove_mcp(
    paths: &ProjectPaths,
    catalog: &mut McpCatalog,
    id: String,
    ui: &dyn CatalogUi,
) -> Result<()> {
    if crate::catalog::service::remove_mcp(paths, catalog, &id)? {
        ui.info(&format!("MCP server '{}' removed successfully.", id));
    } else {
        ui.info(&format!("MCP server '{}' not found in catalog.", id));
    }
    Ok(())
}

pub fn install_skill(
    paths: &ProjectPaths,
    tool: &str,
    id: &str,
    engine: &(impl Engine + ?Sized),
    backend: &dyn crate::catalog::service::CatalogInstallBackend,
    ui: &dyn CatalogUi,
) -> Result<()> {
    let outcome = engine.install_skill(paths, tool, id, backend)?;
    crate::service::project::report_diagnostics(&outcome.diagnostics, ui);
    ui.info(&format!(
        "Installing skill '{}' for {}...",
        id, outcome.tool_title
    ));
    ui.info(&outcome.report.render_cli());
    Ok(())
}

pub fn install_mcp(
    paths: &ProjectPaths,
    id: &str,
    engine: &(impl Engine + ?Sized),
    backend: &dyn crate::catalog::service::CatalogInstallBackend,
    ui: &dyn CatalogUi,
) -> Result<()> {
    let outcome = engine.install_mcp(paths, id, backend)?;
    ui.info(&format!("Installing MCP server '{}'...", id));
    ui.info(&outcome.report.render_cli());
    Ok(())
}

pub fn import_url(
    paths: &ProjectPaths,
    kind: &str,
    id: String,
    url: String,
    name: Option<String>,
    description: String,
    tags: Option<String>,
    parser: &dyn CatalogUrlParser,
    ui: &dyn CatalogUi,
) -> Result<()> {
    let (source_kind, clone_or_url, reference, subpath) =
        if let Some(normalized) = parser.normalize_git_input(&url) {
            (
                SourceKind::Git,
                normalized.clone_url,
                normalized.reference,
                normalized.subpath,
            )
        } else if parser.validate_http_url(&url) {
            (
                SourceKind::Http,
                url.trim().to_string(),
                String::new(),
                String::new(),
            )
        } else {
            return Err(MaccError::Validation(format!(
                "Unsupported URL format: '{}'. Use a git URL/path or http(s) archive URL.",
                url
            )));
        };

    let entry_name = name.unwrap_or_else(|| id.clone());
    let source = Source {
        kind: source_kind,
        url: clone_or_url,
        reference,
        checksum: None,
        subpaths: if subpath.is_empty() {
            vec![]
        } else {
            vec![subpath]
        },
    };

    match kind {
        "skill" => {
            let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
            let mut selector = Selector {
                subpath: "".to_string(),
            };
            if let Some(first) = source.subpaths.first() {
                selector.subpath = first.clone();
            }
            catalog.upsert_skill_entry(SkillEntry {
                id: id.clone(),
                name: entry_name,
                description,
                tags: tags.map_or_else(Vec::new, |t| parse_tags(&t)),
                source,
                selector,
            });
            catalog.save_atomically(paths, &paths.skills_catalog_path())?;
            ui.info(&format!(
                "Imported skill '{}' to {}",
                id,
                paths.skills_catalog_path().display()
            ));
        }
        "mcp" => {
            let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
            let mut selector = Selector {
                subpath: "".to_string(),
            };
            if let Some(first) = source.subpaths.first() {
                selector.subpath = first.clone();
            }
            catalog.upsert_mcp_entry(McpEntry {
                id: id.clone(),
                name: entry_name,
                description,
                tags: tags.map_or_else(Vec::new, |t| parse_tags(&t)),
                source,
                selector,
            });
            catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
            ui.info(&format!(
                "Imported MCP '{}' to {}",
                id,
                paths.mcp_catalog_path().display()
            ));
        }
        _ => {
            return Err(MaccError::Validation(format!(
                "Unknown catalog kind '{}'. Use 'skill' or 'mcp'.",
                kind
            )));
        }
    }
    Ok(())
}

fn parse_tags(csv: &str) -> Vec<String> {
    csv.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}
