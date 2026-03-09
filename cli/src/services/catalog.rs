use macc_adapter_shared::catalog::{remote_search, SearchKind as RemoteSearchKind};
use macc_core::catalog::{
    McpCatalog, McpEntry, Selector, SkillEntry, SkillsCatalog, Source, SourceKind,
};
use macc_core::{MaccError, Result};
use crate::services::engine_provider::SharedEngine;

struct CliRemoteSearchProvider;

impl macc_core::catalog::service::CatalogRemoteSearchProvider for CliRemoteSearchProvider {
    fn search_skills(&self, api: &str, query: &str) -> Result<Vec<SkillEntry>> {
        remote_search(api, RemoteSearchKind::Skill, query)
    }

    fn search_mcp(&self, api: &str, query: &str) -> Result<Vec<macc_core::catalog::McpEntry>> {
        remote_search(api, RemoteSearchKind::Mcp, query)
    }
}

struct CliCatalogInstallBackend<'a> {
    engine: &'a SharedEngine,
}

impl macc_core::catalog::service::CatalogInstallBackend for CliCatalogInstallBackend<'_> {
    fn list_tools(
        &self,
        paths: &macc_core::ProjectPaths,
    ) -> (
        Vec<macc_core::tool::ToolDescriptor>,
        Vec<macc_core::tool::ToolDiagnostic>,
    ) {
        self.engine.list_tools(paths)
    }

    fn materialize_fetch_unit(
        &self,
        paths: &macc_core::ProjectPaths,
        fetch_unit: macc_core::resolve::FetchUnit,
    ) -> Result<macc_core::resolve::MaterializedFetchUnit> {
        macc_adapter_shared::fetch::materialize_fetch_unit(paths, fetch_unit)
    }

    fn apply(
        &self,
        paths: &macc_core::ProjectPaths,
        plan: &mut macc_core::plan::ActionPlan,
        allow_user_scope: bool,
    ) -> Result<macc_core::ApplyReport> {
        self.engine.apply(paths, plan, allow_user_scope)
    }
}

pub fn run_remote_search(
    engine: &SharedEngine,
    paths: &macc_core::ProjectPaths,
    api: String,
    kind: String,
    q: String,
    add: bool,
    add_ids: Option<String>,
) -> Result<()> {
    let search_kind = macc_core::catalog::service::parse_search_kind(kind.as_str())?;

    println!("Searching {} for '{}' in {}...", kind, q, api);

    let outcome = engine.catalog_search_remote(
        paths,
        &CliRemoteSearchProvider,
        &api,
        search_kind,
        &q,
        add,
        add_ids.as_deref(),
    )?;

    if outcome.rows.is_empty() {
        match outcome.kind {
            macc_core::catalog::service::CatalogSearchKind::Skill => println!("No skills found."),
            macc_core::catalog::service::CatalogSearchKind::Mcp => println!("No MCP servers found."),
        }
        return Ok(());
    }

    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for row in &outcome.rows {
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            row.id, row.name, row.kind, row.tags
        );
        if row.queued {
            println!("  [+] Queued import for '{}'", row.id);
        }
    }

    if outcome.imported > 0 {
        match outcome.kind {
            macc_core::catalog::service::CatalogSearchKind::Skill => {
                println!("Saved changes to skills catalog.")
            }
            macc_core::catalog::service::CatalogSearchKind::Mcp => {
                println!("Saved changes to MCP catalog.")
            }
        }
    }

    Ok(())
}

pub fn list_skills(catalog: &SkillsCatalog) {
    if catalog.entries.is_empty() {
        println!("No skills found in catalog.");
        return;
    }
    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in &catalog.entries {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

pub fn search_skills(catalog: &SkillsCatalog, query: &str) {
    let filtered = macc_core::catalog::service::filter_skills(catalog, query);
    if filtered.is_empty() {
        println!("No skills matching '{}' found.", query);
        return;
    }
    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in &filtered {
        let tags = entry.tags.join(", ");
        let kind = macc_core::catalog::service::source_kind_label(&entry.source.kind);
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub fn add_skill(
    paths: &macc_core::ProjectPaths,
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
) -> Result<()> {
    let entry = macc_core::catalog::service::build_skill_entry(
        macc_core::catalog::service::CatalogEntryInput {
            id: id.clone(),
            name,
            description,
            tags_csv: tags,
            subpath,
            kind,
            url,
            reference,
            checksum,
        },
    )?;
    macc_core::catalog::service::upsert_skill(paths, catalog, entry)?;
    println!("Skill '{}' upserted successfully.", id);
    Ok(())
}

pub fn remove_skill(
    paths: &macc_core::ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
) -> Result<()> {
    if macc_core::catalog::service::remove_skill(paths, catalog, &id)? {
        println!("Skill '{}' removed successfully.", id);
    } else {
        println!("Skill '{}' not found in catalog.", id);
    }
    Ok(())
}

pub fn list_mcp(catalog: &McpCatalog) {
    if catalog.entries.is_empty() {
        println!("No MCP servers found in catalog.");
        return;
    }
    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in &catalog.entries {
        let tags = entry.tags.join(", ");
        let kind = match entry.source.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

pub fn search_mcp(catalog: &McpCatalog, query: &str) {
    let filtered = macc_core::catalog::service::filter_mcp(catalog, query);
    if filtered.is_empty() {
        println!("No MCP servers matching '{}' found.", query);
        return;
    }
    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in &filtered {
        let tags = entry.tags.join(", ");
        let kind = macc_core::catalog::service::source_kind_label(&entry.source.kind);
        println!(
            "{:<20} {:<30} {:<10} {:<20}",
            entry.id, entry.name, kind, tags
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub fn add_mcp(
    paths: &macc_core::ProjectPaths,
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
) -> Result<()> {
    let entry = macc_core::catalog::service::build_mcp_entry(
        macc_core::catalog::service::CatalogEntryInput {
            id: id.clone(),
            name,
            description,
            tags_csv: tags,
            subpath,
            kind,
            url,
            reference,
            checksum,
        },
    )?;
    macc_core::catalog::service::upsert_mcp(paths, catalog, entry)?;
    println!("MCP server '{}' upserted successfully.", id);
    Ok(())
}

pub fn remove_mcp(paths: &macc_core::ProjectPaths, catalog: &mut McpCatalog, id: String) -> Result<()> {
    if macc_core::catalog::service::remove_mcp(paths, catalog, &id)? {
        println!("MCP server '{}' removed successfully.", id);
    } else {
        println!("MCP server '{}' not found in catalog.", id);
    }
    Ok(())
}

pub fn install_skill(
    paths: &macc_core::ProjectPaths,
    tool: &str,
    id: &str,
    engine: &SharedEngine,
) -> Result<()> {
    let backend = CliCatalogInstallBackend { engine };
    let outcome = engine.install_skill(paths, tool, id, &backend)?;
    crate::services::project::report_diagnostics(&outcome.diagnostics);
    println!("Installing skill '{}' for {}...", id, outcome.tool_title);
    println!("{}", outcome.report.render_cli());
    Ok(())
}

pub fn install_mcp(
    paths: &macc_core::ProjectPaths,
    id: &str,
    engine: &SharedEngine,
) -> Result<()> {
    let backend = CliCatalogInstallBackend { engine };
    let outcome = engine.install_mcp(paths, id, &backend)?;
    println!("Installing MCP server '{}'...", id);
    println!("{}", outcome.report.render_cli());
    Ok(())
}

pub fn import_url(
    paths: &macc_core::ProjectPaths,
    kind: &str,
    id: String,
    url: String,
    name: Option<String>,
    description: String,
    tags: Option<String>,
) -> Result<()> {
    let (source_kind, clone_or_url, reference, subpath) =
        if let Some(normalized) = macc_adapter_shared::url_parsing::normalize_git_input(&url) {
            (
                SourceKind::Git,
                normalized.clone_url,
                normalized.reference,
                normalized.subpath,
            )
        } else if macc_adapter_shared::url_parsing::validate_http_url(&url) {
            (SourceKind::Http, url.trim().to_string(), String::new(), String::new())
        } else {
            return Err(MaccError::Validation(format!(
                "Invalid or unsupported URL: {}",
                url
            )));
        };

    let name = name.unwrap_or_else(|| id.clone());
    let tags_vec = tags
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();

    let selector = Selector {
        subpath: subpath.clone(),
    };
    let source = Source {
        kind: source_kind,
        url: clone_or_url,
        reference,
        checksum: None,
        subpaths: vec![subpath.clone()],
    };

    match kind {
        "skill" => {
            let mut catalog = SkillsCatalog::load(&paths.skills_catalog_path())?;
            let entry = SkillEntry {
                id: id.clone(),
                name,
                description,
                tags: tags_vec,
                selector,
                source,
            };
            catalog.upsert_skill_entry(entry);
            catalog.save_atomically(paths, &paths.skills_catalog_path())?;
            println!("Imported skill '{}' from URL into catalog.", id);
        }
        "mcp" => {
            let mut catalog = McpCatalog::load(&paths.mcp_catalog_path())?;
            let entry = McpEntry {
                id: id.clone(),
                name,
                description,
                tags: tags_vec,
                selector,
                source,
            };
            catalog.upsert_mcp_entry(entry);
            catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
            println!("Imported MCP server '{}' from URL into catalog.", id);
        }
        _ => {
            return Err(MaccError::Validation(format!(
                "Invalid kind: {}. Must be 'skill' or 'mcp'.",
                kind
            )))
        }
    }
    Ok(())
}
