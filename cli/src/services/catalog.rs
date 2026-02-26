use macc_adapter_shared::catalog::{remote_search, SearchKind as RemoteSearchKind};
use macc_core::catalog::{
    load_effective_mcp_catalog, load_effective_skills_catalog, McpCatalog, McpEntry, Selector,
    SkillEntry, SkillsCatalog, Source, SourceKind,
};
use macc_core::engine::Engine;
use macc_core::plan::builders::{plan_mcp_install, plan_skill_install};
use macc_core::plan::ActionPlan;
use macc_core::resolve::{FetchUnit, Selection, SelectionKind};
use macc_core::{MaccError, Result};

pub fn run_remote_search(
    paths: &macc_core::ProjectPaths,
    api: String,
    kind: String,
    q: String,
    add: bool,
    add_ids: Option<String>,
) -> Result<()> {
    let search_kind = match kind.as_str() {
        "skill" => RemoteSearchKind::Skill,
        "mcp" => RemoteSearchKind::Mcp,
        _ => {
            return Err(MaccError::Validation(format!(
                "Invalid kind: {}. Must be 'skill' or 'mcp'.",
                kind
            )))
        }
    };

    println!("Searching {} for '{}' in {}...", kind, q, api);

    let whitelist: Option<Vec<String>> = add_ids
        .as_ref()
        .map(|s| s.split(',').map(|i| i.trim().to_string()).collect());
    let should_save = add || whitelist.is_some();

    match search_kind {
        RemoteSearchKind::Skill => {
            let results: Vec<SkillEntry> = remote_search(&api, search_kind, &q)?;
            if results.is_empty() {
                println!("No skills found.");
                return Ok(());
            }

            println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
            println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");

            let mut catalog = if should_save {
                Some(SkillsCatalog::load(&paths.skills_catalog_path())?)
            } else {
                None
            };

            for entry in &results {
                let tags = entry.tags.join(", ");
                let kind_str = match entry.source.kind {
                    SourceKind::Git => "git",
                    SourceKind::Http => "http",
                    SourceKind::Local => "local",
                };
                println!(
                    "{:<20} {:<30} {:<10} {:<20}",
                    entry.id, entry.name, kind_str, tags
                );

                if let Some(cat) = &mut catalog {
                    let should_add = if add {
                        true
                    } else if let Some(wl) = &whitelist {
                        wl.contains(&entry.id)
                    } else {
                        false
                    };
                    if should_add {
                        cat.upsert_skill_entry(entry.clone());
                        println!("  [+] Queued import for '{}'", entry.id);
                    }
                }
            }

            if let Some(cat) = catalog {
                cat.save_atomically(paths, &paths.skills_catalog_path())?;
                println!("Saved changes to skills catalog.");
            }
        }
        RemoteSearchKind::Mcp => {
            let results: Vec<McpEntry> = remote_search(&api, search_kind, &q)?;
            if results.is_empty() {
                println!("No MCP servers found.");
                return Ok(());
            }

            println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
            println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");

            let mut catalog = if should_save {
                Some(McpCatalog::load(&paths.mcp_catalog_path())?)
            } else {
                None
            };

            for entry in &results {
                let tags = entry.tags.join(", ");
                let kind_str = match entry.source.kind {
                    SourceKind::Git => "git",
                    SourceKind::Http => "http",
                    SourceKind::Local => "local",
                };
                println!(
                    "{:<20} {:<30} {:<10} {:<20}",
                    entry.id, entry.name, kind_str, tags
                );

                if let Some(cat) = &mut catalog {
                    let should_add = if add {
                        true
                    } else if let Some(wl) = &whitelist {
                        wl.contains(&entry.id)
                    } else {
                        false
                    };
                    if should_add {
                        cat.upsert_mcp_entry(entry.clone());
                        println!("  [+] Queued import for '{}'", entry.id);
                    }
                }
            }

            if let Some(cat) = catalog {
                cat.save_atomically(paths, &paths.mcp_catalog_path())?;
                println!("Saved changes to MCP catalog.");
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
    let query = query.to_lowercase();
    let filtered: Vec<_> = catalog
        .entries
        .iter()
        .filter(|e| {
            e.id.to_lowercase().contains(&query)
                || e.name.to_lowercase().contains(&query)
                || e.description.to_lowercase().contains(&query)
                || e.tags.iter().any(|t| t.to_lowercase().contains(&query))
        })
        .collect();
    if filtered.is_empty() {
        println!("No skills matching '{}' found.", query);
        return;
    }
    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in filtered {
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
    let tags = tags
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    let source_kind = parse_source_kind(&kind)?;
    let entry = SkillEntry {
        id: id.clone(),
        name,
        description,
        tags,
        selector: Selector { subpath },
        source: Source {
            kind: source_kind,
            url,
            reference,
            checksum,
            subpaths: vec![],
        },
    };
    catalog.upsert_skill_entry(entry);
    catalog.save_atomically(paths, &paths.skills_catalog_path())?;
    println!("Skill '{}' upserted successfully.", id);
    Ok(())
}

pub fn remove_skill(
    paths: &macc_core::ProjectPaths,
    catalog: &mut SkillsCatalog,
    id: String,
) -> Result<()> {
    if macc_core::is_required_skill(&id) {
        return Err(MaccError::Validation(format!(
            "cannot disable required skill '{}'",
            id
        )));
    }
    if catalog.delete_skill_entry(&id) {
        catalog.save_atomically(paths, &paths.skills_catalog_path())?;
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
    let query = query.to_lowercase();
    let filtered: Vec<_> = catalog
        .entries
        .iter()
        .filter(|e| {
            e.id.to_lowercase().contains(&query)
                || e.name.to_lowercase().contains(&query)
                || e.description.to_lowercase().contains(&query)
                || e.tags.iter().any(|t| t.to_lowercase().contains(&query))
        })
        .collect();
    if filtered.is_empty() {
        println!("No MCP servers matching '{}' found.", query);
        return;
    }
    println!("{:<20} {:<30} {:<10} {:<20}", "ID", "NAME", "KIND", "TAGS");
    println!("{:-<20} {:-<30} {:-<10} {:-<20}", "", "", "", "");
    for entry in filtered {
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
    let tags = tags
        .map(|t| t.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    let source_kind = parse_source_kind(&kind)?;
    let entry = McpEntry {
        id: id.clone(),
        name,
        description,
        tags,
        selector: Selector { subpath },
        source: Source {
            kind: source_kind,
            url,
            reference,
            checksum,
            subpaths: vec![],
        },
    };
    catalog.upsert_mcp_entry(entry);
    catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
    println!("MCP server '{}' upserted successfully.", id);
    Ok(())
}

pub fn remove_mcp(paths: &macc_core::ProjectPaths, catalog: &mut McpCatalog, id: String) -> Result<()> {
    if catalog.delete_mcp_entry(&id) {
        catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
        println!("MCP server '{}' removed successfully.", id);
    } else {
        println!("MCP server '{}' not found in catalog.", id);
    }
    Ok(())
}

pub fn install_skill<E: Engine>(
    paths: &macc_core::ProjectPaths,
    tool: &str,
    id: &str,
    engine: &E,
) -> Result<()> {
    let catalog = load_effective_skills_catalog(paths)?;
    let entry = catalog
        .entries
        .iter()
        .find(|e| e.id == id)
        .ok_or_else(|| MaccError::Validation(format!("Skill '{}' not found in catalog.", id)))?;

    let (descriptors, diagnostics) = engine.list_tools(paths);
    crate::services::project::report_diagnostics(&diagnostics);
    let tool_title = descriptors
        .iter()
        .find(|d| d.id == tool)
        .map(|d| d.title.as_str())
        .unwrap_or(tool);
    println!("Installing skill '{}' for {}...", id, tool_title);

    let mut source = entry.source.clone();
    if !entry.selector.subpath.is_empty() && entry.selector.subpath != "." {
        source.subpaths = vec![entry.selector.subpath.clone()];
    }

    let fetch_unit = FetchUnit {
        source,
        selections: vec![Selection {
            id: entry.id.clone(),
            subpath: entry.selector.subpath.clone(),
            kind: SelectionKind::Skill,
        }],
    };
    let materialized = macc_adapter_shared::fetch::materialize_fetch_unit(paths, fetch_unit)?;
    let mut plan = ActionPlan::new();
    plan_skill_install(
        &mut plan,
        tool,
        id,
        &materialized.source_root_path,
        &entry.selector.subpath,
    )
    .map_err(MaccError::Validation)?;
    let report = engine.apply(paths, &mut plan, false)?;
    println!("{}", report.render_cli());
    Ok(())
}

pub fn install_mcp<E: Engine>(paths: &macc_core::ProjectPaths, id: &str, engine: &E) -> Result<()> {
    let catalog = load_effective_mcp_catalog(paths)?;
    let entry = catalog
        .entries
        .iter()
        .find(|e| e.id == id)
        .ok_or_else(|| MaccError::Validation(format!("MCP server '{}' not found in catalog.", id)))?;
    println!("Installing MCP server '{}'...", id);

    let mut source = entry.source.clone();
    if !entry.selector.subpath.is_empty() && entry.selector.subpath != "." {
        source.subpaths = vec![entry.selector.subpath.clone()];
    }
    let fetch_unit = FetchUnit {
        source,
        selections: vec![Selection {
            id: entry.id.clone(),
            subpath: entry.selector.subpath.clone(),
            kind: SelectionKind::Mcp,
        }],
    };
    let materialized = macc_adapter_shared::fetch::materialize_fetch_unit(paths, fetch_unit)?;
    let mut plan = ActionPlan::new();
    plan_mcp_install(
        &mut plan,
        id,
        &materialized.source_root_path,
        &entry.selector.subpath,
    )
    .map_err(MaccError::Validation)?;
    let report = engine.apply(paths, &mut plan, false)?;
    println!("{}", report.render_cli());
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

fn parse_source_kind(kind: &str) -> Result<SourceKind> {
    match kind.to_lowercase().as_str() {
        "git" => Ok(SourceKind::Git),
        "http" => Ok(SourceKind::Http),
        "local" => Ok(SourceKind::Local),
        _ => Err(MaccError::Validation(format!(
            "Invalid source kind: {}. Must be 'git', 'http', or 'local'.",
            kind
        ))),
    }
}
