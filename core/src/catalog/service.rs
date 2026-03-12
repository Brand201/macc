use crate::catalog::{
    load_effective_mcp_catalog, load_effective_skills_catalog, McpCatalog, McpEntry, SkillEntry,
    SkillsCatalog, SourceKind,
};
use crate::plan::builders::{plan_mcp_install, plan_skill_install};
use crate::plan::ActionPlan;
use crate::resolve::{FetchUnit, MaterializedFetchUnit, Selection, SelectionKind};
use crate::tool::{ToolDescriptor, ToolDiagnostic};
use crate::{ApplyReport, MaccError, ProjectPaths, Result};

pub use crate::domain::catalog::CatalogEntryInput;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogSearchKind {
    Skill,
    Mcp,
}

impl CatalogSearchKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skill => "skill",
            Self::Mcp => "mcp",
        }
    }
}

pub fn parse_search_kind(kind: &str) -> Result<CatalogSearchKind> {
    match kind {
        "skill" => Ok(CatalogSearchKind::Skill),
        "mcp" => Ok(CatalogSearchKind::Mcp),
        _ => Err(MaccError::Validation(format!(
            "Invalid kind: {}. Must be 'skill' or 'mcp'.",
            kind
        ))),
    }
}

pub trait CatalogRemoteSearchProvider {
    fn search_skills(&self, api: &str, query: &str) -> Result<Vec<SkillEntry>>;
    fn search_mcp(&self, api: &str, query: &str) -> Result<Vec<McpEntry>>;
}

#[derive(Debug, Clone)]
pub struct CatalogRow {
    pub id: String,
    pub name: String,
    pub kind: &'static str,
    pub tags: String,
    pub queued: bool,
}

#[derive(Debug, Clone)]
pub struct RemoteSearchOutcome {
    pub kind: CatalogSearchKind,
    pub rows: Vec<CatalogRow>,
    pub imported: usize,
}

pub fn execute_remote_search(
    paths: &ProjectPaths,
    provider: &dyn CatalogRemoteSearchProvider,
    api: &str,
    kind: CatalogSearchKind,
    query: &str,
    add: bool,
    add_ids: Option<&str>,
) -> Result<RemoteSearchOutcome> {
    let whitelist: Option<Vec<String>> =
        add_ids.map(|s| s.split(',').map(|i| i.trim().to_string()).collect());
    let should_save = add || whitelist.is_some();

    match kind {
        CatalogSearchKind::Skill => {
            let results = provider.search_skills(api, query)?;
            let mut rows = Vec::with_capacity(results.len());
            let mut imported = 0usize;
            let mut catalog = if should_save {
                Some(SkillsCatalog::load(&paths.skills_catalog_path())?)
            } else {
                None
            };

            for entry in results {
                let queued = if let Some(wl) = &whitelist {
                    add || wl.contains(&entry.id)
                } else {
                    add
                };
                if queued {
                    if let Some(cat) = &mut catalog {
                        cat.upsert_skill_entry(entry.clone());
                        imported += 1;
                    }
                }
                rows.push(CatalogRow {
                    id: entry.id,
                    name: entry.name,
                    kind: source_kind_label(&entry.source.kind),
                    tags: entry.tags.join(", "),
                    queued,
                });
            }

            if let Some(cat) = catalog {
                cat.save_atomically(paths, &paths.skills_catalog_path())?;
            }

            Ok(RemoteSearchOutcome {
                kind,
                rows,
                imported,
            })
        }
        CatalogSearchKind::Mcp => {
            let results = provider.search_mcp(api, query)?;
            let mut rows = Vec::with_capacity(results.len());
            let mut imported = 0usize;
            let mut catalog = if should_save {
                Some(McpCatalog::load(&paths.mcp_catalog_path())?)
            } else {
                None
            };

            for entry in results {
                let queued = if let Some(wl) = &whitelist {
                    add || wl.contains(&entry.id)
                } else {
                    add
                };
                if queued {
                    if let Some(cat) = &mut catalog {
                        cat.upsert_mcp_entry(entry.clone());
                        imported += 1;
                    }
                }
                rows.push(CatalogRow {
                    id: entry.id,
                    name: entry.name,
                    kind: source_kind_label(&entry.source.kind),
                    tags: entry.tags.join(", "),
                    queued,
                });
            }

            if let Some(cat) = catalog {
                cat.save_atomically(paths, &paths.mcp_catalog_path())?;
            }

            Ok(RemoteSearchOutcome {
                kind,
                rows,
                imported,
            })
        }
    }
}

pub trait CatalogInstallBackend {
    fn list_tools(&self, paths: &ProjectPaths) -> (Vec<ToolDescriptor>, Vec<ToolDiagnostic>);
    fn materialize_fetch_unit(
        &self,
        paths: &ProjectPaths,
        fetch_unit: FetchUnit,
    ) -> Result<MaterializedFetchUnit>;
    fn apply(
        &self,
        paths: &ProjectPaths,
        plan: &mut ActionPlan,
        allow_user_scope: bool,
    ) -> Result<ApplyReport>;
}

#[derive(Debug, Clone)]
pub struct InstallSkillOutcome {
    pub tool_title: String,
    pub report: ApplyReport,
    pub diagnostics: Vec<ToolDiagnostic>,
}

#[derive(Debug, Clone)]
pub struct InstallMcpOutcome {
    pub report: ApplyReport,
}

pub fn install_skill(
    paths: &ProjectPaths,
    tool: &str,
    id: &str,
    backend: &dyn CatalogInstallBackend,
) -> Result<InstallSkillOutcome> {
    let catalog = load_effective_skills_catalog(paths)?;
    let entry =
        catalog.entries.iter().find(|e| e.id == id).ok_or_else(|| {
            MaccError::Validation(format!("Skill '{}' not found in catalog.", id))
        })?;

    let (descriptors, diagnostics) = backend.list_tools(paths);
    let tool_title = descriptors
        .iter()
        .find(|d| d.id == tool)
        .map(|d| d.title.clone())
        .unwrap_or_else(|| tool.to_string());

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
    let materialized = backend.materialize_fetch_unit(paths, fetch_unit)?;
    let mut plan = ActionPlan::new();
    plan_skill_install(
        &mut plan,
        tool,
        id,
        &materialized.source_root_path,
        &entry.selector.subpath,
    )
    .map_err(MaccError::Validation)?;
    let report = backend.apply(paths, &mut plan, false)?;

    Ok(InstallSkillOutcome {
        tool_title,
        report,
        diagnostics,
    })
}

pub fn install_mcp(
    paths: &ProjectPaths,
    id: &str,
    backend: &dyn CatalogInstallBackend,
) -> Result<InstallMcpOutcome> {
    let catalog = load_effective_mcp_catalog(paths)?;
    let entry = catalog.entries.iter().find(|e| e.id == id).ok_or_else(|| {
        MaccError::Validation(format!("MCP server '{}' not found in catalog.", id))
    })?;

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
    let materialized = backend.materialize_fetch_unit(paths, fetch_unit)?;
    let mut plan = ActionPlan::new();
    plan_mcp_install(
        &mut plan,
        id,
        &materialized.source_root_path,
        &entry.selector.subpath,
    )
    .map_err(MaccError::Validation)?;
    let report = backend.apply(paths, &mut plan, false)?;
    Ok(InstallMcpOutcome { report })
}

pub fn source_kind_label(kind: &SourceKind) -> &'static str {
    crate::domain::catalog::source_kind_label(kind)
}

pub fn parse_source_kind(kind: &str) -> Result<SourceKind> {
    crate::domain::catalog::parse_source_kind(kind)
}

pub fn parse_tags_csv(tags_csv: Option<&str>) -> Vec<String> {
    crate::domain::catalog::parse_tags_csv(tags_csv)
}

pub fn filter_skills(catalog: &SkillsCatalog, query: &str) -> Vec<SkillEntry> {
    crate::domain::catalog::filter_skills(catalog, query)
}

pub fn filter_mcp(catalog: &McpCatalog, query: &str) -> Vec<McpEntry> {
    crate::domain::catalog::filter_mcp(catalog, query)
}

pub fn build_skill_entry(input: CatalogEntryInput) -> Result<SkillEntry> {
    crate::domain::catalog::build_skill_entry(input)
}

pub fn build_mcp_entry(input: CatalogEntryInput) -> Result<McpEntry> {
    crate::domain::catalog::build_mcp_entry(input)
}

pub fn upsert_skill(
    paths: &ProjectPaths,
    catalog: &mut SkillsCatalog,
    entry: SkillEntry,
) -> Result<()> {
    crate::domain::catalog::upsert_skill(paths, catalog, entry)
}

pub fn remove_skill(paths: &ProjectPaths, catalog: &mut SkillsCatalog, id: &str) -> Result<bool> {
    crate::domain::catalog::remove_skill(paths, catalog, id)
}

pub fn upsert_mcp(paths: &ProjectPaths, catalog: &mut McpCatalog, entry: McpEntry) -> Result<()> {
    crate::domain::catalog::upsert_mcp(paths, catalog, entry)
}

pub fn remove_mcp(paths: &ProjectPaths, catalog: &mut McpCatalog, id: &str) -> Result<bool> {
    crate::domain::catalog::remove_mcp(paths, catalog, id)
}
