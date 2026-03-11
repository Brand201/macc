use crate::catalog::{
    McpCatalog, McpEntry, Selector, SkillEntry, SkillsCatalog, Source, SourceKind,
};
use crate::{is_required_skill, MaccError, ProjectPaths, Result};

#[derive(Debug, Clone)]
pub struct CatalogEntryInput {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags_csv: Option<String>,
    pub subpath: String,
    pub kind: String,
    pub url: String,
    pub reference: String,
    pub checksum: Option<String>,
}

pub fn source_kind_label(kind: &SourceKind) -> &'static str {
    match kind {
        SourceKind::Git => "git",
        SourceKind::Http => "http",
        SourceKind::Local => "local",
    }
}

pub fn parse_source_kind(kind: &str) -> Result<SourceKind> {
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

pub fn parse_tags_csv(tags_csv: Option<&str>) -> Vec<String> {
    tags_csv
        .map(|t| {
            t.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

pub fn filter_skills(catalog: &SkillsCatalog, query: &str) -> Vec<SkillEntry> {
    let query = query.to_lowercase();
    catalog
        .entries
        .iter()
        .filter(|e| {
            e.id.to_lowercase().contains(&query)
                || e.name.to_lowercase().contains(&query)
                || e.description.to_lowercase().contains(&query)
                || e.tags.iter().any(|t| t.to_lowercase().contains(&query))
        })
        .cloned()
        .collect()
}

pub fn filter_mcp(catalog: &McpCatalog, query: &str) -> Vec<McpEntry> {
    let query = query.to_lowercase();
    catalog
        .entries
        .iter()
        .filter(|e| {
            e.id.to_lowercase().contains(&query)
                || e.name.to_lowercase().contains(&query)
                || e.description.to_lowercase().contains(&query)
                || e.tags.iter().any(|t| t.to_lowercase().contains(&query))
        })
        .cloned()
        .collect()
}

pub fn build_skill_entry(input: CatalogEntryInput) -> Result<SkillEntry> {
    let source_kind = parse_source_kind(&input.kind)?;
    Ok(SkillEntry {
        id: input.id,
        name: input.name,
        description: input.description,
        tags: parse_tags_csv(input.tags_csv.as_deref()),
        selector: Selector {
            subpath: input.subpath,
        },
        source: Source {
            kind: source_kind,
            url: input.url,
            reference: input.reference,
            checksum: input.checksum,
            subpaths: vec![],
        },
    })
}

pub fn build_mcp_entry(input: CatalogEntryInput) -> Result<McpEntry> {
    let source_kind = parse_source_kind(&input.kind)?;
    Ok(McpEntry {
        id: input.id,
        name: input.name,
        description: input.description,
        tags: parse_tags_csv(input.tags_csv.as_deref()),
        selector: Selector {
            subpath: input.subpath,
        },
        source: Source {
            kind: source_kind,
            url: input.url,
            reference: input.reference,
            checksum: input.checksum,
            subpaths: vec![],
        },
    })
}

pub fn upsert_skill(
    paths: &ProjectPaths,
    catalog: &mut SkillsCatalog,
    entry: SkillEntry,
) -> Result<()> {
    catalog.upsert_skill_entry(entry);
    catalog.save_atomically(paths, &paths.skills_catalog_path())
}

pub fn remove_skill(paths: &ProjectPaths, catalog: &mut SkillsCatalog, id: &str) -> Result<bool> {
    if is_required_skill(id) {
        return Err(MaccError::Validation(format!(
            "cannot disable required skill '{}'",
            id
        )));
    }
    let removed = catalog.delete_skill_entry(id);
    if removed {
        catalog.save_atomically(paths, &paths.skills_catalog_path())?;
    }
    Ok(removed)
}

pub fn upsert_mcp(paths: &ProjectPaths, catalog: &mut McpCatalog, entry: McpEntry) -> Result<()> {
    catalog.upsert_mcp_entry(entry);
    catalog.save_atomically(paths, &paths.mcp_catalog_path())
}

pub fn remove_mcp(paths: &ProjectPaths, catalog: &mut McpCatalog, id: &str) -> Result<bool> {
    let removed = catalog.delete_mcp_entry(id);
    if removed {
        catalog.save_atomically(paths, &paths.mcp_catalog_path())?;
    }
    Ok(removed)
}
