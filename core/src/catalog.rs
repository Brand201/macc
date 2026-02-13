use crate::{write_if_changed, MaccError, ProjectPaths, Result as MaccResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Git,
    Http,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub kind: SourceKind,
    pub url: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subpaths: Vec<String>,
}

impl Source {
    pub fn cache_key(&self) -> String {
        use sha2::{Digest, Sha256};
        let kind_str = match self.kind {
            SourceKind::Git => "git",
            SourceKind::Http => "http",
            SourceKind::Local => "local",
        };
        let input = format!(
            "{}|{}|{}|{}",
            kind_str,
            self.url,
            self.reference,
            self.checksum.as_deref().unwrap_or("")
        );
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Returns a copy of the source without subpaths, useful for grouping.
    pub fn without_subpaths(&self) -> Self {
        Self {
            kind: self.kind.clone(),
            url: self.url.clone(),
            reference: self.reference.clone(),
            checksum: self.checksum.clone(),
            subpaths: vec![],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Selector {
    pub subpath: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub selector: Selector,
    pub source: Source,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tags: Vec<String>,
    pub selector: Selector,
    pub source: Source,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteSearchResponse<T> {
    pub items: Vec<T>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillsCatalog {
    pub schema_version: String,
    #[serde(rename = "type")]
    pub catalog_type: String,
    pub updated_at: String,
    pub entries: Vec<SkillEntry>,
}

impl Default for SkillsCatalog {
    fn default() -> Self {
        Self {
            schema_version: "1.0".to_string(),
            catalog_type: "skills".to_string(),
            updated_at: "".to_string(),
            entries: vec![],
        }
    }
}

impl SkillsCatalog {
    pub fn load(path: &Path) -> MaccResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = fs::read_to_string(path).map_err(|e| MaccError::Io {
            path: path.to_string_lossy().into(),
            action: "read skills catalog".into(),
            source: e,
        })?;
        serde_json::from_str(&content).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse skills catalog at {}: {}",
                path.display(),
                e
            ))
        })
    }

    pub fn save_atomically(&self, paths: &ProjectPaths, path: &Path) -> MaccResult<()> {
        let mut json = serde_json::to_string_pretty(self).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize skills catalog: {}", e))
        })?;
        json.push('\n');
        let _ = write_if_changed(
            paths,
            path.to_string_lossy().as_ref(),
            path,
            json.as_bytes(),
            |_| Ok(()),
        )?;
        Ok(())
    }

    pub fn upsert_skill_entry(&mut self, entry: SkillEntry) {
        if let Some(existing) = self.entries.iter_mut().find(|e| e.id == entry.id) {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
        self.update_timestamp();
    }

    pub fn delete_skill_entry(&mut self, id: &str) -> bool {
        let original_len = self.entries.len();
        self.entries.retain(|e| e.id != id);
        let changed = self.entries.len() != original_len;
        if changed {
            self.update_timestamp();
        }
        changed
    }

    fn update_timestamp(&mut self) {
        use chrono::Local;
        self.updated_at = Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    }
}

pub fn load_skills_catalog_with_local(paths: &ProjectPaths) -> MaccResult<SkillsCatalog> {
    let mut catalog = load_effective_skills_catalog(paths)?;
    let local_entries = discover_local_skill_entries(paths);

    for entry in local_entries {
        if catalog.entries.iter().any(|e| e.id == entry.id) {
            continue;
        }
        catalog.entries.push(entry);
    }

    catalog.entries.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(catalog)
}

const EMBEDDED_SKILLS_CATALOG_JSON: &str = include_str!("../../catalog/skills.catalog.json");
const EMBEDDED_MCP_CATALOG_JSON: &str = include_str!("../../catalog/mcp.catalog.json");

fn embedded_skills_catalog() -> MaccResult<SkillsCatalog> {
    serde_json::from_str(EMBEDDED_SKILLS_CATALOG_JSON).map_err(|e| {
        MaccError::Validation(format!("Failed to parse embedded skills catalog: {}", e))
    })
}

fn embedded_mcp_catalog() -> MaccResult<McpCatalog> {
    serde_json::from_str(EMBEDDED_MCP_CATALOG_JSON)
        .map_err(|e| MaccError::Validation(format!("Failed to parse embedded MCP catalog: {}", e)))
}

fn merge_skill_layers(base: SkillsCatalog, override_layer: SkillsCatalog) -> SkillsCatalog {
    let mut merged: BTreeMap<String, SkillEntry> = base
        .entries
        .into_iter()
        .map(|e| (e.id.clone(), e))
        .collect();
    for entry in override_layer.entries {
        merged.insert(entry.id.clone(), entry);
    }
    SkillsCatalog {
        schema_version: override_layer.schema_version,
        catalog_type: override_layer.catalog_type,
        updated_at: override_layer.updated_at,
        entries: merged.into_values().collect(),
    }
}

fn merge_mcp_layers(base: McpCatalog, override_layer: McpCatalog) -> McpCatalog {
    let mut merged: BTreeMap<String, McpEntry> = base
        .entries
        .into_iter()
        .map(|e| (e.id.clone(), e))
        .collect();
    for entry in override_layer.entries {
        merged.insert(entry.id.clone(), entry);
    }
    McpCatalog {
        schema_version: override_layer.schema_version,
        catalog_type: override_layer.catalog_type,
        updated_at: override_layer.updated_at,
        entries: merged.into_values().collect(),
    }
}

pub fn load_effective_skills_catalog(paths: &ProjectPaths) -> MaccResult<SkillsCatalog> {
    let embedded = embedded_skills_catalog()?;
    let user = SkillsCatalog::load(&paths.skills_catalog_path())?;
    let project = SkillsCatalog::load(&paths.project_skills_catalog_path())?;
    Ok(merge_skill_layers(
        merge_skill_layers(embedded, user),
        project,
    ))
}

pub fn load_effective_mcp_catalog(paths: &ProjectPaths) -> MaccResult<McpCatalog> {
    let embedded = embedded_mcp_catalog()?;
    let user = McpCatalog::load(&paths.mcp_catalog_path())?;
    let project = McpCatalog::load(&paths.project_mcp_catalog_path())?;
    Ok(merge_mcp_layers(merge_mcp_layers(embedded, user), project))
}

fn discover_local_skill_entries(paths: &ProjectPaths) -> Vec<SkillEntry> {
    let mut entries = Vec::new();
    let skills_root = paths.macc_dir.join("skills");
    let Ok(read_dir) = std::fs::read_dir(&skills_root) else {
        return entries;
    };

    for entry in read_dir.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if !has_skill_marker(&path) {
            continue;
        }
        let Some(id) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };

        entries.push(SkillEntry {
            id: id.to_string(),
            name: format!("Local: {}", id),
            description: format!("Local skill from {}", path.display()),
            tags: vec!["local".to_string()],
            selector: Selector {
                subpath: "".to_string(),
            },
            source: Source {
                kind: SourceKind::Local,
                url: path.to_string_lossy().into(),
                reference: "".to_string(),
                checksum: None,
                subpaths: vec![],
            },
        });
    }

    entries
}

fn has_skill_marker(path: &Path) -> bool {
    crate::packages::SKILL_MARKERS
        .iter()
        .any(|marker| path.join(marker).is_file())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct McpCatalog {
    pub schema_version: String,
    #[serde(rename = "type")]
    pub catalog_type: String,
    pub updated_at: String,
    pub entries: Vec<McpEntry>,
}

impl Default for McpCatalog {
    fn default() -> Self {
        Self {
            schema_version: "1.0".to_string(),
            catalog_type: "mcp".to_string(),
            updated_at: "".to_string(),
            entries: vec![],
        }
    }
}

impl McpCatalog {
    pub fn load(path: &Path) -> MaccResult<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = fs::read_to_string(path).map_err(|e| MaccError::Io {
            path: path.to_string_lossy().into(),
            action: "read mcp catalog".into(),
            source: e,
        })?;
        serde_json::from_str(&content).map_err(|e| {
            MaccError::Validation(format!(
                "Failed to parse mcp catalog at {}: {}",
                path.display(),
                e
            ))
        })
    }

    pub fn save_atomically(&self, paths: &ProjectPaths, path: &Path) -> MaccResult<()> {
        let mut json = serde_json::to_string_pretty(self).map_err(|e| {
            MaccError::Validation(format!("Failed to serialize mcp catalog: {}", e))
        })?;
        json.push('\n');
        let _ = write_if_changed(
            paths,
            path.to_string_lossy().as_ref(),
            path,
            json.as_bytes(),
            |_| Ok(()),
        )?;
        Ok(())
    }

    pub fn upsert_mcp_entry(&mut self, entry: McpEntry) {
        if let Some(existing) = self.entries.iter_mut().find(|e| e.id == entry.id) {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
        self.update_timestamp();
    }

    pub fn delete_mcp_entry(&mut self, id: &str) -> bool {
        let original_len = self.entries.len();
        self.entries.retain(|e| e.id != id);
        let changed = self.entries.len() != original_len;
        if changed {
            self.update_timestamp();
        }
        changed
    }

    fn update_timestamp(&mut self) {
        use chrono::Local;
        self.updated_at = Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Skill {
    pub id: String,
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub description: String,
}

pub fn builtin_skills() -> Vec<Skill> {
    vec![
        Skill {
            id: "create-plan".into(),
            name: "Create Plan".into(),
            description: "Produces a structured implementation plan from a request.".into(),
        },
        Skill {
            id: "implement".into(),
            name: "Implement".into(),
            description:
                "Guides an agent through implementing a feature with tests and validation.".into(),
        },
        Skill {
            id: "security-check".into(),
            name: "Security Check".into(),
            description: "Performs basic security checks for common issues and unsafe operations."
                .into(),
        },
    ]
}

pub fn builtin_agents() -> Vec<Agent> {
    vec![
        Agent {
            id: "architect".into(),
            name: "Architect".into(),
            description:
                "Designs system architecture and clarifies trade-offs before implementation.".into(),
        },
        Agent {
            id: "reviewer".into(),
            name: "Reviewer".into(),
            description: "Performs code review focused on correctness, safety, and readability."
                .into(),
        },
        Agent {
            id: "prompt-engineer".into(),
            name: "Prompt Engineer".into(),
            description: "Refines prompts and interaction patterns to improve assistant outputs."
                .into(),
        },
        Agent {
            id: "nextjs-developer".into(),
            name: "Next.js Developer".into(),
            description:
                "Implements product features with React/Next.js following project standards.".into(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_skill_entry_deserialization() {
        let data = json!({
            "id": "test-skill",
            "name": "Test Skill",
            "description": "A test skill",
            "tags": ["test"],
            "selector": {
                "subpath": "path/to/skill"
            },
            "source": {
                "kind": "git",
                "url": "https://github.com/test/test.git",
                "ref": "main",
                "checksum": null
            }
        });

        let entry: SkillEntry = serde_json::from_value(data).unwrap();
        assert_eq!(entry.id, "test-skill");
        assert_eq!(entry.source.kind, SourceKind::Git);
        assert_eq!(entry.source.reference, "main");
        assert!(entry.source.checksum.is_none());
    }

    #[test]
    fn test_mcp_entry_deserialization() {
        let data = json!({
            "id": "test-mcp",
            "name": "Test MCP",
            "description": "A test MCP",
            "tags": ["mcp"],
            "selector": {
                "subpath": "path/to/mcp"
            },
            "source": {
                "kind": "http",
                "url": "https://example.com/mcp.zip",
                "ref": "",
                "checksum": "sha256:123"
            }
        });

        let entry: McpEntry = serde_json::from_value(data).unwrap();
        assert_eq!(entry.id, "test-mcp");
        assert_eq!(entry.source.kind, SourceKind::Http);
        assert_eq!(entry.source.reference, "");
        assert_eq!(entry.source.checksum, Some("sha256:123".to_string()));
    }

    #[test]
    fn test_skills_catalog_deserialization() {
        let data = json!({
            "schema_version": "1.0",
            "type": "skills",
            "updated_at": "2026-01-30",
            "entries": [
                {
                    "id": "test-skill",
                    "name": "Test Skill",
                    "description": "A test skill",
                    "tags": ["test"],
                    "selector": {
                        "subpath": "path/to/skill"
                    },
                    "source": {
                        "kind": "git",
                        "url": "https://github.com/test/test.git",
                        "ref": "main",
                        "checksum": null
                    }
                }
            ]
        });

        let catalog: SkillsCatalog = serde_json::from_value(data).unwrap();
        assert_eq!(catalog.schema_version, "1.0");
        assert_eq!(catalog.catalog_type, "skills");
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].id, "test-skill");
    }

    #[test]
    fn test_mcp_catalog_deserialization() {
        let data = json!({
            "schema_version": "1.0",
            "type": "mcp",
            "updated_at": "2026-01-30",
            "entries": [
                {
                    "id": "test-mcp",
                    "name": "Test MCP",
                    "description": "A test MCP",
                    "tags": ["mcp"],
                    "selector": {
                        "subpath": "path/to/mcp"
                    },
                    "source": {
                        "kind": "git",
                        "url": "https://github.com/test/mcp.git",
                        "ref": "v1",
                        "checksum": null
                    }
                }
            ]
        });

        let catalog: McpCatalog = serde_json::from_value(data).unwrap();
        assert_eq!(catalog.schema_version, "1.0");
        assert_eq!(catalog.catalog_type, "mcp");
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].id, "test-mcp");
    }

    #[test]
    fn test_deny_unknown_fields() {
        let data = json!({
            "id": "test-skill",
            "name": "Test Skill",
            "description": "A test skill",
            "tags": ["test"],
            "selector": {
                "subpath": "path/to/skill"
            },
            "source": {
                "kind": "git",
                "url": "https://github.com/test/test.git",
                "ref": "main",
                "checksum": null
            },
            "unknown_field": "oops"
        });

        let result: Result<SkillEntry, _> = serde_json::from_value(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_skills_catalog_load_missing() {
        let path = Path::new("/tmp/non_existent_catalog_12345.json");
        let catalog = SkillsCatalog::load(path).unwrap();
        assert_eq!(catalog.entries.len(), 0);
        assert_eq!(catalog.catalog_type, "skills");
        assert_eq!(catalog.schema_version, "1.0");
    }

    #[test]
    fn test_skills_catalog_save_and_load_roundtrip() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_catalog_roundtrip_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);
        let path = paths.skills_catalog_path();

        let mut catalog = SkillsCatalog::default();
        catalog.updated_at = "2026-01-30T12:00:00Z".to_string();
        catalog.entries.push(SkillEntry {
            id: "test-id".into(),
            name: "Test Name".into(),
            description: "Test Desc".into(),
            tags: vec!["tag1".into()],
            selector: Selector {
                subpath: "path".into(),
            },
            source: Source {
                kind: SourceKind::Git,
                url: "url".into(),
                reference: "ref".into(),
                checksum: None,
                subpaths: vec![],
            },
        });

        catalog.save_atomically(&paths, &path).unwrap();

        // Check if file exists and has trailing newline
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.ends_with('\n'));
        assert!(content.contains("\"id\": \"test-id\""));

        let loaded = SkillsCatalog::load(&path).unwrap();
        assert_eq!(catalog, loaded);

        // Test idempotence of serialization
        let content2 = std::fs::read_to_string(&path).unwrap();
        catalog.save_atomically(&paths, &path).unwrap();
        let content3 = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content2, content3);

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_mcp_catalog_save_and_load_roundtrip() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_mcp_catalog_roundtrip_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);
        let path = paths.mcp_catalog_path();

        let mut catalog = McpCatalog::default();
        catalog.updated_at = "2026-01-30T12:00:00Z".to_string();
        catalog.entries.push(McpEntry {
            id: "mcp-id".into(),
            name: "MCP Name".into(),
            description: "MCP Desc".into(),
            tags: vec!["mcp-tag".into()],
            selector: Selector {
                subpath: "mcp-path".into(),
            },
            source: Source {
                kind: SourceKind::Http,
                url: "http-url".into(),
                reference: "".into(),
                checksum: Some("sha256:abc".into()),
                subpaths: vec![],
            },
        });

        catalog.save_atomically(&paths, &path).unwrap();

        let loaded = McpCatalog::load(&path).unwrap();
        assert_eq!(catalog, loaded);

        std::fs::remove_dir_all(&temp_base).ok();
    }

    #[test]
    fn test_skills_catalog_upsert_and_delete() {
        let mut catalog = SkillsCatalog::default();
        let entry1 = SkillEntry {
            id: "skill-1".into(),
            name: "Skill 1".into(),
            description: "Desc 1".into(),
            tags: vec![],
            selector: Selector {
                subpath: "p1".into(),
            },
            source: Source {
                kind: SourceKind::Git,
                url: "u1".into(),
                reference: "r1".into(),
                checksum: None,
                subpaths: vec![],
            },
        };

        // Upsert new
        catalog.upsert_skill_entry(entry1.clone());
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].id, "skill-1");
        assert!(!catalog.updated_at.is_empty());

        // Upsert update (same id)
        let mut entry1_v2 = entry1.clone();
        entry1_v2.name = "Skill 1 Updated".into();

        // Wait a tiny bit to ensure timestamp changes if clock resolution allows
        // but since we use Secs, it might not change unless we sleep 1s.
        // For testing purpose, we just check it's still there and updated.
        catalog.upsert_skill_entry(entry1_v2);
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].name, "Skill 1 Updated");

        // Delete existing
        let deleted = catalog.delete_skill_entry("skill-1");
        assert!(deleted);
        assert_eq!(catalog.entries.len(), 0);

        // Delete non-existing
        let deleted = catalog.delete_skill_entry("skill-1");
        assert!(!deleted);
    }

    #[test]
    fn test_mcp_catalog_upsert_and_delete() {
        let mut catalog = McpCatalog::default();
        let entry1 = McpEntry {
            id: "mcp-1".into(),
            name: "MCP 1".into(),
            description: "Desc 1".into(),
            tags: vec![],
            selector: Selector {
                subpath: "p1".into(),
            },
            source: Source {
                kind: SourceKind::Http,
                url: "u1".into(),
                reference: "".into(),
                checksum: None,
                subpaths: vec![],
            },
        };

        // Upsert new
        catalog.upsert_mcp_entry(entry1.clone());
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].id, "mcp-1");
        assert!(!catalog.updated_at.is_empty());

        // Upsert update
        let mut entry1_v2 = entry1.clone();
        entry1_v2.description = "Updated Desc".into();
        catalog.upsert_mcp_entry(entry1_v2);
        assert_eq!(catalog.entries.len(), 1);
        assert_eq!(catalog.entries[0].description, "Updated Desc");

        // Delete
        assert!(catalog.delete_mcp_entry("mcp-1"));
        assert_eq!(catalog.entries.len(), 0);
        assert!(!catalog.delete_mcp_entry("mcp-1"));
    }

    #[test]
    fn test_source_cache_key() {
        let source1 = Source {
            kind: SourceKind::Git,
            url: "https://github.com/test/test.git".into(),
            reference: "main".into(),
            checksum: None,
            subpaths: vec![],
        };
        let key1 = source1.cache_key();
        assert_eq!(key1.len(), 64);

        // Same source should have same key
        let source1_bis = source1.clone();
        assert_eq!(key1, source1_bis.cache_key());

        // Different ref should have different key
        let mut source2 = source1.clone();
        source2.reference = "v1".into();
        assert_ne!(key1, source2.cache_key());

        // Different checksum should have different key
        let mut source3 = source1.clone();
        source3.checksum = Some("sha256:123".into());
        assert_ne!(key1, source3.cache_key());
    }

    #[test]
    fn test_source_cache_key_with_subpaths_ignored() {
        let source1 = Source {
            kind: SourceKind::Git,
            url: "https://github.com/test/test.git".into(),
            reference: "main".into(),
            checksum: None,
            subpaths: vec!["p1".into(), "p2".into()],
        };
        let key1 = source1.cache_key();

        // Same subpaths different order should have same key
        let source2 = Source {
            kind: SourceKind::Git,
            url: "https://github.com/test/test.git".into(),
            reference: "main".into(),
            checksum: None,
            subpaths: vec!["p2".into(), "p1".into()],
        };
        assert_eq!(key1, source2.cache_key());

        // Different subpaths should NOT affect the key
        let mut source3 = source1.clone();
        source3.subpaths = vec!["p1".into()];
        assert_eq!(key1, source3.cache_key());

        // Empty vs non-empty subpaths should NOT affect the key
        let mut source4 = source1.clone();
        source4.subpaths = vec![];
        assert_eq!(key1, source4.cache_key());
    }

    #[test]
    fn test_effective_catalog_precedence_embedded_user_project() {
        let temp_base =
            std::env::temp_dir().join(format!("macc_catalog_layers_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_base).unwrap();
        let paths = ProjectPaths::from_root(&temp_base);
        std::fs::create_dir_all(&paths.catalog_dir).unwrap();
        std::fs::create_dir_all(paths.project_catalog_dir()).unwrap();

        let mut user_skills = SkillsCatalog::default();
        user_skills.entries.push(SkillEntry {
            id: "shared-id".into(),
            name: "user".into(),
            description: "user".into(),
            tags: vec![],
            selector: Selector {
                subpath: "user".into(),
            },
            source: Source {
                kind: SourceKind::Git,
                url: "https://example.com/user.git".into(),
                reference: "main".into(),
                checksum: None,
                subpaths: vec![],
            },
        });
        user_skills
            .save_atomically(&paths, &paths.skills_catalog_path())
            .unwrap();

        let mut project_skills = SkillsCatalog::default();
        project_skills.entries.push(SkillEntry {
            id: "shared-id".into(),
            name: "project".into(),
            description: "project".into(),
            tags: vec![],
            selector: Selector {
                subpath: "project".into(),
            },
            source: Source {
                kind: SourceKind::Git,
                url: "https://example.com/project.git".into(),
                reference: "main".into(),
                checksum: None,
                subpaths: vec![],
            },
        });
        project_skills
            .save_atomically(&paths, &paths.project_skills_catalog_path())
            .unwrap();

        let effective = load_effective_skills_catalog(&paths).unwrap();
        let entry = effective
            .entries
            .iter()
            .find(|e| e.id == "shared-id")
            .unwrap();
        assert_eq!(entry.name, "project");
        assert!(!effective.entries.is_empty());

        std::fs::remove_dir_all(&temp_base).ok();
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        format!("{:?}", since_the_epoch.as_nanos())
    }
}
