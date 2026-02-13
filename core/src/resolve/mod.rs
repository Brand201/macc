use crate::catalog::{load_effective_mcp_catalog, Source};
use crate::config::CanonicalConfig;
use crate::{MaccError, ProjectPaths, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ResolvedConfig {
    pub version: String,
    pub tools: ResolvedToolsConfig,
    pub standards: ResolvedStandardsConfig,
    pub selections: ResolvedSelectionsConfig,
    pub mcp_templates: Vec<crate::config::McpTemplateDefinition>,
    pub automation: crate::config::AutomationConfig,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Default)]
pub struct ResolvedToolsConfig {
    pub enabled: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub config: BTreeMap<String, serde_json::Value>,
    #[serde(flatten)]
    pub specific: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ResolvedStandardsConfig {
    pub path: Option<String>,
    pub inline: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ResolvedSelectionsConfig {
    pub skills: Vec<String>,
    pub agents: Vec<String>,
    pub mcp: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FetchUnit {
    pub source: Source,
    pub selections: Vec<Selection>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MaterializedFetchUnit {
    pub source_root_path: std::path::PathBuf,
    pub selections: Vec<Selection>,
}

pub struct PlanningContext<'a> {
    pub paths: &'a crate::ProjectPaths,
    pub resolved: &'a ResolvedConfig,
    pub materialized_units: &'a [MaterializedFetchUnit],
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Selection {
    pub id: String,
    pub subpath: String,
    pub kind: SelectionKind,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum SelectionKind {
    Skill,
    Mcp,
}

#[derive(Debug, Default, Clone)]
pub struct CliOverrides {
    pub tools: Option<Vec<String>>,
}

impl CliOverrides {
    pub fn from_tools_csv(csv: &str, allowed: &[String]) -> crate::Result<Self> {
        let tools: Vec<String> = csv
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if tools.is_empty() {
            return Ok(CliOverrides { tools: None });
        }

        let allowed_set: HashSet<&str> = allowed.iter().map(|s| s.as_str()).collect();
        let mut filtered_tools = Vec::new();
        for tool in &tools {
            if allowed_set.contains(tool.as_str()) {
                filtered_tools.push(tool.clone());
            } else {
                eprintln!("Warning: Unknown tool: {}", tool);
            }
        }

        if filtered_tools.is_empty() {
            return Ok(CliOverrides { tools: None });
        }

        Ok(CliOverrides {
            tools: Some(filtered_tools),
        })
    }
}

pub fn resolve(canonical: &CanonicalConfig, overrides: &CliOverrides) -> ResolvedConfig {
    // 1. Version (default to v1 if missing)
    let version = canonical
        .version
        .clone()
        .unwrap_or_else(|| "v1".to_string());

    // 2. Tools
    let mut enabled_tools = if let Some(override_tools) = &overrides.tools {
        override_tools.clone()
    } else {
        canonical.tools.enabled.clone()
    };
    enabled_tools.sort();
    enabled_tools.dedup();

    // 3. Standards
    let mut inline_standards: BTreeMap<String, String> = canonical
        .standards
        .inline
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // language default 'English' if not present
    if !inline_standards.contains_key("language") {
        inline_standards.insert("language".to_string(), "English".to_string());
    }

    // 4. Selections
    let mut skills = canonical
        .selections
        .as_ref()
        .map(|s| s.skills.clone())
        .unwrap_or_default();
    skills.sort();
    skills.dedup();

    let mut agents = canonical
        .selections
        .as_ref()
        .map(|s| s.agents.clone())
        .unwrap_or_default();
    agents.sort();
    agents.dedup();

    let mut mcp = canonical
        .selections
        .as_ref()
        .map(|s| s.mcp.clone())
        .unwrap_or_default();
    mcp.sort();
    mcp.dedup();

    ResolvedConfig {
        version,
        tools: ResolvedToolsConfig {
            enabled: enabled_tools,
            config: canonical.tools.config.clone(),
            specific: canonical.tools.settings.clone(),
        },
        standards: ResolvedStandardsConfig {
            path: canonical.standards.path.clone(),
            inline: inline_standards,
        },
        selections: ResolvedSelectionsConfig {
            skills,
            agents,
            mcp,
        },
        mcp_templates: canonical.mcp_templates.clone(),
        automation: canonical.automation.clone(),
    }
}

pub fn resolve_fetch_units(
    paths: &ProjectPaths,
    resolved: &ResolvedConfig,
) -> Result<Vec<FetchUnit>> {
    let skills_catalog = crate::catalog::load_skills_catalog_with_local(paths)?;
    let mcp_catalog = load_effective_mcp_catalog(paths)?;

    let mut raw_selections = Vec::new();

    let mut skill_ids = collect_skill_ids(resolved);
    skill_ids.sort();
    skill_ids.dedup();

    for id in &skill_ids {
        let entry = skills_catalog
            .entries
            .iter()
            .find(|e| &e.id == id)
            .ok_or_else(|| {
                MaccError::Validation(format!("Skill ID not found in catalog: {}", id))
            })?;
        raw_selections.push((
            entry.source.clone(),
            Selection {
                id: entry.id.clone(),
                subpath: entry.selector.subpath.clone(),
                kind: SelectionKind::Skill,
            },
        ));
    }

    for id in &resolved.selections.mcp {
        let entry = mcp_catalog
            .entries
            .iter()
            .find(|e| &e.id == id)
            .ok_or_else(|| MaccError::Validation(format!("MCP ID not found in catalog: {}", id)))?;
        raw_selections.push((
            entry.source.clone(),
            Selection {
                id: entry.id.clone(),
                subpath: entry.selector.subpath.clone(),
                kind: SelectionKind::Mcp,
            },
        ));
    }

    // Group by source (excluding subpaths)
    let mut groups: HashMap<Source, Vec<Selection>> = HashMap::new();

    for (source, selection) in raw_selections {
        let key = source.without_subpaths();
        groups.entry(key).or_default().push(selection);
    }

    let mut fetch_units = Vec::new();

    for (mut source, mut selections) in groups {
        // Collect unique subpaths for the fetch unit
        let mut unique_subpaths: Vec<String> =
            selections.iter().map(|s| s.subpath.clone()).collect();
        unique_subpaths.sort();
        unique_subpaths.dedup();

        source.subpaths = unique_subpaths;

        // Sort selections by ID for determinism
        selections.sort_by(|a, b| a.id.cmp(&b.id));

        fetch_units.push(FetchUnit { source, selections });
    }

    // Sort fetch units by source cache key for determinism
    fetch_units.sort_by(|a, b| a.source.cache_key().cmp(&b.source.cache_key()));

    Ok(fetch_units)
}

fn collect_skill_ids(resolved: &ResolvedConfig) -> Vec<String> {
    let mut ids: Vec<String> = resolved.selections.skills.clone();

    for value in resolved.tools.config.values() {
        ids.extend(read_string_list(value, "skills"));
    }
    for value in resolved.tools.specific.values() {
        ids.extend(read_string_list(value, "skills"));
    }

    ids
}

fn read_string_list(value: &serde_json::Value, key: &str) -> Vec<String> {
    let Some(node) = value.get(key) else {
        return Vec::new();
    };
    match node {
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|item| item.as_str().map(|s| s.to_string()))
            .collect(),
        serde_json::Value::String(text) => text
            .split(',')
            .map(|entry| entry.trim().to_string())
            .filter(|entry| !entry.is_empty())
            .collect(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{SelectionsConfig, StandardsConfig, ToolsConfig};

    fn ordered_tool_ids() -> (String, String, String) {
        let suffix = uuid_v4_like();
        (
            format!("a-{}", suffix),
            format!("b-{}", suffix),
            format!("c-{}", suffix),
        )
    }

    #[test]
    fn test_resolve_basic_normalization() {
        let (tool_one, tool_two, _) = ordered_tool_ids();
        let mut inline = BTreeMap::new();
        inline.insert("b".to_string(), "2".to_string());
        inline.insert("a".to_string(), "1".to_string());

        let canonical = CanonicalConfig {
            version: None,
            tools: ToolsConfig {
                enabled: vec![tool_two.clone(), tool_one.clone()],
                ..Default::default()
            },
            standards: StandardsConfig { path: None, inline },
            selections: Some(SelectionsConfig {
                skills: vec!["z".to_string(), "a".to_string()],
                agents: vec!["y".to_string(), "b".to_string()],
                mcp: vec![],
            }),
            automation: crate::config::AutomationConfig::default(),
            mcp_templates: Vec::new(),
        };

        let resolved = resolve(&canonical, &CliOverrides::default());

        assert_eq!(resolved.version, "v1");
        assert_eq!(resolved.tools.enabled, vec![tool_one, tool_two]);
        assert_eq!(resolved.selections.skills, vec!["a", "z"]);
        assert_eq!(resolved.selections.agents, vec!["b", "y"]);

        // Check stable ordering of inline standards (BTreeMap)
        let keys: Vec<_> = resolved.standards.inline.keys().cloned().collect();
        assert_eq!(keys, vec!["a", "b", "language"]);
        assert_eq!(
            resolved.standards.inline.get("language").unwrap(),
            "English"
        );
    }

    #[test]
    fn test_resolve_cli_overrides() {
        let (tool_one, tool_two, tool_three) = ordered_tool_ids();
        let canonical = CanonicalConfig {
            version: Some("v2".to_string()),
            tools: ToolsConfig {
                enabled: vec![tool_one],
                ..Default::default()
            },
            standards: StandardsConfig::default(),
            selections: None,
            automation: crate::config::AutomationConfig::default(),
            mcp_templates: Vec::new(),
        };

        let overrides = CliOverrides {
            tools: Some(vec![tool_two.clone(), tool_three.clone()]),
        };

        let resolved = resolve(&canonical, &overrides);

        assert_eq!(resolved.version, "v2");
        assert_eq!(resolved.tools.enabled, vec![tool_two, tool_three]);
    }

    #[test]
    fn test_cli_overrides_from_csv() {
        let (tool_one, tool_two, tool_three) = ordered_tool_ids();
        let allowed = vec![tool_one.clone(), tool_two.clone(), tool_three.clone()];

        // Valid CSV
        let overrides =
            CliOverrides::from_tools_csv(&format!("{}, {}", tool_one, tool_two), &allowed).unwrap();
        assert_eq!(
            overrides.tools,
            Some(vec![tool_one.clone(), tool_two.clone()])
        );

        // Valid CSV with whitespace
        let overrides =
            CliOverrides::from_tools_csv(&format!("  {}  ,{} ", tool_three, tool_one), &allowed)
                .unwrap();
        assert_eq!(
            overrides.tools,
            Some(vec![tool_three.clone(), tool_one.clone()])
        );

        // Empty CSV
        let overrides = CliOverrides::from_tools_csv("", &allowed).unwrap();
        assert_eq!(overrides.tools, None);

        // Unknown tool (should be skipped, not error)
        let overrides =
            CliOverrides::from_tools_csv(&format!("{}, unknown", tool_one), &allowed).unwrap();
        assert_eq!(overrides.tools, Some(vec![tool_one]));
    }

    #[test]
    fn test_resolve_determinism() {
        let mut inline = BTreeMap::new();
        inline.insert("a".to_string(), "1".to_string());
        inline.insert("b".to_string(), "2".to_string());

        let canonical1 = CanonicalConfig {
            version: Some("v1".to_string()),
            tools: ToolsConfig {
                enabled: vec!["z".to_string(), "a".to_string()],
                ..Default::default()
            },
            standards: StandardsConfig {
                path: None,
                inline: inline.clone(),
            },
            selections: None,
            automation: crate::config::AutomationConfig::default(),
            mcp_templates: Vec::new(),
        };

        let canonical2 = CanonicalConfig {
            version: Some("v1".to_string()),
            tools: ToolsConfig {
                enabled: vec!["a".to_string(), "z".to_string()],
                ..Default::default()
            },
            standards: StandardsConfig { path: None, inline },
            selections: None,
            automation: crate::config::AutomationConfig::default(),
            mcp_templates: Vec::new(),
        };

        let resolved1 = resolve(&canonical1, &CliOverrides::default());
        let resolved2 = resolve(&canonical2, &CliOverrides::default());

        let yaml1 = serde_yaml::to_string(&resolved1).unwrap();
        let yaml2 = serde_yaml::to_string(&resolved2).unwrap();

        assert_eq!(yaml1, yaml2);
    }

    #[test]
    fn test_resolve_fetch_units_grouping() -> Result<()> {
        use crate::catalog::{Selector, SkillEntry, Source, SourceKind};
        use std::fs;

        let temp_dir = std::env::temp_dir().join(format!("macc_resolve_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        fs::create_dir_all(&paths.catalog_dir).unwrap();

        let source1 = Source {
            kind: SourceKind::Git,
            url: "https://github.com/repo1.git".into(),
            reference: "main".into(),
            checksum: None,
            subpaths: vec![],
        };

        let mut skills_catalog = crate::catalog::SkillsCatalog::default();
        skills_catalog.entries.push(SkillEntry {
            id: "skill1".into(),
            name: "Skill 1".into(),
            description: "".into(),
            tags: vec![],
            selector: Selector {
                subpath: "skills/s1".into(),
            },
            source: source1.clone(),
        });
        skills_catalog.entries.push(SkillEntry {
            id: "skill2".into(),
            name: "Skill 2".into(),
            description: "".into(),
            tags: vec![],
            selector: Selector {
                subpath: "skills/s2".into(),
            },
            source: source1, // Same source
        });

        skills_catalog
            .save_atomically(&paths, &paths.skills_catalog_path())
            .unwrap();

        let resolved = ResolvedConfig {
            version: "v1".into(),
            tools: ResolvedToolsConfig {
                enabled: vec![],
                ..Default::default()
            },
            standards: ResolvedStandardsConfig {
                path: None,
                inline: BTreeMap::new(),
            },
            selections: ResolvedSelectionsConfig {
                skills: vec!["skill1".into(), "skill2".into()],
                agents: vec![],
                mcp: vec![],
            },
            mcp_templates: Vec::new(),
            automation: crate::config::AutomationConfig::default(),
        };

        let fetch_units = resolve_fetch_units(&paths, &resolved)?;

        assert_eq!(fetch_units.len(), 1);
        let unit = &fetch_units[0];
        assert_eq!(unit.source.url, "https://github.com/repo1.git");
        assert_eq!(unit.source.subpaths.len(), 2);
        assert!(unit.source.subpaths.contains(&"skills/s1".into()));
        assert!(unit.source.subpaths.contains(&"skills/s2".into()));
        assert_eq!(unit.selections.len(), 2);
        assert_eq!(unit.selections[0].id, "skill1");
        assert_eq!(unit.selections[1].id, "skill2");

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_resolve_fetch_units_multi_source() -> Result<()> {
        use crate::catalog::{McpCatalog, McpEntry, Selector, SkillEntry, Source, SourceKind};
        use std::fs;

        let temp_dir =
            std::env::temp_dir().join(format!("macc_resolve_multi_test_{}", uuid_v4_like()));
        fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);
        fs::create_dir_all(&paths.catalog_dir).unwrap();

        let mut skills_catalog = crate::catalog::SkillsCatalog::default();
        skills_catalog.entries.push(SkillEntry {
            id: "skill1".into(),
            name: "Skill 1".into(),
            description: "".into(),
            tags: vec![],
            selector: Selector {
                subpath: "s1".into(),
            },
            source: Source {
                kind: SourceKind::Git,
                url: "repo1".into(),
                reference: "main".into(),
                checksum: None,
                subpaths: vec![],
            },
        });

        let mut mcp_catalog = McpCatalog::default();
        mcp_catalog.entries.push(McpEntry {
            id: "mcp1".into(),
            name: "MCP 1".into(),
            description: "".into(),
            tags: vec![],
            selector: Selector {
                subpath: "m1".into(),
            },
            source: Source {
                kind: SourceKind::Http,
                url: "url1".into(),
                reference: "".into(),
                checksum: Some("sha1".into()),
                subpaths: vec![],
            },
        });

        skills_catalog
            .save_atomically(&paths, &paths.skills_catalog_path())
            .unwrap();
        mcp_catalog
            .save_atomically(&paths, &paths.mcp_catalog_path())
            .unwrap();

        let resolved = ResolvedConfig {
            version: "v1".into(),
            tools: ResolvedToolsConfig {
                enabled: vec![],
                ..Default::default()
            },
            standards: ResolvedStandardsConfig {
                path: None,
                inline: BTreeMap::new(),
            },
            selections: ResolvedSelectionsConfig {
                skills: vec!["skill1".into()],
                agents: vec![],
                mcp: vec!["mcp1".into()],
            },
            mcp_templates: Vec::new(),
            automation: crate::config::AutomationConfig::default(),
        };

        let fetch_units = resolve_fetch_units(&paths, &resolved)?;

        assert_eq!(fetch_units.len(), 2);
        // Sorted by cache key
        let git_unit = fetch_units
            .iter()
            .find(|u| u.source.kind == SourceKind::Git)
            .unwrap();
        let http_unit = fetch_units
            .iter()
            .find(|u| u.source.kind == SourceKind::Http)
            .unwrap();

        assert_eq!(git_unit.source.url, "repo1");
        assert_eq!(git_unit.selections[0].id, "skill1");
        assert_eq!(git_unit.selections[0].kind, SelectionKind::Skill);

        assert_eq!(http_unit.source.url, "url1");
        assert_eq!(http_unit.selections[0].id, "mcp1");
        assert_eq!(http_unit.selections[0].kind, SelectionKind::Mcp);

        fs::remove_dir_all(&temp_dir).ok();
        Ok(())
    }

    #[test]
    fn test_resolve_fetch_units_missing_id() {
        let temp_dir =
            std::env::temp_dir().join(format!("macc_resolve_missing_test_{}", uuid_v4_like()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let paths = ProjectPaths::from_root(&temp_dir);

        let resolved = ResolvedConfig {
            version: "v1".into(),
            tools: ResolvedToolsConfig {
                enabled: vec![],
                ..Default::default()
            },
            standards: ResolvedStandardsConfig {
                path: None,
                inline: BTreeMap::new(),
            },
            selections: ResolvedSelectionsConfig {
                skills: vec!["nonexistent".into()],
                agents: vec![],
                mcp: vec![],
            },
            mcp_templates: Vec::new(),
            automation: crate::config::AutomationConfig::default(),
        };

        let result = resolve_fetch_units(&paths, &resolved);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Skill ID not found"));

        std::fs::remove_dir_all(&temp_dir).ok();
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
