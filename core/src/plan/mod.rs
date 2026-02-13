use serde::{Deserialize, Serialize};

pub mod builders;
pub mod diff;
pub mod diff_view;
pub mod ops;
pub use diff::{
    compute_write_status, generate_unified_diff, is_text_file, read_existing, ActionStatus,
    ExistingFile,
};
pub use diff_view::{render_diff, DiffView, DiffViewKind};
pub use ops::{collect_plan_operations, PlannedOp, PlannedOpKind, PlannedOpMetadata};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum Scope {
    Project,
    User,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub struct ActionPlan {
    pub actions: Vec<Action>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Action {
    /// Create a directory if it doesn't exist.
    Mkdir {
        /// Path relative to project root.
        path: String,
        /// Scope of the action.
        scope: Scope,
    },

    /// Backup a file before it is modified.
    BackupFile {
        /// Path relative to project root.
        path: String,
        /// Scope of the action.
        scope: Scope,
    },

    /// Write content to a file.
    WriteFile {
        /// Path relative to project root.
        path: String,
        /// The content to write.
        content: Vec<u8>,
        /// Scope of the action.
        scope: Scope,
    },

    /// Ensure a pattern is in .gitignore.
    EnsureGitignore {
        /// The pattern to ensure.
        pattern: String,
        /// Scope of the action.
        scope: Scope,
    },

    /// Merge a JSON fragment into a file.
    MergeJson {
        /// Path relative to project root.
        path: String,
        /// The JSON fragment to merge.
        patch: serde_json::Value,
        /// Scope of the action.
        scope: Scope,
    },

    /// Mark a file as executable.
    SetExecutable {
        /// Path relative to project root.
        path: String,
        /// Scope of the action.
        scope: Scope,
    },

    /// Placeholder for M0.
    Noop {
        description: String,
        /// Scope of the action.
        scope: Scope,
    },
}

impl PartialOrd for Action {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Action {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let rank = |a: &Action| match a {
            Action::Mkdir { .. } => 0,
            Action::BackupFile { .. } => 1,
            Action::WriteFile { .. } => 2,
            Action::MergeJson { .. } => 3,
            Action::EnsureGitignore { .. } => 4,
            Action::SetExecutable { .. } => 5,
            Action::Noop { .. } => 6,
        };

        let r_cmp = rank(self).cmp(&rank(other));
        if r_cmp != std::cmp::Ordering::Equal {
            return r_cmp;
        }

        // Same variant, sort by path
        let path_cmp = self.path().cmp(other.path());
        if path_cmp != std::cmp::Ordering::Equal {
            return path_cmp;
        }

        // Same path, sort by content/details for strict determinism
        // Fallback to JSON for the rest
        let a_json = serde_json::to_string(self).unwrap_or_default();
        let b_json = serde_json::to_string(other).unwrap_or_default();
        a_json.cmp(&b_json)
    }
}

impl ActionPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn builder(scope: Scope) -> ActionPlanBuilder {
        ActionPlanBuilder::new(scope)
    }

    pub fn add_action(&mut self, action: Action) {
        // We keep all actions in the plan, even User scope.
        // Higher level logic (apply) will enforce M0 Project-only restrictions.
        self.actions.push(action);
    }

    /// Normalizes and sorts actions for deterministic execution.
    /// Ordering: Mkdir < BackupFile < WriteFile < EnsureGitignore < Noop
    pub fn normalize(&mut self) {
        self.actions.sort();
        // Dedup if exact same action is repeated
        self.actions.dedup();
    }

    /// Renders a concise summary of the plan.
    pub fn render_summary(&self, root: &std::path::Path) -> String {
        use crate::plan::diff::{compute_write_status, read_existing};
        use std::fmt::Write;

        let mut output = String::new();
        writeln!(output, "Planned changes summary:").unwrap();
        writeln!(
            output,
            "{:<8} {:<10} {:<40} {:>10}",
            "SCOPE", "STATUS", "PATH", "SIZE"
        )
        .unwrap();
        writeln!(output, "{:-<8} {:-<10} {:-<40} {:-<10}", "", "", "", "").unwrap();

        let mut sorted_actions = self.actions.clone();
        // Sort by path primarily for the summary
        sorted_actions.sort_by(|a, b| {
            let path_a = a.path();
            let path_b = b.path();
            path_a.cmp(path_b)
        });

        for action in sorted_actions {
            let scope_str = match action.scope() {
                Scope::Project => "[Proj]",
                Scope::User => "[User] (REFUSED)",
            };

            let path = action.path();
            let (status, size_str) = match &action {
                Action::WriteFile { path, content, .. } => {
                    let full_path = root.join(path);
                    let existing = read_existing(&full_path);
                    let status = compute_write_status(path, content, &existing);
                    (status.as_str(), format_size(content.len()))
                }
                Action::MergeJson {
                    path,
                    patch,
                    scope: _,
                } => {
                    let full_path = root.join(path);
                    let existing = read_existing(&full_path);

                    let mut base = if existing.exists {
                        serde_json::from_slice(existing.bytes.as_ref().unwrap_or(&vec![]))
                            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
                    } else {
                        serde_json::Value::Object(serde_json::Map::new())
                    };

                    deep_merge(&mut base, patch);
                    let content = serde_json::to_vec_pretty(&base).unwrap_or_default();

                    let status = compute_write_status(path, content.as_slice(), &existing);
                    (status.as_str(), format_size(content.len()))
                }
                Action::Mkdir { path, .. } => {
                    let full_path = root.join(path);
                    let status = if full_path.exists() { "OK" } else { "CREATE" };
                    (status, "-".to_string())
                }
                Action::SetExecutable { .. } => ("CHMOD", "-".to_string()),
                Action::BackupFile { .. } => ("BACKUP", "-".to_string()),
                Action::EnsureGitignore { .. } => ("GITIGNORE", "-".to_string()),
                Action::Noop { .. } => ("NOOP", "-".to_string()),
            };

            writeln!(
                output,
                "{:<8} {:<10} {:<40} {:>10}",
                scope_str, status, path, size_str
            )
            .unwrap();
        }

        output
    }
}

pub fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

pub fn deep_merge(base: &mut serde_json::Value, overlay: &serde_json::Value) {
    match (base, overlay) {
        (serde_json::Value::Object(base_map), serde_json::Value::Object(overlay_map)) => {
            for (k, v) in overlay_map {
                match base_map.get_mut(k) {
                    Some(existing) => deep_merge(existing, v),
                    None => {
                        base_map.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        (serde_json::Value::Array(base_arr), serde_json::Value::Array(overlay_arr)) => {
            for item in overlay_arr {
                if !base_arr.contains(item) {
                    base_arr.push(item.clone());
                }
            }
        }
        (base_slot, overlay_value) => {
            *base_slot = overlay_value.clone();
        }
    }
}

impl Action {
    pub fn path(&self) -> &str {
        match self {
            Action::Mkdir { path, .. } => path,
            Action::BackupFile { path, .. } => path,
            Action::WriteFile { path, .. } => path,
            Action::MergeJson { path, .. } => path,
            Action::SetExecutable { path, .. } => path,
            Action::EnsureGitignore { .. } => ".gitignore",
            Action::Noop { .. } => "(noop)",
        }
    }

    pub fn scope(&self) -> Scope {
        match self {
            Action::Mkdir { scope, .. } => *scope,
            Action::BackupFile { scope, .. } => *scope,
            Action::WriteFile { scope, .. } => *scope,
            Action::MergeJson { scope, .. } => *scope,
            Action::SetExecutable { scope, .. } => *scope,
            Action::EnsureGitignore { scope, .. } => *scope,
            Action::Noop { scope, .. } => *scope,
        }
    }
}

pub struct ActionPlanBuilder {
    plan: ActionPlan,
    scope: Scope,
}

impl ActionPlanBuilder {
    pub fn new(scope: Scope) -> Self {
        Self {
            plan: ActionPlan::new(),
            scope,
        }
    }

    fn validate_path(&self, path: &str) -> Result<String, String> {
        let normalized = path.replace('\\', "/");

        if self.scope == Scope::Project {
            if normalized.starts_with('/') {
                return Err(format!(
                    "Absolute path not allowed in Project scope: {}",
                    path
                ));
            }

            // Windows-style absolute paths (e.g. C:/)
            if normalized.chars().nth(1) == Some(':') {
                return Err(format!(
                    "Absolute path not allowed in Project scope: {}",
                    path
                ));
            }

            let components: Vec<&str> = normalized.split('/').collect();
            for component in components {
                if component == ".." {
                    return Err(format!("Parent directory traversal not allowed: {}", path));
                }
            }
        }

        Ok(normalized)
    }

    pub fn mkdir(&mut self, path: &str) -> Result<&mut Self, String> {
        let path = self.validate_path(path)?;
        self.plan.add_action(Action::Mkdir {
            path,
            scope: self.scope,
        });
        Ok(self)
    }

    pub fn write_text(&mut self, path: &str, content: &str) -> Result<&mut Self, String> {
        let path = self.validate_path(path)?;
        self.plan.add_action(Action::WriteFile {
            path,
            content: content.as_bytes().to_vec(),
            scope: self.scope,
        });
        Ok(self)
    }

    pub fn write_bytes(&mut self, path: &str, content: Vec<u8>) -> Result<&mut Self, String> {
        let path = self.validate_path(path)?;
        self.plan.add_action(Action::WriteFile {
            path,
            content,
            scope: self.scope,
        });
        Ok(self)
    }

    pub fn backup_file(&mut self, path: &str) -> Result<&mut Self, String> {
        let path = self.validate_path(path)?;
        self.plan.add_action(Action::BackupFile {
            path,
            scope: self.scope,
        });
        Ok(self)
    }

    pub fn merge_json(
        &mut self,
        path: &str,
        patch: serde_json::Value,
    ) -> Result<&mut Self, String> {
        let path = self.validate_path(path)?;
        self.plan.add_action(Action::MergeJson {
            path,
            patch,
            scope: self.scope,
        });
        Ok(self)
    }

    pub fn ensure_gitignore_entry(&mut self, pattern: &str) -> &mut Self {
        self.plan.add_action(Action::EnsureGitignore {
            pattern: pattern.to_string(),
            scope: self.scope,
        });
        self
    }

    pub fn noop(&mut self, description: &str) -> &mut Self {
        self.plan.add_action(Action::Noop {
            description: description.to_string(),
            scope: self.scope,
        });
        self
    }

    pub fn build(&mut self) -> ActionPlan {
        let mut plan = std::mem::take(&mut self.plan);
        plan.normalize();
        plan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_sorting() {
        let mut plan = ActionPlan::new();
        plan.add_action(Action::Noop {
            description: "last".into(),
            scope: Scope::Project,
        });
        plan.add_action(Action::WriteFile {
            path: "b.txt".into(),
            content: b"v2".to_vec(),
            scope: Scope::Project,
        });
        plan.add_action(Action::Mkdir {
            path: "a/".into(),
            scope: Scope::Project,
        });
        plan.add_action(Action::WriteFile {
            path: "a.txt".into(),
            content: b"v1".to_vec(),
            scope: Scope::Project,
        });
        plan.add_action(Action::BackupFile {
            path: "a.txt".into(),
            scope: Scope::Project,
        });
        plan.add_action(Action::EnsureGitignore {
            pattern: "*.tmp".into(),
            scope: Scope::Project,
        });

        plan.normalize();

        // Expected order based on enum variant order and then field values
        // Mkdir, BackupFile, WriteFile, EnsureGitignore, Noop
        assert!(matches!(&plan.actions[0], Action::Mkdir { .. }));
        assert!(matches!(&plan.actions[1], Action::BackupFile { .. }));
        assert!(matches!(&plan.actions[2], Action::WriteFile { path, .. } if path == "a.txt"));
        assert!(matches!(&plan.actions[3], Action::WriteFile { path, .. } if path == "b.txt"));
        assert!(matches!(&plan.actions[4], Action::EnsureGitignore { .. }));
        assert!(matches!(&plan.actions[5], Action::Noop { .. }));
    }

    #[test]
    fn test_action_dedup() {
        let mut plan = ActionPlan::new();
        plan.add_action(Action::Mkdir {
            path: "a/".into(),
            scope: Scope::Project,
        });
        plan.add_action(Action::Mkdir {
            path: "a/".into(),
            scope: Scope::Project,
        });

        plan.normalize();
        assert_eq!(plan.actions.len(), 1);
    }

    #[test]
    fn test_keep_user_scope_in_plan() {
        let mut plan = ActionPlan::new();
        plan.add_action(Action::Mkdir {
            path: "a/".into(),
            scope: Scope::User,
        });
        // In M0, we keep it in plan but refuse it in apply.
        assert_eq!(plan.actions.len(), 1);
    }

    #[test]
    fn test_render_summary() {
        let mut plan = ActionPlan::new();
        plan.add_action(Action::WriteFile {
            path: "new.txt".into(),
            content: b"hello".to_vec(),
            scope: Scope::Project,
        });
        plan.add_action(Action::Mkdir {
            path: "dir".into(),
            scope: Scope::User,
        });

        let temp_dir = std::env::temp_dir();
        let summary = plan.render_summary(&temp_dir);

        assert!(summary.contains("Planned changes summary:"));
        // Check for parts of the line to avoid fragile exact spacing if it changes slightly
        assert!(summary.contains("[Proj]"));
        assert!(summary.contains("CREATE"));
        assert!(summary.contains("new.txt"));
        assert!(summary.contains("5 B"));

        assert!(summary.contains("[User] (REFUSED)"));
        assert!(summary.contains("dir"));
    }

    #[test]
    fn test_builder_path_validation() {
        let builder = ActionPlanBuilder::new(Scope::Project);

        // Safe paths
        assert!(builder.validate_path("src").is_ok());
        assert!(builder.validate_path("src/main.rs").is_ok());
        assert!(builder.validate_path("sub/dir/file.txt").is_ok());
        assert!(builder.validate_path("./local").is_ok());

        // Unsafe paths: Absolute
        assert!(builder.validate_path("/etc/passwd").is_err());
        assert!(builder.validate_path("C:/Windows").is_err());
        assert!(builder.validate_path("D:/").is_err());

        // Unsafe paths: Traversal
        assert!(builder.validate_path("../outside").is_err());
        assert!(builder.validate_path("src/../../outside").is_err());
        assert!(builder.validate_path("a/b/../c/../../d").is_err());

        // Path normalization
        assert_eq!(builder.validate_path("a\\b\\c").unwrap(), "a/b/c");

        // User scope (no validation for absolute/traversal in User scope for now, as it's refused anyway in apply)
        let user_builder = ActionPlanBuilder::new(Scope::User);
        assert!(user_builder.validate_path("/etc/passwd").is_ok());
        assert!(user_builder.validate_path("../config").is_ok());
    }

    #[test]
    fn test_builder_helpers() {
        let plan = ActionPlan::builder(Scope::Project)
            .mkdir("new_dir")
            .unwrap()
            .write_text("hello.txt", "world")
            .unwrap()
            .write_bytes("data.bin", vec![1, 2, 3])
            .unwrap()
            .ensure_gitignore_entry("*.log")
            .build();

        assert_eq!(plan.actions.len(), 4);
        assert!(matches!(&plan.actions[0], Action::Mkdir { path, .. } if path == "new_dir"));
        assert!(
            matches!(&plan.actions[1], Action::WriteFile { path, content, .. } if path == "data.bin" && content == &vec![1, 2, 3])
        );
        assert!(
            matches!(&plan.actions[2], Action::WriteFile { path, content, .. } if path == "hello.txt" && content == b"world")
        );
        assert!(
            matches!(&plan.actions[3], Action::EnsureGitignore { pattern, .. } if pattern == "*.log")
        );
    }

    #[test]
    fn test_deep_merge() {
        let mut base = serde_json::json!({
            "a": 1,
            "obj": {
                "x": 10
            },
            "arr": [1, 2]
        });
        let overlay = serde_json::json!({
            "b": 2,
            "obj": {
                "y": 20
            },
            "arr": [2, 3]
        });

        deep_merge(&mut base, &overlay);

        assert_eq!(
            base,
            serde_json::json!({
                "a": 1,
                "b": 2,
                "obj": {
                    "x": 10,
                    "y": 20
                },
                "arr": [1, 2, 3]
            })
        );
    }
}
