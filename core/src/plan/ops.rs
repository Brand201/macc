use super::{Action, ActionPlan, Scope};
use crate::plan::diff::read_existing;
use crate::ProjectPaths;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Metadata such as backup and consent requirements associated with a planned operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PlannedOpMetadata {
    pub backup_required: bool,
    pub consent_required: bool,
    pub set_executable: bool,
}

/// The high-level kind of operation that will happen to a file or directory.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum PlannedOpKind {
    Write,
    Merge,
    Delete,
    Mkdir,
    Other,
}

/// A description of an individual file operation that can be rendered or diffed by downstream UIs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlannedOp {
    pub path: String,
    pub scope: Scope,
    /// Whether the operation targets a user-scoped file and therefore needs explicit consent.
    pub consent_required: bool,
    pub kind: PlannedOpKind,
    pub metadata: PlannedOpMetadata,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
}

struct OperationAccumulator {
    scope: Scope,
    backup_required: bool,
    set_executable: bool,
    kind: Option<PlannedOpKind>,
    write_content: Option<Vec<u8>>,
    merge_patches: Vec<Value>,
    ensure_patterns: BTreeSet<String>,
}

impl Default for OperationAccumulator {
    fn default() -> Self {
        Self {
            scope: Scope::Project,
            backup_required: false,
            set_executable: false,
            kind: None,
            write_content: None,
            merge_patches: Vec::new(),
            ensure_patterns: BTreeSet::new(),
        }
    }
}

impl OperationAccumulator {
    fn update_scope(&mut self, other: Scope) {
        if other == Scope::User || self.scope == Scope::User {
            self.scope = Scope::User;
        }
    }

    fn set_kind(&mut self, kind: PlannedOpKind) {
        match (self.kind, kind) {
            (Some(existing), candidate) if existing == candidate => {}
            (Some(PlannedOpKind::Write), _) => {}
            (Some(existing), candidate) if candidate == PlannedOpKind::Write => {
                self.kind = Some(candidate)
            }
            _ => {
                self.kind = Some(kind);
            }
        }
    }

    fn into_planned_op(self, paths: &ProjectPaths, path: String) -> Option<PlannedOp> {
        let kind = self.kind.unwrap_or(PlannedOpKind::Other);

        if kind == PlannedOpKind::Other
            && self.merge_patches.is_empty()
            && self.write_content.is_none()
        {
            return None;
        }

        let full_path = paths.root.join(&path);
        let existing = read_existing(&full_path);
        let before = existing.bytes.clone();
        let consent_required = self.scope == Scope::User;

        let after = match kind {
            PlannedOpKind::Write => self
                .write_content
                .or_else(|| compute_gitignore_after(&existing, &self.ensure_patterns)),
            PlannedOpKind::Merge => compute_merge_after(&existing, &self.merge_patches),
            _ => None,
        };

        Some(PlannedOp {
            path,
            scope: if self.scope == Scope::User {
                Scope::User
            } else {
                Scope::Project
            },
            consent_required,
            kind,
            metadata: PlannedOpMetadata {
                backup_required: self.backup_required,
                consent_required,
                set_executable: self.set_executable,
            },
            before,
            after,
        })
    }
}

/// Collects operations from an existing `ActionPlan` with deterministic ordering.
pub fn collect_plan_operations(paths: &ProjectPaths, plan: &ActionPlan) -> Vec<PlannedOp> {
    let mut accumulator: BTreeMap<String, OperationAccumulator> = BTreeMap::new();

    for action in &plan.actions {
        match action {
            Action::WriteFile {
                path,
                content,
                scope,
            } => {
                let entry = accumulator.entry(path.clone()).or_default();
                entry.set_kind(PlannedOpKind::Write);
                entry.write_content = Some(content.clone());
                entry.update_scope(*scope);
            }
            Action::MergeJson { path, patch, scope } => {
                let entry = accumulator.entry(path.clone()).or_default();
                entry.set_kind(PlannedOpKind::Merge);
                entry.merge_patches.push(patch.clone());
                entry.update_scope(*scope);
            }
            Action::BackupFile { path, scope } => {
                let entry = accumulator.entry(path.clone()).or_default();
                entry.backup_required = true;
                entry.update_scope(*scope);
            }
            Action::Mkdir { path, scope } => {
                let entry = accumulator.entry(path.clone()).or_default();
                entry.set_kind(PlannedOpKind::Mkdir);
                entry.update_scope(*scope);
            }
            Action::EnsureGitignore { pattern, scope } => {
                let entry = accumulator.entry(".gitignore".to_string()).or_default();
                entry.set_kind(PlannedOpKind::Write);
                entry.ensure_patterns.insert(pattern.clone());
                entry.update_scope(*scope);
            }
            Action::SetExecutable { path, scope } => {
                let entry = accumulator.entry(path.clone()).or_default();
                entry.set_executable = true;
                entry.update_scope(*scope);
            }
            _ => {}
        }
    }

    let mut ops: Vec<PlannedOp> = accumulator
        .into_iter()
        .filter_map(|(path, entry)| entry.into_planned_op(paths, path))
        .collect();

    ops.sort_by(|a, b| a.path.cmp(&b.path).then(a.kind.cmp(&b.kind)));
    ops
}

fn compute_merge_after(
    existing: &crate::plan::diff::ExistingFile,
    patches: &[Value],
) -> Option<Vec<u8>> {
    if patches.is_empty() {
        return None;
    }

    let mut base = if existing.exists {
        serde_json::from_slice(existing.bytes.as_deref().unwrap_or(&[]))
            .unwrap_or_else(|_| Value::Object(serde_json::Map::new()))
    } else {
        Value::Object(serde_json::Map::new())
    };

    for patch in patches {
        crate::plan::deep_merge(&mut base, patch);
    }

    serde_json::to_vec_pretty(&base).ok()
}

fn compute_gitignore_after(
    existing: &crate::plan::diff::ExistingFile,
    patterns: &BTreeSet<String>,
) -> Option<Vec<u8>> {
    if patterns.is_empty() {
        return None;
    }

    let mut lines: Vec<String> = existing
        .bytes
        .as_deref()
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .map(|content| content.lines().map(|line| line.to_string()).collect())
        .unwrap_or_default();

    for pattern in patterns {
        if !lines.iter().any(|line| line.trim() == pattern) {
            lines.push(pattern.clone());
        }
    }

    let mut output = lines.join("\n");
    if !output.is_empty() && !output.ends_with('\n') {
        output.push('\n');
    }

    Some(output.into_bytes())
}
