use crate::plan::ops::{PlannedOp, PlannedOpKind};
use crate::{
    plan::diff::{generate_unified_diff, is_text_file, normalize_json},
    security,
};

const MAX_DIFF_LINES: usize = 600;
const MAX_DIFF_BYTES: usize = 64 * 1024;

/// The kind of diff we rendered, which helps UIs choose how to present it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffViewKind {
    Text,
    Json,
    Unsupported,
}

/// A sanitized unified diff view for a planned operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffView {
    pub kind: DiffViewKind,
    pub diff: String,
    pub truncated: bool,
}

impl DiffView {
    fn unsupported() -> Self {
        Self {
            kind: DiffViewKind::Unsupported,
            diff: String::new(),
            truncated: false,
        }
    }
}

/// Renders a diff for the provided planned operation, preferring JSON-aware output.
pub fn render_diff(op: &PlannedOp) -> DiffView {
    let Some(after) = op.after.as_deref() else {
        return DiffView::unsupported();
    };

    if let Some(diff) = render_json_diff(op, after) {
        return diff;
    }

    if let Some(diff) = render_text_diff(op, after) {
        return diff;
    }

    DiffView::unsupported()
}

fn render_json_diff(op: &PlannedOp, after: &[u8]) -> Option<DiffView> {
    if op.kind != PlannedOpKind::Merge && !op.path.ends_with(".json") {
        return None;
    }

    let normalized_after = normalize_json(after)?;
    let after_bytes = sanitize_text(&op.path, &normalized_after).into_bytes();

    let before_bytes = op.before.as_deref().and_then(|before| {
        normalize_json(before)
            .map(|normalized| sanitize_text(&op.path, &normalized).into_bytes())
            .or_else(|| {
                std::str::from_utf8(before)
                    .ok()
                    .map(|raw| sanitize_text(&op.path, raw).into_bytes())
            })
    });

    let diff = generate_unified_diff(&op.path, before_bytes.as_deref(), after_bytes.as_slice());

    let (diff, truncated) = truncate_diff(&diff);

    Some(DiffView {
        kind: DiffViewKind::Json,
        diff,
        truncated,
    })
}

fn render_text_diff(op: &PlannedOp, after: &[u8]) -> Option<DiffView> {
    if !is_text_file(&op.path, after) {
        return None;
    }

    let after_str = std::str::from_utf8(after).ok()?;
    let after_bytes = sanitize_text(&op.path, after_str).into_bytes();

    let before_bytes = op.before.as_deref().and_then(|before| {
        std::str::from_utf8(before)
            .ok()
            .map(|raw| sanitize_text(&op.path, raw).into_bytes())
    });

    let diff = generate_unified_diff(&op.path, before_bytes.as_deref(), after_bytes.as_slice());

    let (diff, truncated) = truncate_diff(&diff);

    Some(DiffView {
        kind: DiffViewKind::Text,
        diff,
        truncated,
    })
}

fn sanitize_text(path: &str, value: &str) -> String {
    let mut findings = security::scan_bytes(path, value.as_bytes());
    if findings.is_empty() {
        return value.to_string();
    }

    findings.sort_by_key(|finding| finding.range.start);
    let mut sanitized = String::with_capacity(value.len());
    let mut cursor = 0;

    for finding in findings {
        if finding.range.start < cursor {
            continue;
        }

        sanitized.push_str(&value[cursor..finding.range.start]);
        sanitized.push_str(&finding.redacted_match);
        cursor = finding.range.end;
    }

    sanitized.push_str(&value[cursor..]);
    sanitized
}

fn truncate_diff(diff: &str) -> (String, bool) {
    let ends_with_newline = diff.ends_with('\n');
    let mut lines: Vec<String> = diff.lines().map(str::to_string).collect();
    let mut truncated_by_lines = false;

    if lines.len() > MAX_DIFF_LINES {
        lines.truncate(MAX_DIFF_LINES);
        truncated_by_lines = true;
    }

    let mut truncated_by_bytes = false;

    while !lines.is_empty() && diff_len(&lines, ends_with_newline) > MAX_DIFF_BYTES {
        lines.pop();
        truncated_by_bytes = true;
    }

    let mut output = lines.join("\n");
    if ends_with_newline && !output.ends_with('\n') {
        output.push('\n');
    }

    let truncated = truncated_by_lines || truncated_by_bytes;
    if truncated {
        if !output.ends_with('\n') {
            output.push('\n');
        }

        let reason = match (truncated_by_lines, truncated_by_bytes) {
            (true, true) => "lines+bytes",
            (true, false) => "lines",
            (false, true) => "bytes",
            _ => "unknown",
        };

        output.push_str(&format!("[diff truncated: {}]\n", reason));
    }

    (output, truncated)
}

fn diff_len(lines: &[String], ends_with_newline: bool) -> usize {
    if lines.is_empty() {
        return 0;
    }

    let newline_bytes = lines.len() - 1;
    let mut len: usize = lines.iter().map(|line| line.len()).sum();
    len += newline_bytes;

    if ends_with_newline {
        len += 1;
    }

    len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::ops::PlannedOpMetadata;
    use crate::plan::Scope;

    fn diff_for(op: &PlannedOp) -> DiffView {
        render_diff(op)
    }

    fn base_op(path: &str, before: Option<&[u8]>, after: Option<&[u8]>) -> PlannedOp {
        PlannedOp {
            path: path.to_string(),
            scope: Scope::Project,
            consent_required: false,
            kind: PlannedOpKind::Write,
            metadata: PlannedOpMetadata::default(),
            before: before.map(|b| b.to_vec()),
            after: after.map(|b| b.to_vec()),
        }
    }

    #[test]
    fn text_diff_contains_hunk() {
        let before = b"line1\nline2\n";
        let after = b"line1\nline2 modified\n";
        let op = base_op("notes.txt", Some(before), Some(after));

        let view = diff_for(&op);
        assert_eq!(view.kind, DiffViewKind::Text);
        assert!(view.diff.contains("+line2 modified"));
        assert!(!view.truncated);
    }

    #[test]
    fn json_diff_respects_normalization() {
        let before = br#"{"b":2,"a":1}"#;
        let after = br#"{"a":1,"b":3}"#;
        let op = PlannedOp {
            path: "data.json".to_string(),
            scope: Scope::Project,
            consent_required: false,
            kind: PlannedOpKind::Merge,
            metadata: PlannedOpMetadata::default(),
            before: Some(before.to_vec()),
            after: Some(after.to_vec()),
        };

        let view = diff_for(&op);
        assert_eq!(view.kind, DiffViewKind::Json);
        assert!(view.diff.contains("-  \"b\": 2"));
        assert!(view.diff.contains("+  \"b\": 3"));
    }

    #[test]
    fn truncation_applies_marker() {
        let before = b"";
        let mut after = Vec::new();
        for i in 0..(MAX_DIFF_LINES + 10) {
            after.extend_from_slice(format!("line {}\n", i).as_bytes());
        }

        let op = base_op("notes.md", Some(before), Some(&after));
        let view = diff_for(&op);

        assert!(view.truncated);
        assert!(view.diff.contains("[diff truncated"));
    }

    #[test]
    fn secrets_are_redacted_in_diff() {
        let before = b"A secret: AKIA1234567890123456\n";
        let after = b"A new secret: AKIA1234567890123456\n";
        let op = base_op("secret.txt", Some(before), Some(after));

        let view = diff_for(&op);
        assert!(view.diff.contains("AKIA...3456"));
        assert!(!view.diff.contains("AKIA1234567890123456"));
    }
}
