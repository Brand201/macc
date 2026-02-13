use similar::TextDiff;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingFile {
    pub exists: bool,
    pub bytes: Option<Vec<u8>>,
    pub is_text_guess: bool,
}

/// Reads an existing file from the filesystem.
/// Returns information about whether the file exists, its content, and whether it's likely text.
pub fn read_existing<P: AsRef<Path>>(path: P) -> ExistingFile {
    let path = path.as_ref();
    if !path.exists() || !path.is_file() {
        return ExistingFile {
            exists: false,
            bytes: None,
            is_text_guess: false,
        };
    }

    match fs::read(path) {
        Ok(bytes) => {
            let is_text_guess = is_text_file(path, &bytes);
            ExistingFile {
                exists: true,
                bytes: Some(bytes),
                is_text_guess,
            }
        }
        Err(_) => {
            // If we can't read it (e.g. permission denied), we treat it as missing for the purpose of diffing
            ExistingFile {
                exists: false,
                bytes: None,
                is_text_guess: false,
            }
        }
    }
}

/// Heuristic to guess if a file is text based on extension and content.
pub fn is_text_file<P: AsRef<Path>>(path: P, bytes: &[u8]) -> bool {
    let path = path.as_ref();

    // Extension-based rules (requested by task)
    if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
        match ext.to_lowercase().as_str() {
            "md" | "txt" | "rules" | "toml" | "yaml" | "yml" | "json" | "sh" | "rs" => return true,
            _ => {}
        }
    }

    // Fallback to content-based heuristic
    is_text(bytes)
}

/// Simple heuristic to guess if a buffer is text.
/// Checks for UTF-8 validity and absence of null bytes.
fn is_text(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }

    // Check for null bytes which usually indicate binary data
    if bytes.contains(&0) {
        return false;
    }

    // Check if it's valid UTF-8
    std::str::from_utf8(bytes).is_ok()
}

/// Tries to parse bytes as JSON and return a pretty-printed string with stable key ordering.
/// Returns None if parsing fails.
pub fn normalize_json(bytes: &[u8]) -> Option<String> {
    let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    // serde_json::to_string_pretty uses sorted keys by default (unless preserve_order feature is enabled)
    serde_json::to_string_pretty(&v).ok().map(|mut s| {
        if !s.ends_with('\n') {
            s.push('\n');
        }
        s
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActionStatus {
    Created,
    Updated,
    Unchanged,
    Noop,
    Unknown,
}

impl ActionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionStatus::Created => "CREATE",
            ActionStatus::Updated => "UPDATE",
            ActionStatus::Unchanged => "OK",
            ActionStatus::Noop => "NOOP",
            ActionStatus::Unknown => "UNKNOWN",
        }
    }
}

/// Computes the status of a WriteFile action.
pub fn compute_write_status(
    path: &str,
    new_content: &[u8],
    existing: &ExistingFile,
) -> ActionStatus {
    if !existing.exists {
        return ActionStatus::Created;
    }

    let old_content = existing.bytes.as_deref().unwrap_or(&[]);

    if path.ends_with(".json") {
        let old_norm = normalize_json(old_content);
        let new_norm = normalize_json(new_content);

        match (old_norm, new_norm) {
            (Some(o), Some(n)) => {
                if o == n {
                    ActionStatus::Unchanged
                } else {
                    ActionStatus::Updated
                }
            }
            _ => {
                // Fallback to byte comparison if JSON normalization fails
                if old_content == new_content {
                    ActionStatus::Unchanged
                } else {
                    ActionStatus::Updated
                }
            }
        }
    } else if old_content == new_content {
        ActionStatus::Unchanged
    } else {
        ActionStatus::Updated
    }
}

/// Generates a unified diff between old and new content.
pub fn generate_unified_diff(path: &str, old_content: Option<&[u8]>, new_content: &[u8]) -> String {
    let is_json = path.ends_with(".json");

    let old_str_owned: String;
    let old_str = if is_json {
        if let Some(old_bytes) = old_content {
            if let Some(normalized) = normalize_json(old_bytes) {
                old_str_owned = normalized;
                &old_str_owned
            } else {
                std::str::from_utf8(old_bytes).unwrap_or("")
            }
        } else {
            ""
        }
    } else {
        old_content
            .and_then(|b| std::str::from_utf8(b).ok())
            .unwrap_or("")
    };

    let new_str_owned: String;
    let new_str = if is_json {
        if let Some(normalized) = normalize_json(new_content) {
            new_str_owned = normalized;
            &new_str_owned
        } else {
            std::str::from_utf8(new_content).unwrap_or("")
        }
    } else {
        std::str::from_utf8(new_content).unwrap_or("")
    };

    let diff = TextDiff::from_lines(old_str, new_str);

    let header_old = if old_content.is_none() {
        "/dev/null"
    } else {
        path
    };
    let header_new = path;

    let mut output = diff
        .unified_diff()
        .context_radius(3)
        .header(header_old, header_new)
        .to_string();

    // Ensure the output ends with a newline if it's not empty
    if !output.is_empty() && !output.ends_with('\n') {
        output.push('\n');
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use std::io::Write;

    fn temp_file_path(name: &str) -> std::path::PathBuf {
        let mut path = env::temp_dir();
        path.push(format!("macc_test_{}_{}", uuid_v4_like(), name));
        path
    }

    fn uuid_v4_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        format!("{:?}", since_the_epoch.as_nanos())
    }

    #[test]
    fn test_read_missing_file() {
        let res = read_existing("non_existent_file_12345.txt");
        assert!(!res.exists);
        assert!(res.bytes.is_none());
        assert!(!res.is_text_guess);
    }

    #[test]
    fn test_read_text_file() {
        let path = temp_file_path("text.txt");
        {
            let mut file = fs::File::create(&path).unwrap();
            writeln!(file, "Hello, world!").unwrap();
        }

        let res = read_existing(&path);
        assert!(res.exists);
        assert_eq!(res.bytes.unwrap(), b"Hello, world!\n");
        assert!(res.is_text_guess);

        fs::remove_file(path).ok();
    }

    #[test]
    fn test_is_text_file_extensions() {
        assert!(is_text_file("test.md", b""));
        assert!(is_text_file("test.rules", b""));
        assert!(is_text_file("test.toml", b""));
        // Binary content but text extension should return true (as per our rules)
        assert!(is_text_file("test.txt", &[0, 1, 2]));
        // No extension, binary content -> false
        assert!(!is_text_file("test", &[0, 1, 2]));
    }

    #[test]
    fn test_generate_unified_diff_simple() {
        let old = Some(b"line1\nline2\nline3\n".as_slice());
        let new = b"line1\nline2 changed\nline3\n";
        let diff = generate_unified_diff("test.txt", old, new);

        assert!(diff.contains("--- test.txt"));
        assert!(diff.contains("+++ test.txt"));
        assert!(diff.contains("-line2"));
        assert!(diff.contains("+line2 changed"));
    }

    #[test]
    fn test_generate_unified_diff_new_file() {
        let old = None;
        let new = b"new file content\n";
        let diff = generate_unified_diff("new.txt", old, new);

        assert!(diff.contains("--- /dev/null"));
        assert!(diff.contains("+++ new.txt"));
        assert!(diff.contains("+new file content"));
    }

    #[test]
    fn test_normalize_json_stable() {
        let old = Some(br#"{"b": 2, "a": 1}"#.as_slice());
        let new = br#"{"a": 1, "b": 2}"#;
        // They are semantically same, so normalized should be identical.
        let diff = generate_unified_diff("test.json", old, new);
        assert_eq!(
            diff, "",
            "Diff should be empty for semantically identical JSON"
        );
    }

    #[test]
    fn test_normalize_json_changed() {
        let old = Some(br#"{"a": 1}"#.as_slice());
        let new = br#"{"a": 2}"#;
        let diff = generate_unified_diff("test.json", old, new);
        assert!(diff.contains("-  \"a\": 1"));
        assert!(diff.contains("+  \"a\": 2"));
    }

    #[test]
    fn test_normalize_json_invalid_fallback() {
        let old = Some(b"not json".as_slice());
        let new = b"still not json but different";
        let diff = generate_unified_diff("test.json", old, new);
        assert!(diff.contains("-not json"));
        assert!(diff.contains("+still not json but different"));
    }
}
