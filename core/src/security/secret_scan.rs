use regex::Regex;
use std::ops::Range;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Clone)]
pub struct Finding {
    pub path: String,
    pub pattern_name: String,
    pub redacted_match: String,
    pub range: Range<usize>,
    pub severity: Severity,
}

struct SecretPattern {
    name: &'static str,
    regex: &'static str,
    severity: Severity,
}

const PATTERNS: &[SecretPattern] = &[
    SecretPattern {
        name: "AWS Access Key",
        regex: r"AKIA[0-9A-Z]{16}",
        severity: Severity::Error,
    },
    SecretPattern {
        name: "Generic Secret Token",
        regex: r"sk-[a-zA-Z0-9]{20,}",
        severity: Severity::Error,
    },
    SecretPattern {
        name: "GitHub Token",
        regex: r"ghp_[a-zA-Z0-9]{36}",
        severity: Severity::Error,
    },
];

static RE_LIST: OnceLock<Vec<(&'static SecretPattern, Regex)>> = OnceLock::new();

fn get_regexes() -> &'static [(&'static SecretPattern, Regex)] {
    RE_LIST.get_or_init(|| {
        PATTERNS
            .iter()
            .map(|p| (p, Regex::new(p.regex).expect("Invalid regex pattern")))
            .collect()
    })
}

pub fn scan_bytes(path: &str, bytes: &[u8]) -> Vec<Finding> {
    let mut findings = Vec::new();

    // We assume UTF-8 for secret scanning as most secrets are ASCII-compatible.
    // If it's not valid UTF-8, we just skip it for now (best-effort).
    let content = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => return findings,
    };

    for (pattern, re) in get_regexes() {
        for m in re.find_iter(content) {
            findings.push(Finding {
                path: path.to_string(),
                pattern_name: pattern.name.to_string(),
                redacted_match: redact(m.as_str()),
                range: m.range(),
                severity: pattern.severity.clone(),
            });
        }
    }

    findings
}

fn redact(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 8 {
        return "****".to_string();
    }

    let first_part: String = chars.iter().take(4).collect();
    let last_part: String = chars.iter().skip(chars.len() - 4).collect();

    format!("{}...{}", first_part, last_part)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redact() {
        assert_eq!(redact("AKIA1234567890123456"), "AKIA...3456");
        assert_eq!(redact("sk-abcdefghijklmnopqrstuvwxyz"), "sk-a...wxyz");
        assert_eq!(redact("short"), "****");
    }

    #[test]
    fn test_scan_bytes() {
        let content =
            b"Here is a secret: AKIA1234567890123456 and another: sk-12345678901234567890";
        let findings = scan_bytes("test.txt", content);

        assert_eq!(findings.len(), 2);
        assert_eq!(findings[0].pattern_name, "AWS Access Key");
        assert_eq!(findings[0].redacted_match, "AKIA...3456");
        assert_eq!(findings[1].pattern_name, "Generic Secret Token");
        assert_eq!(findings[1].redacted_match, "sk-1...7890");

        // Verify that the full secret is NOT in the redacted_match
        assert!(!findings[0].redacted_match.contains("123456789012"));
        assert!(!findings[1].redacted_match.contains("123456789012"));
    }

    #[test]
    fn test_scan_no_secrets() {
        let content = b"This is just some normal text with no secrets.";
        let findings = scan_bytes("safe.txt", content);
        assert!(findings.is_empty());
    }

    #[test]
    fn test_pattern_variety() {
        // Test with different valid lengths and formats
        let content =
            b"sk-ABCdef1234567890GHIJ (23 chars) and sk-verylongtoken1234567890abcdef (32 chars)";
        let findings = scan_bytes("multi.txt", content);
        assert_eq!(findings.len(), 2);
        assert_eq!(findings[0].redacted_match, "sk-A...GHIJ");
        assert_eq!(findings[1].redacted_match, "sk-v...cdef");

        // Test GitHub token
        let content = b"My github token is ghp_123456789012345678901234567890123456";
        let findings = scan_bytes("gh.txt", content);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].pattern_name, "GitHub Token");
        assert_eq!(findings[0].redacted_match, "ghp_...3456");
        assert!(!findings[0].redacted_match.contains("123456789012"));
    }
}
