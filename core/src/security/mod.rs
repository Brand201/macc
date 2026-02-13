pub mod secret_scan;

pub use secret_scan::{scan_bytes, Finding, Severity};

/// Standard placeholder strings and patterns.
pub const RECOMMENDED_PLACEHOLDERS: &[&str] = &[
    "YOUR_API_KEY_HERE",
    "YOUR_SECRET_HERE",
    "REPLACE_ME",
    "${ENV_VAR}",
];

/// Common regex patterns for placeholders.
pub const PLACEHOLDER_PATTERNS: &[&str] =
    &[r"YOUR_[A-Z0-9_]+_HERE", r"\$\{[A-Z0-9_]+\}", r"REPLACE_ME"];

/// Checks if a string contains any of the standard placeholder patterns.
pub fn contains_placeholder(content: &str) -> bool {
    use regex::Regex;
    use std::sync::OnceLock;

    static RES: OnceLock<Vec<Regex>> = OnceLock::new();
    let res = RES.get_or_init(|| {
        PLACEHOLDER_PATTERNS
            .iter()
            .map(|p| Regex::new(p).expect("Invalid placeholder regex"))
            .collect()
    });

    res.iter().any(|re| re.is_match(content))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_placeholder() {
        assert!(contains_placeholder("Here is YOUR_API_KEY_HERE"));
        assert!(contains_placeholder("Use ${DATABASE_URL} for connection"));
        assert!(contains_placeholder("Change this: REPLACE_ME"));
        assert!(!contains_placeholder("Normal text with no placeholders"));
        assert!(!contains_placeholder("akia1234567890123456")); // lower case, not our placeholder
    }
}
