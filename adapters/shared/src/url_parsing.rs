use regex::Regex;
use std::sync::OnceLock;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedGit {
    pub clone_url: String,
    pub reference: String,
    pub subpath: String,
}

static TREE_RE: OnceLock<Regex> = OnceLock::new();
static PLAIN_RE: OnceLock<Regex> = OnceLock::new();
static CHECKSUM_RE: OnceLock<Regex> = OnceLock::new();

pub fn normalize_git_input(url: &str) -> Option<NormalizedGit> {
    let url = url.trim();

    let tree_re = TREE_RE.get_or_init(|| {
        Regex::new(r"^https?://github\.com/([^/]+)/([^/]+)/tree/([^/]+)/(.*)$").unwrap()
    });

    if let Some(caps) = tree_re.captures(url) {
        let org = &caps[1];
        let repo = &caps[2];
        let reference = &caps[3];
        let subpath = caps[4].trim_matches('/');
        return Some(NormalizedGit {
            clone_url: format!("https://github.com/{}/{}.git", org, repo),
            reference: reference.to_string(),
            subpath: subpath.to_string(),
        });
    }

    let plain_re = PLAIN_RE.get_or_init(|| {
        Regex::new(r"^https?://github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$").unwrap()
    });

    if let Some(caps) = plain_re.captures(url) {
        let org = &caps[1];
        let repo = &caps[2];
        return Some(NormalizedGit {
            clone_url: format!("https://github.com/{}/{}.git", org, repo),
            reference: "".to_string(),
            subpath: "".to_string(),
        });
    }

    None
}

pub fn validate_http_url(url: &str) -> bool {
    let url = url.trim().to_lowercase();
    url.starts_with("http://") || url.starts_with("https://")
}

pub fn validate_checksum(checksum: &str) -> bool {
    let checksum_re = CHECKSUM_RE.get_or_init(|| Regex::new(r"^sha256:[0-9a-fA-F]{64}$").unwrap());
    checksum_re.is_match(checksum)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_github_tree() {
        let input = "https://github.com/macc-project/macc/tree/master/adapters/shared";
        let expected = NormalizedGit {
            clone_url: "https://github.com/macc-project/macc.git".to_string(),
            reference: "master".to_string(),
            subpath: "adapters/shared".to_string(),
        };
        assert_eq!(normalize_git_input(input), Some(expected));
    }

    #[test]
    fn test_normalize_github_plain() {
        let inputs = vec![
            "https://github.com/macc-project/macc",
            "https://github.com/macc-project/macc.git",
            "https://github.com/macc-project/macc/",
            "http://github.com/macc-project/macc.git/",
        ];
        let expected = NormalizedGit {
            clone_url: "https://github.com/macc-project/macc.git".to_string(),
            reference: "".to_string(),
            subpath: "".to_string(),
        };
        for input in inputs {
            assert_eq!(
                normalize_git_input(input),
                Some(expected.clone()),
                "Failed for {}",
                input
            );
        }
    }

    #[test]
    fn test_normalize_github_nested_subpath() {
        let input = "https://github.com/org/repo/tree/v1.2.3/deeply/nested/folder/";
        let expected = NormalizedGit {
            clone_url: "https://github.com/org/repo.git".to_string(),
            reference: "v1.2.3".to_string(),
            subpath: "deeply/nested/folder".to_string(),
        };
        assert_eq!(normalize_git_input(input), Some(expected));
    }

    #[test]
    fn test_normalize_unsupported_url() {
        let inputs = vec![
            "https://gitlab.com/org/repo",
            "https://example.com",
            "not a url",
        ];
        for input in inputs {
            assert_eq!(
                normalize_git_input(input),
                None,
                "Should be None for {}",
                input
            );
        }
    }

    #[test]
    fn test_validate_http_url() {
        assert!(validate_http_url("http://example.com/file.zip"));
        assert!(validate_http_url("https://example.com/file.zip"));
        assert!(validate_http_url("HTTPS://EXAMPLE.COM/FILE.ZIP"));
        assert!(!validate_http_url("ftp://example.com/file.zip"));
        assert!(!validate_http_url("not a url"));
    }

    #[test]
    fn test_validate_checksum() {
        assert!(validate_checksum(
            "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        assert!(validate_checksum(
            "sha256:E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
        ));
        assert!(!validate_checksum("sha256:short"));
        assert!(!validate_checksum(
            "md5:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        ));
        assert!(!validate_checksum(
            "sha256:gggggggggggggggggggggggg6fb92427ae41e4649b934ca495991b7852b855"
        ));
    }
}
