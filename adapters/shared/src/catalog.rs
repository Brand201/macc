pub use macc_core::catalog::*;
use macc_core::{MaccError, Result as MaccResult};
use reqwest::blocking::Client;
use reqwest::header::USER_AGENT;
use serde::de::DeserializeOwned;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchKind {
    Skill,
    Mcp,
}

impl SearchKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SearchKind::Skill => "skill",
            SearchKind::Mcp => "mcp",
        }
    }
}

pub fn remote_search<T: DeserializeOwned>(
    api_base: &str,
    kind: SearchKind,
    q: &str,
) -> MaccResult<Vec<T>> {
    let url = format!("{}/search", api_base.trim_end_matches('/'));

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| MaccError::Validation(format!("Failed to build HTTP client: {}", e)))?;

    let response = client
        .get(&url)
        .query(&[("kind", kind.as_str()), ("q", q)])
        .header(USER_AGENT, format!("macc/{}", env!("CARGO_PKG_VERSION")))
        .send()
        .map_err(|e| MaccError::Validation(format!("Failed to execute search request: {}", e)))?;

    let status = response.status();
    if !status.is_success() {
        let error_text = response.text().unwrap_or_else(|_| "(no body)".to_string());
        // Truncate error text if it's too long
        let truncated_error = if error_text.len() > 200 {
            format!("{}...", &error_text[..200])
        } else {
            error_text
        };
        return Err(MaccError::Validation(format!(
            "Search failed with status {}: {}",
            status, truncated_error
        )));
    }

    let search_response: RemoteSearchResponse<T> = response.json().map_err(|e| {
        MaccError::Validation(format!(
            "Failed to parse search response from {}: {}",
            url, e
        ))
    })?;

    Ok(search_response.items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::thread;

    fn bind_loopback() -> Option<(TcpListener, u16)> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => {
                let port = listener.local_addr().ok()?.port();
                Some((listener, port))
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                eprintln!("Skipping test: cannot bind loopback socket ({})", e);
                None
            }
            Err(e) => panic!("Failed to bind loopback socket: {}", e),
        }
    }

    #[test]
    fn test_remote_search_success() {
        let (listener, port) = match bind_loopback() {
            Some(v) => v,
            None => return,
        };
        let server_url = format!("http://127.0.0.1:{}", port);

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();

            // Check request line
            assert!(line.starts_with("GET /search?kind=skill&q=test HTTP/1.1"));

            // Read headers to check User-Agent
            let mut user_agent_found = false;
            while line.trim() != "" {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line.to_lowercase().starts_with("user-agent: macc/") {
                    user_agent_found = true;
                }
            }
            assert!(user_agent_found);

            let response_body = r#"{"items": [{"id": "s1", "name": "Skill 1"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        #[derive(serde::Deserialize, Debug, PartialEq)]
        struct SimpleItem {
            id: String,
            name: String,
        }

        let results: Vec<SimpleItem> =
            remote_search(&server_url, SearchKind::Skill, "test").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "s1");
        assert_eq!(results[0].name, "Skill 1");
    }

    #[test]
    fn test_remote_search_error_status() {
        let (listener, port) = match bind_loopback() {
            Some(v) => v,
            None => return,
        };
        let server_url = format!("http://127.0.0.1:{}", port);

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let response = "HTTP/1.1 400 Bad Request\r\nContent-Length: 12\r\n\r\nInvalid kind";
            stream.write_all(response.as_bytes()).unwrap();
        });

        #[derive(serde::Deserialize, Debug)]
        struct SimpleItem {}

        let result = remote_search::<SimpleItem>(&server_url, SearchKind::Mcp, "bad");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Search failed with status 400 Bad Request"));
        assert!(err_msg.contains("Invalid kind"));
    }

    #[test]
    fn test_remote_search_parse_error() {
        let (listener, port) = match bind_loopback() {
            Some(v) => v,
            None => return,
        };
        let server_url = format!("http://127.0.0.1:{}", port);

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\nNot JSON";
            stream.write_all(response.as_bytes()).unwrap();
        });

        #[derive(serde::Deserialize, Debug)]
        struct SimpleItem {}

        let result = remote_search::<SimpleItem>(&server_url, SearchKind::Skill, "test");
        assert!(result.is_err(), "Expected error, got Ok");
        let err_msg = result.unwrap_err().to_string();
        println!("Actual error: {}", err_msg);
        assert!(err_msg.contains("Failed to parse search response"));
    }
}
