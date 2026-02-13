use macc_core::catalog::{McpEntry, RemoteSearchResponse, SkillEntry};
use std::fs;
use std::path::PathBuf;

#[test]
fn test_deserialize_skill_search_response() {
    let path = PathBuf::from("tests/fixtures/search_response_skill.json");
    let content = fs::read_to_string(path).expect("Failed to read fixture");
    let response: RemoteSearchResponse<SkillEntry> =
        serde_json::from_str(&content).expect("Failed to deserialize");

    assert_eq!(response.items.len(), 1);
    let item = &response.items[0];
    assert_eq!(item.id, "skill-1");
    assert_eq!(item.name, "Skill 1");
    assert_eq!(item.source.url, "https://example.com/repo.git");
}

#[test]
fn test_deserialize_mcp_search_response() {
    let path = PathBuf::from("tests/fixtures/search_response_mcp.json");
    let content = fs::read_to_string(path).expect("Failed to read fixture");
    let response: RemoteSearchResponse<McpEntry> =
        serde_json::from_str(&content).expect("Failed to deserialize");

    assert_eq!(response.items.len(), 1);
    let item = &response.items[0];
    assert_eq!(item.id, "mcp-1");
    assert_eq!(item.name, "MCP 1");
    assert_eq!(item.source.checksum, Some("sha256:abc".to_string()));
}
