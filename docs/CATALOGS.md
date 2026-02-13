# MACC Catalog Management

MACC uses local catalogs to manage **Skills** and **MCP Servers**. These catalogs act as a bridge between remote sources (Git repositories, HTTP archives) and your local AI tool configurations.

## File Locations

By default, catalogs are stored in your project's `.macc` directory:
- `.macc/catalog/skills.catalog.json`: Registry of available skills.
- `.macc/catalog/mcp.catalog.json`: Registry of available Model Context Protocol (MCP) servers.

## Catalog Schema

Catalogs follow a standard JSON schema:

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | `string` | Version of the catalog schema (currently `1.0`). |
| `type` | `string` | `skills` or `mcp`. |
| `updated_at` | `string` | Last modification timestamp. |
| `entries` | `array` | List of catalog entries. |

### Entry Schema

Each entry in the `entries` array contains:

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique identifier for the entry. |
| `name` | `string` | Human-readable name. |
| `description` | `string` | Short description of what it does. |
| `tags` | `string[]` | List of tags for categorization and search. |
| `selector.subpath` | `string` | Path inside the source (e.g., `skills/my-skill`). Use `""` or `"."` for the root. |
| `source.kind` | `string` | `git` or `http`. |
| `source.url` | `string` | URL to the Git repository or HTTP ZIP archive. |
| `source.ref` | `string` | Git branch, tag, or commit SHA (only for `git`). |
| `source.checksum` | `string?` | Optional SHA256 checksum (recommended for `http`). |

---

## CLI Workflows

### 1. Manual Add (Advanced)
If you know the exact source details, you can add an entry manually:

```bash
# Add a skill from a specific subpath in a Git repo
macc catalog skills add \
  --id my-skill \
  --name "My Skill" \
  --description "A custom skill" \
  --kind git \
  --url "https://github.com/user/repo.git" \
  --reference "main" \
  --subpath "path/to/skill"

# Add an MCP server from a ZIP archive
macc catalog mcp add \
  --id brave-search \
  --name "Brave Search" \
  --description "Search the web" \
  --kind http \
  --url "https://example.com/mcp.zip" \
  --checksum "sha256:..."
```

### 2. Import from URL (Recommended for Git)
MACC can automatically parse GitHub tree links to extract the repository URL, reference, and subpath.

```bash
macc catalog import-url \
  --kind skill \
  --id custom-skill \
  --url "https://github.com/org/repo/tree/v1.2.0/skills/target-skill"
```

### 3. Remote Search & Discovery
Search for entries on a remote MACC registry and import them directly.

```bash
# Search for skills related to 'testing'
macc catalog search-remote --kind skill --q "testing"

# Search and add all matches to your local catalog
macc catalog search-remote --kind skill --q "workflow" --add

# Add specific IDs from search results
macc catalog search-remote --kind skill --q "aws" --add-ids "aws-s3,aws-lambda"
```

### 4. Direct Installation
Once an entry is in your catalog, you can install it into your AI tool's directory.

```bash
# Install a skill for Claude Code
macc install skill --tool claude --id my-skill

# Install an MCP server (automatically merges into .mcp.json)
macc install mcp --id brave-search
```

---

## Security Constraints

MACC is designed with safety as a priority:

1. **No Post-Install Scripts**: MACC never executes code downloaded from remote sources. It only materializes files and merges configurations.
2. **Symlink Rejection**: During ZIP extraction or file-walks, MACC explicitly rejects symlinks to prevent directory traversal attacks.
3. **Checksum Verification**: For `http` sources, it is strongly recommended to provide a `checksum` (SHA256). MACC verifies the download before use.
4. **Secret Scanning**: All generated files and merged JSON/YAML outputs are scanned for common secret patterns (API keys, tokens) before being written to disk.
5. **Atomic Writes & Backups**: Every change made by `macc apply` or `macc install` is atomic and backed up in `.macc/backups/`.
6. **MCP template placeholders**: Template definitions listed under `mcp_templates` in `.macc/macc.yaml` may reference commands/arguments and environment variables, but the entries must use placeholder values such as `${BRAVE_API_KEY}` or `YOUR_API_KEY_HERE`. The `auth_notes` field should explain where real secrets must be provided locally; MACC never writes real credentials to disk.
