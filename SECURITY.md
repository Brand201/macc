# Security Policy

## Supported Versions

Security fixes are applied to the latest released minor line.

| Version | Supported |
|---------|-----------|
| latest  | yes       |
| older   | no        |

## Reporting a Vulnerability

Please do not open a public issue for suspected security vulnerabilities.

Send a private report with:

- affected version / commit,
- impact summary,
- reproduction steps,
- optional patch suggestion.

Contact maintainers via repository security advisories (preferred) or maintainer email listed in project metadata.

## Response Targets

- Initial acknowledgment: within 3 business days.
- Triage decision: within 7 business days.
- Fix and release target: depends on severity and complexity.

## Scope

Security-sensitive areas include:

- file writes/merges and user-scope operations,
- backup/restore flows,
- remote catalog/fetch handling (git/http),
- automation scripts (coordinator/performer),
- secrets handling and logging.
