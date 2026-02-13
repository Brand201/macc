# Contributing to MACC

Thank you for contributing. This guide defines the minimum quality bar for pull requests.

## Standards

- Use English for code, docs, commit messages, and PR descriptions.
- Keep CLI/TUI/Core tool-agnostic. Do not hardcode vendor names in generic UX paths.
- Prefer reusable, small functions over feature-specific branching in central flows.

## Local Setup

```bash
cargo build --workspace
```

## Quality Baseline

Run these checks before opening a PR.

### Formatting

```bash
make fmt-check
```

### Linting

```bash
make lint
```

### Tests

Run workspace tests:

```bash
make test
```

Run contract tests for registry/tool specs:

```bash
make test-contract
```

Run automation integration tests (coordinator/performer flows):

```bash
./automat/tests/run.sh
```

### Tool-Agnostic Guardrail

MACC enforces a generic UI/UX layer. `cli/`, `tui/`, and `core/` must not contain tool-specific branding in generic paths.

```bash
make check-generic
```

## All Checks

```bash
make check
```

## Pull Request Checklist

- Explain the problem and the chosen approach.
- List user-visible behavior changes (CLI/TUI/automation).
- Include validation commands and results.
- Add/update tests for new behavior.
- Update docs (`README.md`, `MACC.md`, or module docs) when behavior changes.
- Update `CHANGELOG.md` (`Unreleased`) for user-visible changes.

## Release and Compatibility References

- Release strategy: `docs/RELEASE.md`
- Compatibility policy: `docs/COMPATIBILITY.md`
- Security disclosure process: `SECURITY.md`
