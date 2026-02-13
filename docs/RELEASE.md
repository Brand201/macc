# Release Process

This project uses Semantic Versioning (`MAJOR.MINOR.PATCH`) and Git tags (`vX.Y.Z`).

## Versioning Rules

- `PATCH`: backward-compatible fixes and minor internal improvements.
- `MINOR`: backward-compatible features.
- `MAJOR`: breaking changes.

## Release Checklist

1. Ensure CI is green on `main`.
2. Update `CHANGELOG.md`:
   - move relevant items from `Unreleased` to a new version section with date.
3. Run quality baseline locally:
   - `make check`
   - `make test-contract`
   - `./automat/tests/run.sh`
4. Bump crate versions where applicable.
5. Commit release prep:
   - `chore(release): vX.Y.Z`
6. Tag:
   - `git tag vX.Y.Z`
   - `git push origin vX.Y.Z`
7. Create GitHub Release:
   - title: `vX.Y.Z`
   - notes: from `CHANGELOG.md` section for `vX.Y.Z` (plus generated notes if desired).

## Hotfix Flow

For urgent fixes:

1. Branch from last release tag.
2. Apply fix + tests.
3. Release as next patch version.
4. Merge back into `main`.

## Changelog Policy

- Keep entries user-visible and action-oriented.
- Group by `Added`, `Changed`, `Fixed`, `Removed`, `Security` where relevant.
- Avoid duplicate bullets between sections.
