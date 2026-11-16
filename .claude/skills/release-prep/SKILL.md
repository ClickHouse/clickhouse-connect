---
name: release-prep
description: Prepare a release by creating a release branch, updating the changelog, and bumping the version number. Use when the user wants to cut a new release.
---

# Release Prep

Prepare a release for version `$ARGUMENTS`.

## Steps

### 1. Validate the version argument

The user must supply a version number (e.g. `0.14.0`). If `$ARGUMENTS` is empty or missing, ask the user to provide a version number and stop.

### 2. Ensure you are on the correct release branch

First, switch to `main` and run `git pull` to ensure you have the latest changes. Then, if you are NOT already on `release/$ARGUMENTS`, create the branch `release/$ARGUMENTS` from `main` and switch to it. If the branch already exists remotely or locally, switch to it instead of creating a new one.

### 3. Update the changelog

Open `CHANGELOG.md` and find the `## UNRELEASED` section. Rename it to `## $ARGUMENTS, <today's date in YYYY-MM-DD format>`. Then insert a new empty `## UNRELEASED` heading immediately above the newly renamed section, with a blank line separating them.

The result should look like:

```
## UNRELEASED

## $ARGUMENTS, YYYY-MM-DD

### Breaking Changes
...
```

### 4. Update the version file

Open `clickhouse_connect/_version.py` and update the `version` string to `"$ARGUMENTS"`.

### 5. Generate release notes

Draft GitHub release notes for this release and print them to the console (do NOT publish them, do NOT write them to a file). The user will copy/edit them when they cut the GitHub release.

Tailor the notes to the kind of release this actually is — do not force a one-size-fits-all template. Use the version number and the CHANGELOG entries (the section you just renamed) to decide:

- **Patch / bug-fix release** (e.g. `1.0.1`): short. A one-paragraph "what this fixes" intro, then a `## What's Changed` section with a `### Bug Fixes` list. Link issue/PR numbers where the CHANGELOG mentions them. No migration section needed.
- **Minor release** (e.g. `1.1.0`): "Highlights" section grouping notable changes (Performance / Features / Bug fixes / Other), then a brief installation block. Include a migration note only if there is genuinely user-visible behavior change.
- **Major release** (e.g. `2.0.0`): full structure — intro, upgrading section pointing to `MIGRATION.md` if it exists, Highlights grouped by theme, and a list of any prior RC tags if applicable.
- **Release candidate / pre-release** (e.g. `1.0.0rc1`, `1.1.0a1`): lead with a "this is a release candidate, please test" paragraph and a pinned-version install command. Then highlights / what's changed scoped to this RC.

For inspiration on tone and structure, see existing notes at https://github.com/ClickHouse/clickhouse-connect/releases (e.g. `v1.0.0` for a major release, `v1.0.0rc3` for an RC patch).

Sources of truth for the content:
- The `CHANGELOG.md` section you just renamed — every entry there should be reflected.
- `git log main..HEAD` (or `git log <previous-tag>..HEAD` if you can determine the previous release tag) to catch anything the CHANGELOG missed.

Include an installation block (`pip install clickhouse-connect` for stable, `pip install clickhouse-connect==$ARGUMENTS` for pre-releases). Preserve issue/PR links in the form `[#NNN](https://github.com/ClickHouse/clickhouse-connect/issues/NNN)`.

Print the notes in a fenced markdown block so the user can copy them verbatim.

### 6. Summary

After making the changes, show the user a short summary of what was done:
- The branch you are on
- The changelog update (old heading -> new heading)
- The version bump (old version -> new version)
- That release notes were printed above for review

Do NOT commit the changes. Let the user review and commit when ready.
