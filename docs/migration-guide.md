# Migration Guide

Use this guide to roll out `holywell` across an existing codebase with minimal churn.

## 1) Understand the style model

`holywell` is intentionally opinionated:

- Optional `.holywellrc.json` for operational settings (`maxLineLength`, `maxDepth`, `maxInputSize`, `strict`, `recover`)
- No style toggles (indent/casing/alignment modes)
- Deterministic output

Plan for one-time diffs when first applying formatting.

## 2) Known behavior changes

Before you run `holywell` on existing SQL, understand what will change:

### Keywords become uppercase

All SQL keywords are uppercased. `select` becomes `SELECT`, `inner join` becomes `INNER JOIN`, etc.

### ALL-CAPS identifiers become lowercase

ALL-CAPS unquoted identifiers are lowercased to avoid shouting: `MYTABLE` becomes `mytable`, `USERID` becomes `userid`. Mixed-case identifiers are preserved as-is: `MyTable` stays `MyTable`, `userId` stays `userId`.

**Quoted identifiers are preserved exactly.** `"MyTable"` stays `"MyTable"`.

### Whitespace is normalized

- Leading and trailing whitespace is stripped from every line
- Indentation is replaced with river-aligned formatting
- Blank lines inside statements are removed
- A trailing newline is added at the end of each statement

### Warning: case-sensitive databases

Most databases (PostgreSQL, MySQL, SQL Server) treat unquoted identifiers as case-insensitive, so lowercasing ALL-CAPS identifiers has no effect on query behavior. Mixed-case identifiers are preserved, so this is rarely an issue.

However, if your database or collation is configured to treat unquoted identifiers as case-sensitive (uncommon, but possible in some configurations), the ALL-CAPS lowercasing could change which table or column is referenced. In this case:

1. Use quoted identifiers (`"MyTable"`) for any names that depend on specific casing
2. Or run `holywell --check` first to preview changes before applying `--write`

### What does NOT change

- String literals are preserved exactly (`'Hello World'` stays `'Hello World'`)
- Numeric literals are preserved
- Quoted identifiers are preserved
- Comments are preserved (though their position may shift with reformatting)
- SQL semantics are not altered -- only whitespace and casing change

## 3) Start in check-only mode

Run in CI without writing changes:

```bash
npx holywell --check "**/*.sql"
```

If your repo has generated/vendor SQL, exclude it first:

```bash
npx holywell --check --ignore "vendor/**" --ignore "generated/**" "**/*.sql"
```

Or define ignores once in `.holywellignore`:

```text
vendor/**
generated/**
```

## 4) Batch-format in one commit

Create a dedicated formatting commit:

```bash
npx holywell --write "**/*.sql"
git add -A
git commit -m "style: apply holywell"
```

Keeping formatting separate from feature changes makes review and rollback easier.

## 5) Enforce in CI

After baseline formatting, enforce check mode in CI:

```bash
npx holywell --check "**/*.sql"
```

Useful companion flag for PR logs:

```bash
npx holywell --check --list-different "**/*.sql"
```

## 6) Add pre-commit guard

Run only on staged SQL files:

```bash
npx holywell --check $(git diff --cached --name-only -- '*.sql')
```

Or auto-fix staged files before commit:

```bash
npx holywell --write $(git diff --cached --name-only -- '*.sql')
git add $(git diff --cached --name-only -- '*.sql')
```

## 7) Monorepo rollout strategy

For large repos, migrate package-by-package:

1. Format one domain/folder.
2. Merge.
3. Enable CI check for that scope.
4. Repeat until full coverage.

## 8) Handling unsupported syntax

In CLI recovery mode (default for the CLI), statements that fail structural parsing are preserved as raw SQL where possible. The `formatSQL` API is strict by default.

To make parse failures block CI:

```bash
npx holywell --strict --check "**/*.sql"
```

For custom tooling, use API parse options with `recover: false`.

For a current dialect-by-dialect coverage snapshot, see [SQL Dialect Support](../README.md#sql-dialect-support).
