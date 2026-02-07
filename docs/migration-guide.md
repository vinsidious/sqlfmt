# Migration Guide

Use this guide to roll out `sqlfmt` across an existing codebase with minimal churn.

## 1) Understand the style model

`sqlfmt` is intentionally opinionated:

- No `.sqlfmtrc`
- No style toggles
- Deterministic output

Plan for one-time diffs when first applying formatting.

## 2) Start in check-only mode

Run in CI without writing changes:

```bash
npx sqlfmt --check "**/*.sql"
```

If your repo has generated/vendor SQL, exclude it first:

```bash
npx sqlfmt --check --ignore "vendor/**" --ignore "generated/**" "**/*.sql"
```

Or define ignores once in `.sqlfmtignore`:

```text
vendor/**
generated/**
```

## 3) Batch-format in one commit

Create a dedicated formatting commit:

```bash
npx sqlfmt --write "**/*.sql"
git add -A
git commit -m "style: apply sqlfmt"
```

Keeping formatting separate from feature changes makes review and rollback easier.

## 4) Enforce in CI

After baseline formatting, enforce check mode in CI:

```bash
npx sqlfmt --check "**/*.sql"
```

Useful companion flag for PR logs:

```bash
npx sqlfmt --check --list-different "**/*.sql"
```

## 5) Add pre-commit guard

Run only on staged SQL files:

```bash
sqlfmt --check $(git diff --cached --name-only -- '*.sql')
```

Or auto-fix staged files before commit:

```bash
sqlfmt --write $(git diff --cached --name-only -- '*.sql')
git add $(git diff --cached --name-only -- '*.sql')
```

## 6) Monorepo rollout strategy

For large repos, migrate package-by-package:

1. Format one domain/folder.
2. Merge.
3. Enable CI check for that scope.
4. Repeat until full coverage.

## 7) Handling unsupported syntax

In recovery mode (default), unknown constructs are preserved as raw SQL where possible. If you need strict parse failures, use API parse options with `recover: false` in custom tooling.

