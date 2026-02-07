# Contributing

## Prerequisites

- Node.js 18+
- Bun (latest stable)

## Setup

```bash
bun install
```

## Local checks

Run all checks before opening a PR:

```bash
bun run check
bun test
bun run build
```

## Development guidelines

- Keep formatter behavior idempotent (`formatSQL(formatSQL(x)) === formatSQL(x)`).
- Add tests for every behavior change and bug fix.
- Prefer explicit parser/tokenizer errors over silent fallback when input is malformed.
- Keep public API changes backward compatible when possible.

## Pull requests

- Include a clear summary of the problem and fix.
- Include tests demonstrating failures before the fix and passing after.
- Update `CHANGELOG.md` for user-visible changes.
