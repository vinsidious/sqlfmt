# Contributing

## Prerequisites

- Node.js 18+
- Bun (latest stable)

## Setup

```bash
bun install
```

## Running tests locally

```bash
# Run the full test suite
bun test

# Run a specific test file
bun test tests/formatter.test.ts

# Run tests matching a name pattern
bun test -t "window function"

# Type check (no emit)
bun run check

# Build the dist output
bun run build
```

Run all three checks before opening a PR:

```bash
bun run check && bun test && bun run build
```

## How the pipeline works

holywell processes SQL through three stages:

1. **Tokenizer** (`src/tokenizer.ts`) -- Splits raw SQL text into a flat array of tokens: keywords, identifiers, literals, operators, comments, and whitespace.
2. **Parser** (`src/parser.ts`, with sub-modules in `src/parser/`) -- Consumes the token array and produces an AST (abstract syntax tree). Each SQL statement becomes a tree of typed nodes (`SelectStatement`, `JoinClause`, `CaseExpr`, etc.). Sub-modules handle specific grammar areas: `expressions.ts`, `dml.ts`, and `ddl.ts`.
3. **Formatter** (`src/formatter.ts`) -- Walks the AST and emits formatted text. This stage derives the river width, right-aligns keywords, and handles indentation.

When adding new features, changes typically flow through all three stages.

## Adding a new SQL feature

Use this walkthrough as a guide when adding support for a new SQL construct.

### 1. Add AST node(s)

Define the new node type(s) in the AST type definitions (`src/ast.ts`). Each node should have a `type` field and contain only the information needed for formatting.

### 2. Add parser rule(s)

Add parsing logic in the parser (`src/parser.ts` or the appropriate sub-module in `src/parser/`). The parser consumes tokens and returns AST nodes. Follow the existing pattern of `parse*` functions (e.g., `parseSelect`, `parseJoin`, `parseCTE`, `parseFetchClause` in the main parser; `parseInsertStatement`, `parseCreateStatement`, `parseAlterStatement` in sub-modules).

### 3. Add formatter case(s)

Handle the new node type in the formatter (`src/formatter.ts`). The formatter switch/if-chain dispatches on `node.type` -- add a case for your new node and emit the formatted output.

### 4. Add tests

Add test cases in the appropriate test file under `tests/`.

- **Core test files** cover broad areas: `formatter.test.ts`, `parser.test.ts`, `tokenizer.test.ts`, `cli.test.ts`, `idempotency.test.ts`, and `regressions.test.ts`.
- **Feature-specific test files** are named after the SQL behavior they cover (e.g., `mixed-join-alignment.test.ts`, `cte-leading-comment-idempotency.test.ts`, `postgresql-insert-on-conflict-returning-alignment.test.ts`).
- Add to an existing file when your change extends or fixes behavior already tested there. Create a new file when adding a distinct feature or dialect-specific behavior that doesn't fit an existing file.

Each test should include:

- An input SQL string (messy/unformatted)
- The expected formatted output
- Edge cases and variations

### Example

If adding support for `LATERAL JOIN`:

1. **AST**: Ensure `JoinClause` has a `lateral` flag or a `LateralJoin` node type
2. **Parser**: Recognize `LATERAL` before `JOIN` in `parseJoin` and set the flag
3. **Formatter**: When emitting a join with the lateral flag, include `LATERAL` in the output
4. **Tests**: Add cases for `LATERAL JOIN`, `LEFT LATERAL JOIN`, etc.

## Development guidelines

- Keep formatter behavior idempotent (`formatSQL(formatSQL(x)) === formatSQL(x)`).
- Add tests for every behavior change and bug fix.
- Prefer explicit parser/tokenizer errors over silent fallback when input is malformed.
- Keep public API changes backward compatible when possible.

## Pull requests

- Include a clear summary of the problem and fix.
- Include tests demonstrating failures before the fix and passing after.
- Update `CHANGELOG.md` for user-visible changes.
