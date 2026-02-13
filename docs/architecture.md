# Architecture Overview

## Pipeline

1. `src/tokenizer.ts` converts SQL text into typed tokens (`Token[]`). Exports `tokenize`, `TokenizeError`, and token type definitions.
2. `src/parser.ts` builds AST nodes from tokens. Delegates expression parsing to `src/parser/expressions.ts`, DML statements (INSERT, UPDATE, DELETE) to `src/parser/dml.ts`, and DDL statements (CREATE, ALTER, DROP) to `src/parser/ddl.ts`.
3. `src/formatter.ts` renders AST nodes into canonical SQL output with river-aligned keywords. Exports `formatStatements` and `FormatterError`.
4. `src/format.ts` is the public API glue layer (`formatSQL`). Coordinates parsing, dialect resolution, and formatting in a single call.
5. `src/cli.ts` handles file/stdin UX, glob expansion, and error reporting.
6. `src/ast.ts` defines all AST node types (`Statement`, `Expression`, `Node`, etc.).
7. `src/visitor.ts` provides `visitAst`, a schema-agnostic depth-first traversal that treats any object with a string `type` field as a node.
8. `src/dialects/` provides dialect profiles used by the tokenizer and parser:
   - `types.ts` defines `DialectName` (`'ansi' | 'postgres' | 'mysql' | 'tsql'`), `DialectProfile`, and `DialectStatementHandler`.
   - `profiles.ts` contains the built-in profile constants (`ANSI_PROFILE`, `POSTGRES_PROFILE`, `MYSQL_PROFILE`, `TSQL_PROFILE`) and `DIALECT_PROFILES`, a frozen record mapping each `DialectName` to its profile.
   - `resolve.ts` exports `resolveDialectProfile`, which maps a dialect name or custom profile to a resolved `DialectProfile`. When no dialect is specified, `POSTGRES_PROFILE` is the default.
   - `index.ts` is the barrel re-export for the dialects directory. Re-exports types from `types.ts`, profile constants and `DIALECT_PROFILES` from `profiles.ts`, and `resolveDialectProfile` from `resolve.ts`.
   - Only these four profiles are selectable via `--dialect`. Oracle, SQLite, Snowflake, ClickHouse, BigQuery, Exasol, DB2, and H2 syntax is recognized by the tokenizer and parser but handled through these base profiles rather than dedicated dialect configurations.
9. `src/dialect.ts` re-exports public dialect types from `src/dialects/types.ts` and defines the `SQLDialect` union (`DialectName | DialectProfile`).
10. `src/constants.ts` centralizes shared limits: `DEFAULT_MAX_DEPTH`, `TERMINAL_WIDTH`, `DEFAULT_MAX_INPUT_SIZE`, `MAX_TOKEN_COUNT`, `MAX_IDENTIFIER_LENGTH`.
11. `src/keywords.ts` defines `KEYWORD_LIST`, `FUNCTION_KEYWORD_LIST`, and their `Set` counterparts (`KEYWORDS`, `FUNCTION_KEYWORDS`). Also exports `isKeyword`, a case-insensitive lookup that checks membership in the combined `KEYWORDS` set.
12. `src/index.ts` is the package barrel entry point. Re-exports the public API from all modules (`formatSQL`, dialect types and profiles, tokenizer, parser, formatter, visitor, and AST types) and defines the `version` constant (injected at build time by tsup).

## Depth Guards

- Parser and formatter both use the shared `DEFAULT_MAX_DEPTH` (200) from `src/constants.ts`.
- Parser throws `MaxDepthError` (a `ParseError` subclass) when nesting exceeds the limit. `MaxDepthError` always throws, even in recovery mode.
- Formatter throws `FormatterError` when AST traversal exceeds the limit, instead of emitting fallback comment text.

## Recovery Model

- Recovery mode (`recover: true`) preserves unparseable statements as `RawExpression` nodes (with `reason: 'parse_error'`) instead of throwing.
- `onRecover(error, raw, context)` reports recovered statements. `raw` is the `RawExpression` node, or `null` in rare end-of-input cases.
- `onDropStatement(error, context)` reports recovery failures where no raw text can be produced. When omitted, recovery failures throw.
- `onPassthrough(raw, context)` reports statements the parser intentionally does not format because the syntax is recognized but unsupported for structured formatting (e.g. SET, USE, DBCC, CALL). These produce `RawExpression` nodes with `reason: 'unsupported'`. Unlike `onRecover`, this is not triggered by parse errors.
- `ParseRecoveryContext` provides `statementIndex` and `totalStatements` for progress tracking in all three callbacks.

## AST Notes

- The AST favors structured nodes for common SQL constructs and keeps targeted raw fallbacks for unsupported edge syntax.
- `TableElement.raw` remains as a fallback for partially structured DDL constraint bodies. `TableElement` carries optional structured fields (`name`, `dataType`, `columnConstraints`, `checkExpr`, etc.) alongside the raw string.
- `WindowSpec` and `WindowFunctionExpr` intentionally both expose window-clause fields (`partitionBy`, `orderBy`, `frame`, `exclude`) because they model two SQL syntactic sites: the `WINDOW` clause (name + spec) vs inline `OVER (...)` on a function call.
- `RawExpression` nodes carry an optional `reason` tag (`'parse_error'`, `'unsupported'`, `'comment_only'`, `'verbatim'`, `'transaction_control'`, `'trailing_semicolon_comment'`, `'slash_terminator'`) to distinguish why a node was not structurally parsed.
