# Architecture Overview

## Pipeline

1. `src/tokenizer.ts` converts SQL text into typed tokens.
2. `src/parser.ts` builds AST nodes from tokens (with optional recovery to raw passthrough).
3. `src/formatter.ts` renders AST nodes into canonical SQL output.
4. `src/format.ts` is the public API glue layer (`formatSQL`).
5. `src/cli.ts` handles file/stdin UX and error reporting.
6. `src/dialects/` provides dialect profiles (`ansi`, `postgres`, `mysql`, `tsql`) used by tokenizer/parser.

## Depth Guards

- Parser and formatter both use the shared `DEFAULT_MAX_DEPTH` from `src/constants.ts`.
- Parser throws `MaxDepthError` when parsing exceeds the limit.
- Formatter throws `FormatterError` when AST traversal exceeds the limit, instead of emitting fallback comment text.

## Recovery Model

- Recovery mode (`recover: true`) preserves unparseable statements as `raw` nodes where possible.
- `onRecover` reports recovered statements.
- `onDropStatement` reports rare recovery failures where no raw text can be produced; without it, recovery failures throw.

## AST Notes

- The AST favors structured nodes for common SQL constructs and keeps targeted raw fallbacks for unsupported edge syntax.
- `TableElement.raw` remains as a fallback for partially structured DDL constraint bodies.
- `WindowSpec` and `WindowFunctionExpr` intentionally both expose window-clause fields because they model two SQL syntactic sites (`WINDOW` clause vs inline `OVER (...)`).
