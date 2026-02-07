# sqlfmt Public Release Readiness Plan

Findings from 5 independent code reviews (code reviewer, security reviewer, tech lead, code simplifier, UX reviewer). Items are ordered by priority within each phase.

---

## Phase 1: Critical (Blocks Public Release)

These cause **silent data corruption** -- formatted SQL is semantically different from input with no error.

### Tokenizer Gaps

- [x] **Dollar-quoted strings completely broken** (5/5 agents)
  - `src/tokenizer.ts` -- `$` is consumed as an unknown character
  - `$$body$$` and `$tag$...$tag$` produce garbage output
  - Extremely common in PostgreSQL function bodies, triggers, DO blocks
  - Fix: Add dollar-quote detection before the unknown-character fallback. Match `$$` or `$identifier$` as opening delimiters, scan for matching closing delimiter, emit as a string token.

- [x] **Positional parameters ($1, $2) broken** (3/5 agents)
  - `src/tokenizer.ts` -- `$1` becomes two tokens (`$` + `1`), output: `$ 1`
  - Breaks every parameterized query
  - Fix: Recognize `$` followed by digits as a single parameter token (new type `'parameter'` or treat as identifier).

- [x] **Scientific notation numbers broken** (5/5 agents)
  - `src/tokenizer.ts:97-106` -- `1e5` tokenizes as number `1` + identifier `e5`
  - Formatter outputs `1 AS e5` -- silently changes SQL semantics
  - Fix: After consuming integer/decimal portion, check for `[eE][+-]?[0-9]+` suffix.

- [x] **E-string escape syntax broken** (4/5 agents)
  - `src/tokenizer.ts` -- `E'\n\t'` splits into identifier `E` + string `'\n\t'`
  - PostgreSQL escape strings are silently mangled
  - Fix: Before standard string literal case, check for `E`/`e` immediately followed by `'`. Tokenize `E'...'` as a single string token, handling `\` escapes within.

- [x] **B-string and X-string literals broken** (4/5 agents)
  - `src/tokenizer.ts` -- `B'1010'` and `X'FF'` split into identifier + string
  - Fix: Check for `B`/`b`/`X`/`x` immediately followed by `'`. Tokenize as single string token.

### Error Handling

- [x] **Unterminated strings/comments/identifiers silently consumed** (5/5 agents)
  - `src/tokenizer.ts:52-93` -- An unterminated `'string`, `/* comment`, or `"identifier` silently eats rest of file
  - No error, no warning, silent data loss
  - Fix: After each while loop, check if `pos >= len` without finding closing delimiter. Either throw a tokenization error or emit a diagnostic token.

- [x] **CLI crashes with raw stack traces** (5/5 agents)
  - `src/cli.ts:4-43` -- No try/catch in `main()`
  - Malformed SQL or missing files produce opaque Node.js stack traces
  - Fix: Wrap `main()` body in try/catch. Catch `ParseError` with position context. Catch `ENOENT`/`EISDIR` from `readFileSync` with clean message. Exit code 2 for parse errors, 1 for check failures.

---

## Phase 2: High (Should Fix Before Release)

Causes incorrect output for valid SQL patterns or represents major quality gaps.

### Parser Safety

- [x] **Parser `advance()` can return undefined** (5/5 agents)
  - `src/parser.ts:2407-2411` -- No bounds check; past-EOF returns `undefined` typed as `Token`
  - Fix: Add guard: `if (this.pos >= this.tokens.length) throw new ParseError('more input', this.peek());`

### Missing SQL Syntax

- [x] **Multi-word type names unsupported** (1/5 agents, but critical correctness)
  - `src/parser.ts` `consumeTypeNameToken()` -- only consumes single token
  - `DOUBLE PRECISION`, `CHARACTER VARYING`, `TIMESTAMP WITH TIME ZONE` all break CAST/column definitions
  - Fix: After first word, look ahead for known continuations: `PRECISION`, `VARYING`, `WITH TIME ZONE`, `WITHOUT TIME ZONE`, etc.

- [x] **FOR UPDATE / FOR SHARE not parsed** (2/5 agents)
  - `src/parser.ts` `parseSelect()` -- locking clauses become separate raw statements
  - `SELECT * FROM t FOR UPDATE` gets a spurious semicolon after `t`
  - Fix: Add `FOR` detection after FETCH/OFFSET in `parseSelect()`, store as field on `SelectStatement`.

- [x] **ORDER BY ... NULLS FIRST/LAST not supported** (2/5 agents)
  - `src/parser.ts` `parseOrderByItem()` -- `NULLS LAST` becomes a separate statement
  - Fix: After consuming `ASC`/`DESC`, check for `NULLS` followed by `FIRST` or `LAST`.

- [x] **DELETE ... USING not supported** (1/5 agents)
  - `src/parser.ts` `parseDelete()` -- PostgreSQL's USING clause breaks DELETE parsing
  - Fix: Add `USING` clause support after FROM in `parseDelete()`.

- [x] **INSERT ... DEFAULT VALUES not supported** (1/5 agents)
  - `src/parser.ts` `parseInsert()` -- `DEFAULT VALUES` not recognized
  - Fix: Check for `DEFAULT VALUES` before the `VALUES` keyword check.

- [x] **UNION DISTINCT / INTERSECT ALL not supported** (1/5 agents)
  - `src/parser.ts` `consumeUnionKeyword()` -- only `UNION ALL` handled
  - Fix: Extend to consume `ALL` or `DISTINCT` after any set operator.

- [x] **INSERT ... SELECT doesn't support UNION in subquery** (1/5 agents)
  - `src/parser.ts:1524` -- uses `parseSelect()` instead of `parseUnionOrSelect()`
  - Fix: Use `parseUnionOrSelect()`.

### Testing Foundation

- [x] **Add unit tests for tokenizer** (5/5 agents)
  - Create `tests/tokenizer.test.ts`
  - Test each token type, boundary conditions, edge cases (unterminated strings, Unicode, operators, empty input, comment-only input)

- [x] **Add unit tests for parser** (5/5 agents)
  - Create `tests/parser.test.ts`
  - Test AST shape assertions, expression precedence, error recovery

- [x] **Add error-path / malformed SQL tests** (5/5 agents)
  - Create `tests/errors.test.ts`
  - Test: unterminated strings, missing parens, invalid syntax, empty statements, deeply nested expressions, random non-SQL text

- [x] **Unicode identifier support** (5/5 agents)
  - `src/tokenizer.ts:321` -- `/[a-zA-Z_]/` excludes accented chars, CJK, etc.
  - Fix: Use `/[\p{L}_]/u` and `/[\p{L}\p{N}_]/u` for identifier start/continuation.

---

## Phase 3: Medium (Recommended Before Wide Adoption)

### Formatting Consistency

- [x] **RETURNING not river-aligned** (4/5 agents)
  - `src/formatter.ts:1110-1112, 1162-1163, 1188-1189` -- width calculation includes RETURNING but it's not right-aligned
  - Fix: Use `rightAlign('RETURNING', dmlCtx)` instead of bare `'RETURNING '`.

- [x] **Function names uppercased aggressively** (1/5 agents)
  - `src/formatter.ts:1739` -- ALL function names uppercased, including user-defined functions
  - Fix: Only uppercase names in `FUNCTION_KEYWORDS`; leave others in original case or lowercase.

- [x] **FROM alias not lowercased consistently** (1/5 agents)
  - `src/formatter.ts:660` -- `FROM t AS T` stays uppercase while `SELECT x AS T` becomes `AS t`
  - Fix: Apply `lowerIdent` to FROM/JOIN aliases.

- [x] **Star qualifier not lowercased** (1/5 agents)
  - `src/formatter.ts:1673` -- `T.*` stays uppercase
  - Fix: Apply `lowerIdent` to `expr.qualifier`.

### Parser Robustness

- [x] **TRUNCATE requires TABLE keyword** (5/5 agents)
  - `src/parser.ts:1912` -- PostgreSQL allows `TRUNCATE table_name` without TABLE
  - Fix: Make TABLE optional.

- [x] **DROP only supports TABLE** (5/5 agents)
  - `src/parser.ts:2125` -- DROP INDEX, VIEW, SCHEMA, FUNCTION all fail
  - Fix: Check keyword after DROP and dispatch, or use generic handler.

- [x] **ALTER only supports TABLE** (2/5 agents)
  - `src/parser.ts:2098` -- ALTER INDEX, VIEW, SEQUENCE all fail
  - Fix: Similar to DROP.

- [x] **Hex numeric literals not parsed** (2/5 agents)
  - `src/tokenizer.ts:97` -- `0xFF` becomes number `0` + identifier `xFF`
  - Fix: Check for `0x`/`0X` prefix in number parsing.

- [x] **No ESCAPE clause support for LIKE/ILIKE** (1/5 agents)
  - Fix: After parsing LIKE pattern, check for `ESCAPE` keyword.

- [x] **No stack overflow protection for deep nesting** (2/5 agents)
  - `src/parser.ts` + `src/formatter.ts` -- recursive descent with no depth limit
  - Fix: Add depth counter with configurable max (e.g., 100).

### CLI UX

- [x] **No `--version` flag** (5/5 agents)
  - Fix: Read version from package.json, add `--version` / `-v` handler.

- [x] **`--check` comparison is fragile** (3/5 agents)
  - `src/cli.ts:32` -- `input.trimEnd() + '\n' === output` fails for leading whitespace
  - Fix: Compare `formatSQL(input) === formatSQL(formatSQL(input))` or normalize both sides identically.

- [x] **No `--write` / `-w` flag for in-place formatting** (4/5 agents)
  - Fix: Add flag that writes formatted output back to input file.

- [x] **Unknown flags silently treated as filenames** (3/5 agents)
  - `src/cli.ts:18` -- `--foo` tries to open file named `--foo`
  - Fix: Reject arguments starting with `-` that are not recognized flags.

- [x] **Help text too minimal** (4/5 agents)
  - `src/cli.ts:13-16` -- No examples, no version, no link to docs
  - Fix: Expand with description, examples, exit codes, link to style guide.

- [x] **No `--diff` flag** (4/5 agents)
  - Fix: Show unified diff when `--check` fails.

- [x] **Multiple file arguments silently uses last one** (1/5 agents)
  - `src/cli.ts:18` -- only last `filePath` assignment wins
  - Fix: Support multiple files or error on more than one positional arg.

### API Design

- [x] **Public API exports only `formatSQL`** (4/5 agents)
  - `src/index.ts:1` -- No access to tokenizer, parser, AST types, or ParseError
  - Fix: Export at minimum: `tokenize`, `Parser`/`parse`, AST types, `ParseError`.

- [x] **No options parameter for future extensibility** (2/5 agents)
  - `src/format.ts` -- Signature is `formatSQL(input: string): string`
  - Fix: Add optional `options?: FormatOptions` to avoid future breaking change.

- [x] **No JSDoc on public `formatSQL` function** (5/5 agents)
  - `src/format.ts:5` -- No documentation on parameters, return value, error behavior
  - Fix: Add JSDoc with description, params, return, examples, throws.

### Code Quality

- [x] **`isPartOfBetween()` always returns false -- dead code** (5/5 agents)
  - `src/parser.ts:693-695` -- Called in `parseAnd()` but always returns false
  - Fix: Remove method and check. Add comment explaining BETWEEN's AND is consumed at comparison level.

- [x] **`fmtExprForRaw` default uses `as any` -- no exhaustive check** (4/5 agents)
  - `src/parser.ts:2465` -- Unknown expression types silently produce empty strings
  - Fix: Add explicit cases for all Expression types, or use `assertNever` pattern.

- [x] **`formatNode` has no default case** (2/5 agents)
  - `src/formatter.ts:118-139` -- New node types would silently return `undefined`
  - Fix: Add `default: throw new Error(\`Unknown node type: ${(node as any).type}\`)`.

- [x] **`expectKeyword` is a pass-through adding nothing** (5/5 agents)
  - `src/parser.ts:2421-2423` -- Just delegates to `expect()`
  - Fix: Either add actual keyword-type validation or remove and use `expect()` directly.

- [x] **`isClauseKeywordValue` creates new array per call** (2/5 agents)
  - `src/parser.ts:2359-2368` -- Should be module-level `Set` with `.has()`
  - Fix: `const CLAUSE_KEYWORDS = new Set([...])` at module level.

- [x] **Regex per character in tokenizer inner loops** (5/5 agents)
  - `src/tokenizer.ts:35-36, 97-105, 321-322` -- `/\s/`, `/[0-9]/`, `/[a-zA-Z_]/` per character
  - Fix: Replace with charCode comparisons for hot loops.

- [x] **`RawExpression` type is overloaded** (2/5 agents)
  - `src/ast.ts:397-400` -- NULL, INTERVAL, DATE constructors, and fallback all use same type
  - Fix: Add proper AST nodes for NULL, INTERVAL, DATE/TIME/TIMESTAMP constructors.

- [x] **`fmtExprForRaw` in parser duplicates formatter's `fmtExpr`** (1/5 agents)
  - `src/parser.ts:2431-2467` -- Partial reimplementation that can diverge
  - Fix: Parse POSITION/SUBSTRING/OVERLAY/TRIM into proper AST nodes instead of collapsing to raw text.

### Testing

- [x] **No systematic idempotency test** (3/5 agents)
  - Fix: Add meta-test looping all test inputs verifying `formatSQL(formatSQL(x)) === formatSQL(x)`.

- [x] **No CLI integration tests** (4/5 agents)
  - Fix: Add `tests/cli.test.ts` using subprocess invocation for --check, --help, stdin, file, errors, exit codes.

### Documentation

- [x] **README does not document limitations or scope** (5/5 agents)
  - Fix: Add "Limitations" section covering: PostgreSQL focus, unsupported dialects, dollar-quoting status, no procedural SQL, ASCII-only identifiers (if not fixed).

- [x] **No CHANGELOG** (5/5 agents)
  - Fix: Add CHANGELOG.md with retroactive entries.

- [x] **PLAN.md is internal -- archive before release** (2/5 agents)
  - Fix: Remove or move to docs/.

---

## Phase 4: Low (Refinement)

### Dead Code & Cleanup

- [x] **Unused variable `indent` in `formatAlterTable`** (5/5 agents) -- `src/formatter.ts:1454`
- [x] **Unused variable `createPos` in `parseCreate`** (3/5 agents) -- `src/parser.ts:1659`
- [x] **Duplicate entries in KEYWORDS / FUNCTION_KEYWORDS** (5/5 agents) -- `src/keywords.ts`
- [x] **`Expr` vs `Expression` inconsistent naming** (3/5 agents) -- `src/ast.ts:54-55`
- [x] **`filterTokens` exported but never used** (3/5 agents) -- `src/tokenizer.ts:343-345`
- [x] **`toUpperKeyword` exported but never used** (2/5 agents) -- `src/keywords.ts:73-76`
- [x] **`check()` calls `peek()` twice unnecessarily** (2/5 agents) -- `src/parser.ts:2403-2405`
- [x] **`blankLinesBefore` is parser concern leaking into Token type** (1/5 agents) -- `src/tokenizer.ts:22`

### Build & Config

- [x] **tsconfig `module: CommonJS` vs package.json `type: module`** (5/5 agents) -- Misleading but not broken
- [x] **No `engines` field in package.json** (2/5 agents) -- Add `"engines": { "node": ">=18" }`
- [x] **No `.editorconfig`** (2/5 agents)
- [x] **Release workflow uses Node 24 (not yet released)** (1/5 agents) -- `.github/workflows/release.yml:39`
- [x] **NPM_TOKEN may be missing from release workflow** (2/5 agents) -- `.github/workflows/release.yml:47`
- [x] **No CONTRIBUTING.md** (2/5 agents)
