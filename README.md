# holywell

[![npm version](https://img.shields.io/npm/v/holywell)](https://www.npmjs.com/package/holywell)
[![npm downloads](https://img.shields.io/npm/dm/holywell)](https://www.npmjs.com/package/holywell)
[![license](https://img.shields.io/npm/l/holywell)](https://github.com/vinsidious/holywell/blob/main/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/vinsidious/holywell/ci.yml?branch=main&label=CI)](https://github.com/vinsidious/holywell/actions)

An opinionated SQL formatter that implements [Simon Holywell's SQL Style Guide](https://www.sqlstyle.guide/). It faithfully applies the guide's formatting rules -- including river alignment, keyword uppercasing, and consistent indentation -- to produce deterministic, readable SQL with minimal configuration.

**[Try it live in your browser at holywell.sh](https://holywell.sh)**


## Why?

The [SQL Style Guide](https://www.sqlstyle.guide/) is an excellent resource for writing readable, maintainable SQL, but no formatter existed to enforce it automatically. holywell was created to fill that gap -- a zero-dependency TypeScript formatter that faithfully implements the guide's river alignment and formatting conventions with minimal configuration.

```sql
SELECT e.name,
       e.salary,
       d.department_name,
       RANK() OVER (PARTITION BY d.department_name
                        ORDER BY e.salary DESC) AS dept_rank
  FROM employees AS e
       INNER JOIN departments AS d
       ON e.department_id = d.id
 WHERE e.start_date >= '2024-01-01'
   AND d.active = TRUE
 ORDER BY d.department_name, dept_rank;
```

> **Disclaimer:** This project is not officially associated with or endorsed by Simon Holywell or sqlstyle.guide. It is an independent, faithful implementation of the SQL formatting rules described in that style guide.

## Quick Start

### Install

Requires Node.js 18 or later.

```bash
npm install holywell
```

### CLI Usage

```bash
# Format a file
npx holywell query.sql

# Format all SQL files
npx holywell "**/*.sql"

# Check formatting (CI mode)
npx holywell --check "**/*.sql"

# Format in place
npx holywell --write "**/*.sql"
```

### Programmatic Usage

```typescript
import { formatSQL } from 'holywell';

const formatted = formatSQL('select id, name from users where active = true;');
// Output:
// SELECT id, name
//   FROM users
//  WHERE active = TRUE;
```

## Table of Contents

- [What it does](#what-it-does)
- [When NOT to use holywell](#when-not-to-use-holywell)
- [SQL Dialect Support](#sql-dialect-support)
- [CLI Reference](#cli-reference)
- [API Guide](#api-guide)
- [How the formatter works](#how-the-formatter-works)
- [Edge Cases & Behavior](#edge-cases--behavior)
- [FAQ](#faq)
- [Documentation](#documentation)
- [Development](#development)
- [Performance](#performance)
- [Limitations](#limitations)
- [License](#license)

## What it does

Takes messy SQL and formats it according to the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/). A key technique from the guide is **river alignment** -- right-aligning keywords so content flows along a consistent vertical column:

```sql
-- Input
select e.name, e.salary, d.department_name from employees as e inner join departments as d on e.department_id = d.department_id where e.salary > 50000 and d.department_name in ('Sales', 'Engineering') order by e.salary desc;

-- Output
SELECT e.name, e.salary, d.department_name
  FROM employees AS e
       INNER JOIN departments AS d
       ON e.department_id = d.department_id
 WHERE e.salary > 50000
   AND d.department_name IN ('Sales', 'Engineering')
 ORDER BY e.salary DESC;
```

### More examples

**Multi-table JOINs:**

```sql
-- Input
select o.id, c.name, p.title, o.total from orders o join customers c on o.customer_id = c.id left join products p on o.product_id = p.id left join shipping s on o.id = s.order_id where o.created_at > '2024-01-01' and s.status = 'delivered' order by o.created_at desc;

-- Output
SELECT o.id, c.name, p.title, o.total
  FROM orders AS o
  JOIN customers AS c
    ON o.customer_id = c.id
       LEFT JOIN products AS p
       ON o.product_id = p.id

       LEFT JOIN shipping AS s
       ON o.id = s.order_id
 WHERE o.created_at > '2024-01-01'
   AND s.status = 'delivered'
 ORDER BY o.created_at DESC;
```

**CTEs (Common Table Expressions):**

```sql
-- Input
with monthly_totals as (select date_trunc('month', created_at) as month, sum(amount) as total from payments group by 1), running as (select month, total, sum(total) over (order by month) as cumulative from monthly_totals) select * from running where cumulative > 10000;

-- Output
  WITH monthly_totals AS (
           SELECT DATE_TRUNC('month', created_at) AS month,
                  SUM(amount) AS total
             FROM payments
            GROUP BY 1),
       running AS (
           SELECT month, total, SUM(total) OVER (ORDER BY month) AS cumulative
             FROM monthly_totals)
SELECT *
  FROM running
 WHERE cumulative > 10000;
```

**Window functions:**

```sql
-- Input
select department, employee, salary, rank() over (partition by department order by salary desc) as dept_rank, salary - avg(salary) over (partition by department) as diff_from_avg from employees;

-- Output
SELECT department,
       employee,
       salary,
       RANK() OVER (PARTITION BY department
                        ORDER BY salary DESC) AS dept_rank,
       salary - AVG(salary) OVER (PARTITION BY department) AS diff_from_avg
  FROM employees;
```

**CASE expressions:**

```sql
-- Input
select name, case status when 'A' then 'Active' when 'I' then 'Inactive' when 'P' then 'Pending' else 'Unknown' end as status_label, case when balance > 10000 then 'high' when balance > 1000 then 'medium' else 'low' end as tier from accounts;

-- Output
SELECT name,
       CASE status
       WHEN 'A' THEN 'Active'
       WHEN 'I' THEN 'Inactive'
       WHEN 'P' THEN 'Pending'
       ELSE 'Unknown'
       END AS status_label,
       CASE
       WHEN balance > 10000 THEN 'high'
       WHEN balance > 1000 THEN 'medium'
       ELSE 'low'
       END AS tier
  FROM accounts;
```

## When NOT to use holywell

- **You need highly configurable style output** -- holywell intentionally does not expose style knobs for indentation strategy, keyword casing, or alignment mode. If you need full style customization, use [sql-formatter](https://github.com/sql-formatter-org/sql-formatter) or [prettier-plugin-sql](https://github.com/JounQin/prettier-plugin-sql).
- **You need full procedural-language AST rewriting** -- holywell formats SQL and supports many procedural/script syntaxes, but it does not build full PL/pgSQL / PL/SQL / T-SQL procedural ASTs for semantic rewrites.
- **You need a language server** -- holywell is a formatter, not a linter or LSP. It does not provide diagnostics, completions, or semantic analysis.

## SQL Dialect Support

holywell ships four selectable dialect profiles: **ansi**, **postgres**, **mysql**, and **tsql**. These profiles control keyword recognition, clause boundaries, and statement-handler behavior during tokenization and parsing. PostgreSQL is the default when no dialect is specified.

Syntax from other database engines (Oracle, SQLite, Snowflake, ClickHouse, BigQuery, Exasol, DB2, H2) may work through the existing profiles -- typically `ansi` or `postgres` -- rather than via dedicated dialect configurations. The tokenizer and parser handle many vendor-specific patterns (hierarchical queries, q-quoted strings, numbered parameters, etc.), but there is no `--dialect oracle` or `--dialect snowflake` flag. If you work primarily with one of these engines, use whichever built-in profile is the closest match and validate the output with `--check`.

| Syntax Family | Status | Notes |
|---|---|---|
| **ANSI SQL core** | Selectable profile (`--dialect ansi`) | Structured parsing/formatting for SELECT/CTE/DML/MERGE, joins, windows, and common DDL |
| **PostgreSQL** | Selectable profile (`--dialect postgres`); default | Casts, arrays, JSON operators, FILTER/WITHIN GROUP, ON CONFLICT, RETURNING, COPY stdin handling |
| **MySQL / MariaDB** | Selectable profile (`--dialect mysql`) | Backticks, LIMIT offset normalization, STRAIGHT_JOIN, RLIKE, INTERVAL, ALTER key actions, FULLTEXT, DELIMITER scripts |
| **SQL Server (T-SQL)** | Selectable profile (`--dialect tsql`) | GO batches, IF/BEGIN/END chains, CROSS APPLY, PIVOT, bracket identifiers, BACKUP/BULK, PRINT, compound assignments |
| Oracle / PL/SQL surface syntax | No dedicated profile; common patterns handled via `ansi`/`postgres` | START WITH/CONNECT BY, q-quoted strings, slash terminators, RETURNING INTO, type declarations, nested table storage |
| SQLite | No dedicated profile; handled via `ansi` | Numbered positional parameters (`?1`, `?2`, ...), INSERT OR conflict actions, plus ANSI-compatible statements |
| Snowflake | No dedicated profile; handled via `ansi` | Variant path access, CREATE STAGE, CREATE FILE FORMAT, CREATE VIEW COMMENT, COPY INTO handling |
| ClickHouse | No dedicated profile; limited coverage via `ansi` | CREATE MATERIALIZED VIEW ... TO ... AS and CREATE TABLE option clauses |
| BigQuery / Exasol / DB2 / H2 | No dedicated profile; limited coverage via `ansi` | Backtick multipart identifiers + SAFE_CAST/TRY_CAST, Lua bracket strings, slash delimiters, MERGE ... VALUES shorthand |
| Client/meta command syntax | First-class passthrough | psql `\` commands/variables, SQL*Plus slash run terminators, MySQL DELIMITER blocks, T-SQL GO separators |

If you rely heavily on vendor extensions not covered by a built-in profile, run `--check` in CI and use `--strict` where parse failures should block merges.

Choose a built-in dialect profile explicitly when formatting:

```typescript
import { formatSQL } from 'holywell';

const formatted = formatSQL(sql, {
  dialect: 'postgres', // ansi | postgres | mysql | tsql
});
```

### PostgreSQL + ANSI (selectable profiles)

- Type casts (`::integer`), JSON operators (`->`, `->>`), dollar-quoting (`$$...$$`)
- Array constructors, window functions, CTEs, LATERAL joins
- ON CONFLICT (UPSERT), RETURNING clauses
- COPY stdin blocks and psql interpolation forms are preserved and formatted safely
- **Note:** procedural bodies are block-aware, but not modeled as full procedural ASTs

### MySQL / MariaDB (selectable profile)

- STRAIGHT_JOIN, RLIKE, INSERT ... VALUE, UPDATE multi-target/join forms
- CREATE/ALTER TABLE options including ENGINE/CHARSET and FULLTEXT/KEY constraints
- DELIMITER script boundaries and conditional comments are preserved

### SQL Server / T-SQL (selectable profile)

- GO separators, IF/BEGIN/END and ELSE IF chains, CROSS APPLY, PIVOT, BACKUP/BULK statements

### Oracle (no dedicated profile)

- Hierarchical queries, q-quoted strings, slash run terminators, DELETE shorthand normalization, RETURNING ... INTO
- Uses `ansi` or `postgres` profile; common Oracle patterns are handled by the tokenizer and parser directly

### Pass-Through Model and Strict Mode

holywell supports statements through a mix of:

- Structured AST parsing (deep formatting)
- Keyword-normalized pass-through for unmodeled statement families
- Verbatim pass-through for client/script commands

`formatSQL` defaults to recovery mode (`recover: true`), passing through unparseable statements. Use `--strict` or `recover: false` when parse failures should halt formatting.

## Style Guide

This formatter implements the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/). Key principles from the guide that holywell enforces:

- **River alignment** -- Clause/logical keywords are right-aligned to a per-statement river width derived from the longest top-level aligned keyword
- **Keyword uppercasing** -- Reserved words like `SELECT`, `FROM`, `WHERE` are uppercased
- **Identifier normalization** -- ALL-CAPS unquoted identifiers are lowercased (e.g., `MYTABLE` becomes `mytable`); mixed-case identifiers like `MyColumn` are preserved; quoted identifiers are unchanged. Projection aliases (column aliases in SELECT) are an exception -- they are preserved as-is even when ALL-CAPS (e.g., `SELECT id AS TOTAL` keeps `TOTAL` unchanged)
- **Right-aligned clause/logical keywords** -- `SELECT`, `FROM`, `WHERE`, `AND`, `OR`, `JOIN`, `ON`, `ORDER BY`, `GROUP BY`, etc. align within each formatted block
- **Consistent indentation** -- Continuation lines and subexpressions are indented predictably

For the full style guide, see [sqlstyle.guide](https://www.sqlstyle.guide/) or the [source on GitHub](https://github.com/treffynnon/sqlstyle.guide).

## Prior Art & Alternatives

If "river alignment" and opinionated defaults aren't your vibe, these tools might be a better fit:

- **[sql-formatter](https://github.com/sql-formatter-org/sql-formatter)** — Configurable TypeScript/JavaScript formatter with indentation-based output and broad dialect support. Good choice when you need fine-grained control over style.
- **[prettier-plugin-sql](https://github.com/JounQin/prettier-plugin-sql)** — SQL formatting via the Prettier ecosystem. Ideal if your team already uses Prettier and wants unified formatting tooling.
- **[pgFormatter](https://github.com/darold/pgFormatter)** — Perl-based PostgreSQL formatter with extensive configuration options. Mature and battle-tested in PostgreSQL-heavy environments.
- **[sqlfluff](https://github.com/sqlfluff/sqlfluff)** — Python-based SQL linter and formatter with 50+ rules and broad dialect support. More than a formatter — it also lints for anti-patterns and style violations.

### Configuration philosophy

holywell keeps style deterministic by design: no indentation/casing style matrix, no formatter presets.
It does support a focused optional config file (`.holywellrc.json`) for operational settings:

- `maxLineLength`
- `maxDepth`
- `maxInputSize`
- `maxTokenCount`
- `dialect`
- `strict`
- `recover`

CLI flags still override config values.

A starter config is available at `.holywellrc.json.example`.

## CLI Reference

```
Usage: holywell [options] [file ...]
```

File arguments support glob patterns (e.g. `**/*.sql`). By default, `holywell query.sql` prints formatted output to **stdout**. Use `--write` to modify files in place.

### Flags

**General:**

| Flag | Description |
|------|-------------|
| `-h, --help` | Show help text |
| `-V, --version` | Show version |

**Formatting:**

| Flag | Description |
|------|-------------|
| `--check` | Exit 1 when input is not formatted |
| `--diff` | Show unified diff when `--check` fails |
| `--dry-run, --preview` | Preview changes without writing (implies `--check --diff`) |
| `-w, --write` | Write formatted output back to input file(s) |
| `-l, --list-different` | Print only filenames that need formatting |
| `--max-line-length <n>` | Preferred output line width (default: 80) |
| `--max-input-size <n>` | Maximum input size in bytes (default: 10 MB) |
| `--max-token-count <n>` | Tokenizer ceiling for very large SQL files |
| `--dialect <name>` | SQL dialect: `ansi`, `postgres`, `mysql`, `tsql` |
| `--strict` | Disable parser recovery; exit 2 on parse errors (recommended for CI) |

**File selection:**

| Flag | Description |
|------|-------------|
| `--ignore <pattern>` | Exclude files matching glob pattern (repeatable) |
| `--config <path>` | Use an explicit config file (default: `.holywellrc.json`) |
| `--stdin-filepath <p>` | Path shown in error messages when reading stdin |

**Output:**

| Flag | Description |
|------|-------------|
| `-v, --verbose` | Print progress details to stderr |
| `--quiet` | Suppress all output except errors |
| `--color <mode>` | Colorize output: `auto` (default), `always`, `never` |
| `--no-color` | Alias for `--color=never` |
| `--completion <shell>` | Print shell completion script (`bash`, `zsh`, `fish`) |

The `--color` flag controls ANSI color output. In `auto` mode (the default), color is enabled when stderr is a TTY and neither `NO_COLOR` nor `CI` environment variables are set. Use `--color=always` to force color (e.g. when piping to a pager), or `--color=never` / `--no-color` to disable it.

### Examples

```bash
# Format a file (prints to stdout by default)
npx holywell query.sql

# Format a file in place
npx holywell --write query.sql

# Format from stdin
cat query.sql | npx holywell

# Check if a file is already formatted (exits non-zero if not)
npx holywell --check query.sql

# List files that would change (useful in CI)
npx holywell --list-different "src/**/*.sql"
npx holywell -l "migrations/*.sql"

# Strict mode: fail on unparseable SQL instead of passing through
npx holywell --strict --check "**/*.sql"

# Select SQL dialect explicitly
npx holywell --dialect postgres --write query.sql

# Tune output width
npx holywell --max-line-length 100 query.sql

# Use project config
npx holywell --config .holywellrc.json --check "**/*.sql"

# Ignore files (can repeat --ignore)
npx holywell --check --ignore "migrations/**" "**/*.sql"

# Or store ignore patterns in .holywellignore (one pattern per line)
npx holywell --check "**/*.sql"

# Generate shell completion
npx holywell --completion bash
npx holywell --completion zsh
npx holywell --completion fish

# Pipe patterns
pbpaste | npx holywell | pbcopy          # Format clipboard (macOS)
pg_dump mydb --schema-only | npx holywell > schema.sql
echo "select 1" | npx holywell
```

When present, `.holywellignore` is read from the current working directory and combined with any `--ignore` flags.

**CLI exit codes:**

| Code | Meaning |
|------|---------|
| `0` | Success (or all files already formatted with `--check`) |
| `1` | Check failure |
| `2` | Parse or tokenize error |
| `3` | Usage or I/O error |

## API Guide

### Basic Usage

```typescript
import { formatSQL } from 'holywell';

const formatted = formatSQL('SELECT * FROM users;');
```

### Synchronous API by design

`formatSQL`, `parse`, and `tokenize` are intentionally synchronous.
This keeps editor/CLI integration predictable and avoids hidden async overhead.

### Error Recovery

`formatSQL` uses recovery mode by default, preserving unparseable statements as raw text. You can hook into recovery events:

```typescript
const warnings: string[] = [];
const formatted = formatSQL(sql, {
  recover: true,
  onRecover: (error, raw, context) => {
    warnings.push(`Line ${error.token.line}: ${error.message}`);
  }
});
```

### Strict Mode (throw on parse errors)

Opt into strict mode to throw on parse errors:

```typescript
import { formatSQL, ParseError } from 'holywell';

try {
  formatSQL(sql, { recover: false });
} catch (err) {
  if (err instanceof ParseError) {
    console.error(`Parse error: ${err.message}`);
  }
}
```

### Depth Limits

```typescript
formatSQL(sql, { maxDepth: 300 }); // Increase for deeply nested CTEs (default: 200)
```

### Input Size Limits

```typescript
formatSQL(sql, { maxInputSize: 5_000_000 }); // 5MB limit (default: 10MB)
```

### Low-Level Access

```typescript
import { tokenize, parse, formatStatements, visitAst } from 'holywell';

// Tokenize SQL into a token stream
const tokens = tokenize(sql);

// Parse SQL into an AST
const ast = parse(sql);

// Format AST nodes back to SQL
const output = formatStatements(ast);

// Visit AST nodes (for custom linting/analysis)
visitAst(ast, {
  byType: {
    select(node) {
      console.log('SELECT node:', node);
    },
  },
});
```

### Error Types

```typescript
import { formatSQL, TokenizeError, ParseError, MaxDepthError, FormatterError } from 'holywell';

try {
  const result = formatSQL(input);
} catch (err) {
  if (err instanceof TokenizeError) {
    // Invalid token encountered during lexing (e.g., unterminated string)
    console.error(`Tokenize error at ${err.line}:${err.column}: ${err.message}`);
  } else if (err instanceof MaxDepthError) {
    // Parser nesting exceeded configured maxDepth
    console.error(`Parse depth exceeded: ${err.message}`);
  } else if (err instanceof ParseError) {
    // Structural error in the SQL (e.g., unmatched parentheses)
    console.error(`Parse error at ${err.line}:${err.column}: ${err.message}`);
  } else if (err instanceof FormatterError) {
    // Error during AST-to-text formatting
    console.error(`Formatter error: ${err.message}`);
  } else if (err instanceof Error && err.message.includes('Input exceeds maximum size')) {
    // Input exceeded maxInputSize
    console.error(`Input too large: ${err.message}`);
  } else {
    throw err;
  }
}
```

### Recovery Callbacks

`FormatOptions` supports three callbacks for observing recovery and passthrough behavior:

```typescript
const warnings: string[] = [];
const formatted = formatSQL(sql, {
  recover: true,

  // Called when the parser falls back to raw passthrough for an unparseable statement
  onRecover: (error, raw, context) => {
    warnings.push(`Line ${error.line}: recovered — ${error.message}`);
  },

  // Called when recovery cannot preserve a statement at all (rare)
  onDropStatement: (error, context) => {
    warnings.push(`Line ${error.line}: dropped — ${error.message}`);
  },

  // Called for statements the parser intentionally does not format (SET, USE, DBCC, etc.)
  onPassthrough: (raw, context) => {
    // Informational — these are not errors
  },

  // Additional FormatOptions fields
  maxTokenCount: 500_000,   // Tokenizer ceiling for very large SQL files
  maxLineLength: 100,       // Preferred output line width
});
```

### Version

The library exports a `version` string constant:

```typescript
import { version } from 'holywell';
console.log(version); // e.g. "1.8.6"
```

### Custom Dialect Profiles

For advanced use cases such as building custom dialect integrations, holywell exports the four built-in dialect profile constants and the resolver function:

```typescript
import {
  resolveDialectProfile,
  ANSI_PROFILE,
  POSTGRES_PROFILE,
  MYSQL_PROFILE,
  TSQL_PROFILE,
} from 'holywell';

// Resolve a dialect name to its profile
const profile = resolveDialectProfile('postgres');
console.log(profile.clauseKeywords); // Set of keywords that start clauses

// Use a profile constant directly
const pgProfile = POSTGRES_PROFILE;
```

The `Parser` class is also exported for callers who need direct access to parser internals beyond the `parse()` convenience function.

Key types for custom dialect work:

| Type | Description |
|------|-------------|
| `SQLDialect` | Union of dialect name strings and `DialectProfile` |
| `DialectProfile` | Full profile object with keyword sets, clause boundaries, and statement handlers |
| `DialectStatementHandler` | Handler function type for dialect-specific statement parsing |

## How the formatter works

```
SQL Text -> Tokenizer -> Parser -> AST -> Formatter -> Formatted SQL
```

1. **Tokenizer** (`src/tokenizer.ts`) -- Splits SQL text into tokens (keywords, identifiers, literals, operators, comments)
2. **Parser** (`src/parser.ts`) -- Builds an AST from the token stream
3. **Formatter** (`src/formatter.ts`) -- Walks the AST and produces formatted output

The key formatting concept is the **river**. For each statement, holywell derives a river width from the longest top-level aligned keyword in that statement (for example, `RETURNING` can widen DML alignment). Clause/logical keywords are then right-aligned to that width so content starts in a consistent column. Nested blocks may use their own derived widths. This approach comes directly from the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/).

## Edge Cases & Behavior

### Long Lines

holywell targets 80-column output by default and supports `maxLineLength` (CLI flag or config file). It does not break individual tokens (identifiers, string literals), so single-token lines can still exceed the configured width.

### Comment Preservation

Line comments and block comments are preserved. Comments attached to specific expressions maintain their association.

### Keyword Casing

All SQL keywords are uppercased. Quoted identifiers keep their case and quotes. ALL-CAPS unquoted identifiers are lowercased to avoid shouting (e.g., `MYTABLE` becomes `mytable`); mixed-case identifiers like `MyColumn` are preserved as-is. Projection aliases are exempt from lowercasing and kept verbatim.

### Idempotency

Formatting is idempotent: `formatSQL(formatSQL(x)) === formatSQL(x)` for all valid inputs.

## FAQ

**Q: Can I change the indentation style or keyword casing?**

No. Style output is intentionally fixed. holywell provides operational configuration (line length, strictness/safety), not style customization.

**Q: What happens with SQL syntax holywell doesn't understand?**

`formatSQL` uses recovery mode by default: unsupported or unparseable statements are passed through as-is. Use `--strict` (CLI) or `recover: false` (API) to throw on parse errors instead.

**Q: How fast is holywell?**

Throughput varies by statement complexity: ~6,000 simple statements/sec or ~5,500 complex statements/sec when formatting individually, and higher in batch mode (multiple statements in a single `formatSQL` call). A typical migration file formats in under 10ms.

**Q: Does holywell modify SQL semantics?**

No. holywell changes whitespace, uppercases SQL keywords, lowercases ALL-CAPS unquoted identifiers (mixed-case identifiers and projection aliases are preserved), and normalizes alias syntax (e.g., inserting AS). Quoted identifiers and string literals are preserved exactly. The semantic meaning is preserved.

**Q: Does holywell respect `.editorconfig`?**

No. holywell does not read `.editorconfig`. It does read `.holywellrc.json` (or `--config`) for operational settings, but style output remains deterministic.

**Q: Can I customize the river width?**

Not directly. River width is derived automatically from statement structure. You can influence wrapping via `maxLineLength`, but keyword alignment behavior itself is fixed.

**Q: Does holywell work with MySQL / SQL Server / SQLite / Oracle?**

MySQL and SQL Server (T-SQL) have dedicated selectable profiles (`--dialect mysql`, `--dialect tsql`) with tailored keyword sets and statement handlers. Oracle and SQLite do not have their own `--dialect` flag, but common syntax patterns from these engines (hierarchical queries, q-quoted strings, numbered parameters, INSERT OR, etc.) are recognized by the tokenizer and parser when using the `ansi` or `postgres` profile. Snowflake, ClickHouse, BigQuery, and others are handled similarly. See [SQL Dialect Support](#sql-dialect-support) for the current matrix.

## Documentation

- [Integrations](docs/integrations.md) -- Pre-commit hooks, CI pipelines, and editor setup recipes
- [Architecture](docs/architecture.md) -- Internal pipeline and design decisions
- [Style Guide Mapping](docs/style-guide.md) -- How holywell maps to each rule in the Simon Holywell SQL Style Guide
- [Migration Guide](docs/migration-guide.md) -- Rolling out holywell in existing codebases with minimal churn
- [Contributing](CONTRIBUTING.md) -- Development setup, running tests, and submitting changes
- [Changelog](CHANGELOG.md) -- Release history

## Development

Requires [Bun](https://bun.sh/).

```bash
# Install dependencies
bun install

# Run tests
bun test

# Type check
bun run check

# Build dist (for npm publishing)
bun run build
```

## Performance

holywell has zero runtime dependencies and formats SQL through a single tokenize-parse-format pass. Throughput varies by statement complexity: ~6,000 simple statements/sec or ~5,500 complex statements/sec when formatting individually, and higher in batch mode. Input is bounded by default size limits to prevent excessive memory use on untrusted input.

## Limitations

- Four selectable dialect profiles are available (ansi, postgres, mysql, tsql). Other database engines are supported through these profiles, with vendor-specific syntax recognized by the tokenizer and parser but without dedicated dialect configurations.
- Procedural SQL bodies and vendor scripting commands are mostly handled via block-aware pass-through, not full procedural AST rewriting.
- Unknown/unmodeled constructs may still fall back to raw statement preservation.
- Formatting style is opinionated and focused on faithfully implementing the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/) rather than per-project style configurability.

## License

MIT
