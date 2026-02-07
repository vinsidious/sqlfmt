# sqlfmt

An opinionated, zero-config SQL formatter that implements the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/) ([GitHub](https://github.com/treffynnon/sqlstyle.guide)).

## What it does

Takes messy SQL and formats it with **river alignment** — right-aligning keywords so content flows along a consistent vertical column:

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

## Style Guide

This formatter is inspired by and makes every attempt to conform to the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/). Key principles from the guide that `sqlfmt` enforces:

- **River alignment** — Clause/logical keywords are right-aligned to a per-statement river width derived from the longest top-level aligned keyword
- **Keyword uppercasing** — Reserved words like `SELECT`, `FROM`, `WHERE` are uppercased
- **Identifier normalization** — Most unquoted identifiers are lowercased; quoted identifiers are preserved
- **Right-aligned clause/logical keywords** — `SELECT`, `FROM`, `WHERE`, `AND`, `OR`, `JOIN`, `ON`, `ORDER BY`, `GROUP BY`, etc. align within each formatted block
- **Consistent indentation** — Continuation lines and subexpressions are indented predictably

For the full style guide, see [sqlstyle.guide](https://www.sqlstyle.guide/) or the [source on GitHub](https://github.com/treffynnon/sqlstyle.guide).

## Features

- Right-aligned keyword river for clause/logical structure (`SELECT`, `FROM`, `WHERE`, `AND`, `OR`, etc.)
- Smart column wrapping (single-line when short, grouped continuation when long)
- `JOIN` formatting with aligned `ON` / `USING`, including `LATERAL`
- Subquery formatting with context-aware indentation
- `CTE` / `WITH` support, including `RECURSIVE`, `MATERIALIZED` / `NOT MATERIALIZED`, `VALUES`, and `UNION` members
- `CASE` expression formatting (simple, searched, and nested)
- Window function support (`PARTITION BY`, `ORDER BY`, frames, `EXCLUDE`, named `WINDOW` clauses)
- Aggregate extras: `FILTER (WHERE ...)` and `WITHIN GROUP (ORDER BY ...)`
- Grouping extensions: `GROUPING SETS`, `ROLLUP`, `CUBE`
- DML support: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `RETURNING`, `ON CONFLICT`
- DDL/admin support: `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE INDEX`, `CREATE VIEW`, `TRUNCATE`, `GRANT` / `REVOKE`
- Postgres-heavy expression support (casts `::`, arrays, JSON/path operators, regex, `IS [NOT] DISTINCT FROM`, `TABLESAMPLE`, `FETCH FIRST`)
- Comment-aware formatting for line and block comments
- Idempotent — formatting already-formatted SQL produces the same output

## Install

```bash
npm install @vcoppola/sqlfmt
```

## Usage

### As a CLI

```bash
# Format a file
npx sqlfmt query.sql

# Format from stdin
cat query.sql | npx sqlfmt

# Check if a file is already formatted (exits non-zero if not)
npx sqlfmt --check query.sql
```

### As a library

```typescript
import { formatSQL } from '@vcoppola/sqlfmt';

const formatted = formatSQL(`
  select name, email from users where active = true
`);

console.log(formatted);
// SELECT name, email
//   FROM users
//  WHERE active = TRUE;
```

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

## How the formatter works

The pipeline is:

1. **Tokenizer** (`src/tokenizer.ts`) — Splits SQL text into tokens (keywords, identifiers, literals, operators, comments)
2. **Parser** (`src/parser.ts`) — Builds an AST from the token stream
3. **Formatter** (`src/formatter.ts`) — Walks the AST and produces formatted output

The key formatting concept is the **river**. For each statement, `sqlfmt` derives a river width from the longest top-level aligned keyword in that statement (for example, `RETURNING` can widen DML alignment). Clause/logical keywords are then right-aligned to that width so content starts in a consistent column. Nested blocks may use their own derived widths. This approach comes directly from the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/).

## Limitations

- Dialect coverage is broad but intentionally pragmatic, with strongest support for PostgreSQL-style syntax.
- Procedural SQL bodies (`CREATE FUNCTION ... LANGUAGE plpgsql` control-flow blocks, vendor-specific scripting extensions) are not fully parsed as procedural ASTs.
- Unknown/unsupported constructs may fall back to raw statement preservation.
- Formatting style is opinionated and focused on a Holywell-style output rather than per-project style configurability.

## License

MIT
