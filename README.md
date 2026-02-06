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

- **River alignment** — Keywords are right-aligned so their last character falls at a consistent column, creating a vertical "river" of whitespace that makes SQL structure easy to scan
- **Keyword uppercasing** — Reserved words like `SELECT`, `FROM`, `WHERE` are uppercased; identifiers are lowercased
- **Right-aligned keywords** — `SELECT`, `FROM`, `WHERE`, `AND`, `OR`, `JOIN`, `ON`, `ORDER BY`, `GROUP BY`, etc. all align to form the river
- **Consistent indentation** — Continuation lines and subexpressions are indented predictably

For the full style guide, see [sqlstyle.guide](https://www.sqlstyle.guide/) or the [source on GitHub](https://github.com/treffynnon/sqlstyle.guide).

## Features

- Right-aligned keyword river (`SELECT`, `FROM`, `WHERE`, `AND`, `OR`, etc.)
- Smart column wrapping (single-line when short, grouped continuation when long)
- Proper `JOIN` formatting with `ON` condition alignment
- Subquery formatting with correct indentation
- `CTE` / `WITH` clause support (including nested CTEs, `VALUES`, `UNION`)
- `CASE` expression formatting (simple, searched, and nested)
- Window function formatting with `PARTITION BY` / `ORDER BY` alignment
- `CREATE TABLE` with column alignment
- `INSERT`, `UPDATE`, `DELETE` formatting
- Comment preservation (line comments and block comments)
- Keyword uppercasing, identifier lowercasing
- Idempotent — formatting already-formatted SQL produces the same output

## Install

```bash
npm install sqlfmt
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
import { formatSQL } from 'sqlfmt';

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

The key formatting concept is the **river** — keywords like `SELECT`, `FROM`, `WHERE`, `AND`, `OR` are right-aligned so their last character falls at column 6 (the length of `SELECT`). This creates a vertical "river" of whitespace that makes the SQL structure easy to scan. This approach comes directly from the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/).

## License

MIT
