# sqlfmt

[![npm version](https://img.shields.io/npm/v/@vcoppola/sqlfmt)](https://www.npmjs.com/package/@vcoppola/sqlfmt)
[![npm downloads](https://img.shields.io/npm/dm/@vcoppola/sqlfmt)](https://www.npmjs.com/package/@vcoppola/sqlfmt)
[![license](https://img.shields.io/npm/l/@vcoppola/sqlfmt)](https://github.com/vinsidious/sqlfmt/blob/main/LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/vinsidious/sqlfmt/ci.yml?branch=main&label=CI)](https://github.com/vinsidious/sqlfmt/actions)

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
            GROUP BY 1
       ),
       running AS (
           SELECT month, total, SUM(total) OVER (ORDER BY month) AS cumulative
             FROM monthly_totals
       )
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

## Why sqlfmt?

| | **sqlfmt** | sql-formatter | prettier-plugin-sql |
|---|---|---|---|
| **Formatting style** | River alignment ([sqlstyle.guide](https://www.sqlstyle.guide/)) | Indentation-based | Indentation-based |
| **Configuration** | Zero-config, opinionated | Configurable | Configurable via Prettier |
| **PostgreSQL support** | First-class (casts, JSON ops, dollar-quoting, arrays) | Partial | Partial |
| **Runtime dependencies** | Zero | Several | Prettier + parser |
| **Idempotent** | Yes | Yes | Yes |
| **Keyword casing** | Uppercase (enforced) | Configurable | Configurable |
| **Identifier casing** | Lowercase (enforced) | Not modified | Not modified |
| **Output** | Deterministic, single style | Depends on config | Depends on config |

sqlfmt is the right choice when you want a formatter that produces consistent, readable SQL without any configuration decisions -- just run it and move on.

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

# List files that would change (useful in CI)
npx sqlfmt --list-different "src/**/*.sql"
npx sqlfmt -l "migrations/*.sql"

# Pipe patterns
pbpaste | npx sqlfmt | pbcopy          # Format clipboard (macOS)
pg_dump mydb --schema-only | npx sqlfmt > schema.sql
echo "select 1" | npx sqlfmt
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

## Documentation

- [Integrations](docs/integrations.md) -- Pre-commit hooks, CI pipelines, and editor setup recipes
- [Style Guide Mapping](docs/style-guide.md) -- How sqlfmt maps to each rule in the Simon Holywell SQL Style Guide
- [Contributing](CONTRIBUTING.md) -- Development setup, running tests, and submitting changes

## Error Handling

`formatSQL` throws typed errors that you can catch and handle:

```typescript
import { formatSQL, TokenizeError, ParseError } from '@vcoppola/sqlfmt';

try {
  const result = formatSQL(input);
} catch (err) {
  if (err instanceof TokenizeError) {
    // Invalid token encountered during lexing (e.g., unterminated string)
    console.error(`Tokenize error at position ${err.position}: ${err.message}`);
  } else if (err instanceof ParseError) {
    // Structural error in the SQL (e.g., unmatched parentheses)
    console.error(`Parse error: ${err.message}`);
  } else if (err instanceof RangeError) {
    // Input exceeded size limits
    console.error(`Input too large: ${err.message}`);
  } else {
    throw err;
  }
}
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

## Performance

sqlfmt has zero runtime dependencies and formats SQL through a single tokenize-parse-format pass. Typical throughput is 5,000+ statements per second on modern hardware. Input is bounded by default size limits to prevent excessive memory use on untrusted input.

## Limitations

- Dialect coverage is broad but intentionally pragmatic, with strongest support for PostgreSQL-style syntax.
- Procedural SQL bodies (`CREATE FUNCTION ... LANGUAGE plpgsql` control-flow blocks, vendor-specific scripting extensions) are not fully parsed as procedural ASTs.
- Unknown/unsupported constructs may fall back to raw statement preservation.
- Formatting style is opinionated and focused on a Holywell-style output rather than per-project style configurability.

## License

MIT
