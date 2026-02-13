# Style Guide Rule Mapping

How holywell maps to the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/).

## Enforced Rules

These rules are automatically applied by holywell on every format.

| Rule | Description |
|------|-------------|
| Right-aligned keywords | Clause keywords (`SELECT`, `FROM`, `WHERE`, `AND`, `OR`, etc.) are right-aligned to a per-statement river derived from the widest aligned keyword in that statement |
| Uppercase keywords | All SQL reserved words and function keywords are uppercased (`SELECT`, `FROM`, `WHERE`, `INSERT`, `COALESCE`, `NULLIF`, `EXTRACT`, `CAST`, `COUNT`, `SUM`, etc.) |
| Boolean and NULL literals | `TRUE`, `FALSE`, and `NULL` are uppercased, treated as keywords |
| Identifier normalization | ALL-CAPS unquoted identifiers are lowercased (e.g., `MYTABLE` becomes `mytable`); mixed-case identifiers like `MyColumn` are preserved; quoted identifiers are unchanged. Exception: projection aliases (column aliases in `SELECT`) are never lowercased, so `SELECT id AS TOTAL` keeps `TOTAL` as-is |
| Trailing commas | When column lists wrap to multiple lines, commas are placed in trailing position (end of line) rather than leading position; continuation lines are aligned with the first item after the keyword |
| No trailing whitespace | All trailing whitespace is stripped from every line |
| Consistent indentation | Continuation lines, subexpressions, and nested blocks are indented relative to the river |
| JOIN formatting | Bare `JOIN` is right-aligned to the river alongside `FROM`; modified joins (`INNER JOIN`, `LEFT JOIN`, `CROSS JOIN`, etc.) are indented past the river; `ON` and `USING` clauses are indented under their join |
| Subquery indentation | Subqueries are formatted with their own derived river width |
| CTE formatting | `WITH` / `AS` blocks are formatted with aligned clause keywords inside each CTE body |
| CASE expression formatting | `WHEN`/`THEN`/`ELSE`/`END` are consistently indented within CASE blocks |
| Semicolon termination | Every statement is terminated with a semicolon; missing semicolons are added |
| Deterministic and idempotent | Formatting the same input always produces the same output; formatting already-formatted output produces no changes |

## Partially Supported Rules

These rules are addressed but may not cover all edge cases.

| Rule | Description | Notes |
|------|-------------|-------|
| Operator spacing | Spaces around comparison and arithmetic operators (`=`, `<>`, `+`, `-`, etc.) | Handled for most operators; some dialect-specific operators (e.g., PostgreSQL `@>`, `?|`) are spaced but not configurable |
| Line length | Long expressions are wrapped to respect `maxLineLength` (default: 80) | Wrapping uses heuristics; very long single tokens may exceed the configured width |
| Alignment of VALUES | `INSERT ... VALUES` rows are formatted with alignment | Multi-row VALUES lists are formatted but column alignment across rows is not guaranteed |

## Not Enforced

These guidelines from sqlstyle.guide are not enforced by holywell because they involve semantic or naming decisions that a formatter cannot reliably make.

| Rule | Description | Reason |
|------|-------------|--------|
| Avoid `SELECT *` | Use explicit column lists | Semantic decision; the formatter preserves `SELECT *` as-is |
| Table/column naming conventions | Avoid abbreviations, use snake_case, avoid reserved words as names | Naming is a design decision, not a formatting concern |
| Correlation name rules | Use first letters of table name, avoid `AS` for column aliases | Naming convention; holywell preserves user-chosen aliases |
| Uniform suffixes | Use `_id`, `_status`, `_date`, etc. | Schema design convention |
| Query structure guidelines | Avoid vendor-specific syntax, use ANSI joins | holywell formats whatever syntax is used, including vendor extensions |

## Intentional Deviations

holywell makes a small number of deliberate choices that differ from or extend sqlstyle.guide:

| Deviation | holywell behavior | Rationale |
|-----------|-------------------|-----------|
| River width is per-statement | Each statement derives its own river width from its longest aligned keyword rather than using a fixed column | Produces tighter output for simple queries while accommodating wide keywords like `RETURNING` in DML |
| Keyword uppercasing scope | holywell uppercases all recognized SQL keywords, including function-like keywords (`COALESCE`, `NULLIF`, `EXTRACT`, `CAST`, `COUNT`, etc.) and aggregate functions (`SUM`, `AVG`, `MAX`, `MIN`) | Consistent treatment of all reserved words |
