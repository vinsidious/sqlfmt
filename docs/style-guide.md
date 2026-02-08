# Style Guide Rule Mapping

How holywell maps to the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/).

## Enforced Rules

These rules are automatically applied by holywell on every format.

| Rule | Description |
|------|-------------|
| Right-aligned keywords | Clause keywords (`SELECT`, `FROM`, `WHERE`, `AND`, `OR`, `JOIN`, etc.) are right-aligned to a per-statement river width |
| Uppercase keywords | All SQL reserved words are uppercased (`SELECT`, `FROM`, `WHERE`, `INSERT`, etc.) |
| Lowercase identifiers | Unquoted identifiers (table names, column names) are lowercased; quoted identifiers are preserved as-is |
| Trailing commas | Column lists use trailing commas with continuation lines indented past the river |
| No trailing whitespace | All trailing whitespace is stripped from every line |
| Consistent indentation | Continuation lines, subexpressions, and nested blocks are indented predictably relative to the river |
| JOIN alignment | `JOIN`, `INNER JOIN`, `LEFT JOIN`, etc. are right-aligned with `ON`/`USING` clauses properly indented |
| Subquery indentation | Subqueries are indented with their own derived river width |
| CTE formatting | `WITH` / `AS` blocks are formatted with aligned clause keywords inside each CTE body |
| CASE expression formatting | `WHEN`/`THEN`/`ELSE`/`END` are consistently indented within CASE blocks |
| Semicolon termination | Statements are terminated with semicolons |

## Partially Supported Rules

These rules are addressed but may not cover all edge cases.

| Rule | Description | Notes |
|------|-------------|-------|
| Operator spacing | Spaces around comparison and arithmetic operators (`=`, `<>`, `+`, `-`, etc.) | Handled for most operators; some PostgreSQL-specific operators (e.g., `@>`, `?|`) are spaced but not configurable |
| Line length | Long expressions are wrapped to multiple lines | Wrapping is driven by column count heuristics rather than a strict character limit; no configurable max line width |
| Alignment of VALUES | INSERT ... VALUES rows are formatted | Multi-row VALUES lists are formatted but column alignment across rows is not guaranteed |

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
|-----------|----------------|-----------|
| River width is per-statement | Each statement derives its own river width from its longest aligned keyword rather than using a fixed column | Produces tighter output for simple queries while accommodating wide keywords like `RETURNING` in DML |
| Keyword uppercasing scope | holywell uppercases all recognized SQL keywords, including function-like keywords (`COALESCE`, `NULLIF`, `EXTRACT`, etc.) | Consistent treatment of all reserved words |
| Boolean literals | `TRUE`, `FALSE`, and `NULL` are uppercased | Treated as keywords for consistency, matching the guide's general uppercase-keywords rule |
