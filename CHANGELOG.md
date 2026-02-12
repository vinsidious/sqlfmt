# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Examples gallery on the website with dialect-specific SQL samples.
- Dialect selection in the playground UI.
- PostgreSQL ANALYSE and NATURAL JOIN variant parsing.
- Comprehensive multi-dialect keyword and statement support for MySQL and T-SQL.
- Node.js version smoke tests (v18, v20, v22) in CI.

### Changed

- Default API behavior is now strict mode (`recover: false`); CLI still defaults to recovery mode.
- Overhauled formatter engine with runtime context and improved table layout.
- Unified dialect resolution with opt-in formatter fallback.
- Formatter depth limit now throws `FormatterError` instead of emitting fallback comment text.
- Improved CLI robustness, glob handling, and path security.

### Fixed

- UTF-8 byte counting for malformed surrogate sequences.
- Prevented prototype-level mutation of frozen Sets via Proxy.
- Standalone line comment normalization and DDL element alignment.

## [1.8.3] - 2026-02-11

### Added

- `ONLY` keyword support in table references (e.g., `SELECT ... FROM ONLY parent_table`).
- Improved subquery and parenthesized join condition alignment.

### Fixed

- DDL parsing robustness and error recovery for edge-case schemas.
- Formatting stability and indentation handling regressions.
- Function call parsing and keyword identification accuracy.
- Routine body indentation and comment layout logic.
- Formatting idempotency for comment separation edge cases.

## [1.8.2] - 2026-02-10

### Added

- Named table constraint alignment with column data types.
- DROP statement option support (e.g., `CASCADE`, `RESTRICT`).
- Array subscript support in UPDATE SET clauses.
- Index hint and nested CTE query syntax.

### Fixed

- Backslash escape logic for quoted string literals.

## [1.8.1] - 2026-02-10

### Added

- T-SQL OPTION hints and VIEW attribute support.
- Advanced ON CONFLICT syntax and tuple SET assignments.
- VALUES aliases and expression comment handling.
- T-SQL template placeholders and PostgreSQL named arguments (`=>` syntax).
- SQLite INSERT OR conflict resolution actions (e.g., `INSERT OR REPLACE`).
- T-SQL INSERT EXECUTE statement support.

### Fixed

- Division operator disambiguation from statement terminators.
- INSERT INTO keyword validation.
- Empty expression lists in VALUES tuples.
- Parser reliability for statement boundaries and charset introducers.

## [1.8.0] - 2026-02-10

### Added

- Snowflake variant path access and core expression parsing.
- Snowflake identifier and clustered index DDL support.
- REINDEX statement, VALUES subqueries, and table inheritance syntax.
- PostgreSQL INSERT aliases and cursor-based DELETE syntax.
- Routine block formatting with enhanced identifier/comment handling.
- Compound assignment operators and dialect-specific DML syntax.
- T-SQL PRINT statement and ELSE IF control flow.
- Dynamic INTERVAL expression parsing.
- Temporary view and enhanced table creation DDL parsing.
- EXPLAIN syntax enhancements and control flow statement formatting.

### Changed

- Expanded SQL keyword and function coverage across all supported dialects.
- Enhanced core formatting engine and comment management.
- LEFT and RIGHT treated as function keywords for proper casing.

### Fixed

- Backslash escaping logic in string literals.
- Statement boundary detection for ALTER TABLE actions.
- Oracle terminator support and parser boundary detection.
- DDL statement parsing and column constraint formatting.

## [1.7.0] - 2026-02-09

### Added

- MySQL UPDATE JOIN syntax with improved SET layout.
- Expanded dialect support for MySQL, SQLite, and DB2 syntax.

### Changed

- Improved DDL parsing and dialect-specific formatting.
- Enhanced core formatting engine and comment management.
- Enhanced formatting layout and comment preservation.
- Expanded SQL keyword and function coverage.

## [1.6.0] - 2026-02-09

_No user-visible changes. Internal release for version alignment._

## [1.5.0] - 2026-02-09

### Added

- Oracle hierarchical query support (START WITH / CONNECT BY / PRIOR / NOCYCLE).
- PL/SQL block surface syntax handling.
- T-SQL, Oracle, and SQL\*Plus dialect expansion.
- CREATE POLICY statement support.
- CLI encoding detection and token limit configuration.
- Dialect-specific lexical pattern and expression support.

### Changed

- Extended parser and tokenizer for multi-dialect SQL support.
- Improved formatter alignment logic and comment preservation.
- Enhanced DDL and DML parsing for MySQL, PostgreSQL, and T-SQL.
- Expanded playground with diverse SQL samples.

### Fixed

- Parsing errors and idempotency regressions from GitHub corpus validation.
- Multiple SQL parsing and formatting regressions.

## [1.4.0] - 2026-02-08

### Added

- Transaction Control Language support (BEGIN, COMMIT, ROLLBACK, SAVEPOINT).
- Dotted/qualified table identifiers (e.g., `schema.table`) in DDL and DML.
- MySQL-style backtick-quoted identifiers in the tokenizer.
- Inline PRIMARY KEY and UNIQUE constraint support in CREATE TABLE.
- Bare EXPLAIN option parsing (e.g., `EXPLAIN (ANALYZE, BUFFERS)`).
- Qualified column names in UPDATE and MERGE SET clauses (e.g., `alias.column`).
- Website favicons, web manifest, and PWA metadata.

### Changed

- Overhauled website design with new "Emerald" dark theme, modern typography, and animations.
- Redesigned playground with sample query rotation, performance metrics, and improved responsive layout.
- Switched website monospace font to IBM Plex Mono.
- Lowered minimum Node.js requirement to v18.
- Improved formatting for NOT EXISTS clauses and multi-row VALUES statements.
- Expanded keyword dictionary with common PostgreSQL types and transaction keywords.
- Optimized trailing whitespace removal using native `trimEnd` over regex.

### Fixed

- Unterminated dollar-quoted strings now recover gracefully instead of throwing.
- Position checkpointing in IS comparison parsing to prevent incorrect token consumption when used as an alias.

## [1.2.1] - 2026-02-07

### Changed

- Updated release workflow and removed website deployment from CI.

## [1.2.0] - 2026-02-07

### Added

- VS Code extension for holywell formatting with max input size limit and icon.
- Documentation website and interactive playground.
- EXPLAIN statement support.
- Configurable line-length formatting with `lineWidth` option.
- Project-level configuration file support (`.holywellrc`).
- Dry-run mode (`--dry-run`) for CLI.
- `--strict` flag to disable recovery mode for CI usage.
- `--version`/`-v` CLI flag.
- `--write`/`-w` and `--diff` CLI flags.
- Intelligent wrapping for IN lists and array constructors.
- CJK character width awareness in formatting.
- Numeric literal underscores and unicode-escape string support.
- Recovery callback (`onRecover`) in `FormatOptions`.
- DML parser module (INSERT/UPDATE/DELETE/MERGE).
- Expression parser module (comparison/primary expressions).
- Structured AST nodes for complex SQL constructs.
- Comprehensive test suites for tokenizer, parser, CLI, security, and formatting.
- SECURITY.md and architecture documentation.
- Benchmarks.

### Changed

- Formatter depth limit enforced at 200 (falls back to simple formatting on overflow).
- Formatter strict mode with improved error recovery.
- Readonly constraints applied to AST types.
- Unified alias parsing across SELECT, FROM, and subqueries.
- Centralized formatter layout thresholds into policy constants.
- Generalized function/frame formatting with TRIM shorthand support.
- Full query-expression subqueries with parser probing.
- Strict parser expectations with error recovery.
- CTE comment style preservation without AST mutation.
- Expanded parser and tokenizer for broader SQL support.
- Enhanced error reporting and tokenization accuracy.
- Version injection via tsup build.

### Fixed

- IS NOT TRUE/FALSE parsing semantics.
- Escaped quoted identifiers in tokenizer.
- Path traversal and sensitive data leak in CLI.
- Security and robustness improvements in formatting logic.
- Silent consumption of unterminated strings/comments/quoted identifiers.
- Multiple parser and formatter correctness gaps across common SQL dialect patterns.

## [1.1.1] - 2026-02-06

### Fixed

- Dynamic river width derivation for improved formatting of DML statements with wide keywords like `RETURNING`.

## [1.1.0] - 2026-02-06

### Added

- Advanced SQL feature support: `GROUPING SETS`, `ROLLUP`, `CUBE`, `MERGE`, `TABLESAMPLE`, `FETCH FIRST`, `LATERAL`, and more.
- Comprehensive tests for advanced SQL features.

### Changed

- Tokenizer improvements for complex operators and syntax edge cases.
- New AST nodes and keywords for advanced SQL constructs.
- Expanded parser and formatter to handle advanced SQL statements and clauses.

## [1.0.1] - 2026-02-06

### Changed

- Migrated build process to tsup.
- Updated package name to `holywell` for npm publishing.
- Updated installation and import instructions.

## [1.0.0] - 2026-02-06

### Added

- Initial release.
- River-aligned SQL formatting based on the [Simon Holywell SQL Style Guide](https://www.sqlstyle.guide/).
- Tokenizer, parser, and formatter pipeline.
- CLI with `--check`, `--list-different`, `--ignore`, `--color`, glob pattern support.
- Library API with `formatSQL` function.
- Support for SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX, CREATE VIEW, CTEs, window functions, CASE expressions, subqueries, and JOINs.
- PostgreSQL-specific syntax: casts (`::`), arrays, JSON/path operators, regex operators, dollar-quoting.
- Comment-aware formatting for line and block comments.
- `.holywellignore` file support.
- Zero runtime dependencies.

[Unreleased]: https://github.com/vinsidious/holywell/compare/v1.8.3...HEAD
[1.8.3]: https://github.com/vinsidious/holywell/compare/v1.8.2...v1.8.3
[1.8.2]: https://github.com/vinsidious/holywell/compare/v1.8.1...v1.8.2
[1.8.1]: https://github.com/vinsidious/holywell/compare/v1.8.0...v1.8.1
[1.8.0]: https://github.com/vinsidious/holywell/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/vinsidious/holywell/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/vinsidious/holywell/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/vinsidious/holywell/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/vinsidious/holywell/compare/v1.2.1...v1.4.0
[1.2.1]: https://github.com/vinsidious/holywell/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/vinsidious/holywell/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/vinsidious/holywell/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/vinsidious/holywell/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/vinsidious/holywell/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/vinsidious/holywell/releases/tag/v1.0.0
