# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.3.0] - 2026-02-08

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

[Unreleased]: https://github.com/vinsidious/holywell/compare/v1.3.0...HEAD
[1.3.0]: https://github.com/vinsidious/holywell/compare/v1.2.1...v1.3.0
[1.2.1]: https://github.com/vinsidious/holywell/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/vinsidious/holywell/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/vinsidious/holywell/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/vinsidious/holywell/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/vinsidious/holywell/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/vinsidious/holywell/releases/tag/v1.0.0
