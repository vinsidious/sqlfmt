# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Refreshed website design and layout for improved visual hierarchy.
- Enhanced playground with sample queries and improved layout.
- Added website favicons and web manifest.

## [1.2.1] - 2026-02-07

### Changed

- Updated release workflow and removed website deployment from CI.

## [1.2.0] - 2026-02-07

### Added

- VS Code extension for sqlfmt formatting with max input size limit and icon.
- Documentation website and interactive playground.
- EXPLAIN statement support.
- Configurable line-length formatting with `lineWidth` option.
- Project-level configuration file support (`.sqlfmtrc`).
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
- Updated package name to `@vcoppola/sqlfmt` for npm publishing.
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
- `.sqlfmtignore` file support.
- Zero runtime dependencies.

[Unreleased]: https://github.com/vinsidious/sqlfmt/compare/v1.2.1...HEAD
[1.2.1]: https://github.com/vinsidious/sqlfmt/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/vinsidious/sqlfmt/compare/v1.1.1...v1.2.0
[1.1.1]: https://github.com/vinsidious/sqlfmt/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/vinsidious/sqlfmt/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/vinsidious/sqlfmt/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/vinsidious/sqlfmt/releases/tag/v1.0.0
