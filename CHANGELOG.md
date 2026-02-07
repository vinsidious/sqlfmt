# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- Dedicated tokenizer, parser, error-path, CLI, API, and idempotency tests.
- CLI flags: `--version`/`-v`, `--write`/`-w`, and `--diff`.
- Public API exports for tokenizer/parser types and helpers.
- Formatter options (`FormatOptions`) with depth-limit support.

### Changed

- Tokenizer now supports dollar-quoted strings, positional parameters, scientific notation, hex numerics, prefixed strings, and Unicode identifiers.
- Parser now supports additional SQL syntax and stricter keyword/error handling.
- Formatter now handles additional expression nodes and alignment behaviors.
- CLI error handling and multi-file behavior were improved.

### Fixed

- Silent consumption of unterminated strings/comments/quoted identifiers.
- Multiple parser and formatter correctness gaps across common SQL dialect patterns.

## [1.1.1] - 2026-02-07

### Notes

- Baseline release before public-hardening changes listed in `Unreleased`.
