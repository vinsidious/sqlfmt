# Robustness Improvement Plan

- [ ] 1. Replace subquery parsing that only accepts bare `SELECT` with a full query-expression parser (`SELECT | UNION | WITH`), including nested forms.
- [x] 2. Make parser expectations strict (`expect`/`expectKeyword`) with explicit parse errors and statement-level recovery instead of silent token drift.
- [x] 3. Fix `IS NOT TRUE` / `IS NOT FALSE` semantics so they are not rewritten as `IS NOT NULL`.
- [x] 4. Fix tokenizer support for escaped quoted identifiers (`""`) so identifiers like `"a""b"` are parsed correctly.
- [ ] 5. Remove brittle subquery lookahead heuristics and replace with checkpoint/rollback query probing.
- [x] 6. Eliminate `as any` alias-column plumbing by adding typed AST fields for alias column lists.
- [ ] 7. Centralize formatter width/wrapping thresholds into a policy object instead of scattered magic numbers.
- [ ] 8. Replace fragile function/frame string special-casing with more general and structured formatting logic.
- [ ] 9. Remove AST mutation during formatting (e.g., mutating `leadingComments`) and keep formatting operations pure.
- [ ] 10. Preserve original comment styles by default in CTE comment handling, avoiding heuristic `--` to `/* ... */` rewrites.
