# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.x     | Yes       |
| < 1.0   | No        |

## Reporting a Vulnerability

If you discover a security vulnerability in sqlfmt, please report it responsibly:

1. **Do not** open a public issue.
2. Email **vincecoppola@gmail.com** with a description of the vulnerability, steps to reproduce, and any relevant context.
3. You should receive an acknowledgment within 48 hours.
4. A fix will be developed privately and released as a patch version.

## Security Considerations

sqlfmt is a **formatter only**. It transforms SQL text into formatted SQL text. It does not:

- **Execute SQL** -- sqlfmt never connects to a database or runs any queries.
- **Make network requests** -- sqlfmt has zero runtime dependencies and performs no I/O beyond reading input and writing output.
- **Evaluate expressions** -- SQL content is parsed structurally but never interpreted or executed.

This reduces risk substantially versus tools that execute SQL, but it does **not** eliminate denial-of-service risk from adversarially large or complex input. For multi-tenant or hostile environments, run sqlfmt with CPU/memory/time limits in a sandboxed process.

## Input Constraints

To prevent resource exhaustion when processing untrusted input, sqlfmt enforces:

- **Input size limit** -- Inputs exceeding the maximum byte size are rejected before processing.
- **Token count limit** -- The tokenizer limits the number of tokens produced from a single input.
- **Parse depth limit** -- The parser limits recursion depth to prevent stack overflow from deeply nested expressions.

These limits are set to values that accommodate any reasonable SQL while guarding against adversarial input.

## Dependencies

sqlfmt has **zero runtime dependencies**. The only dependencies are build-time dev dependencies (TypeScript, tsup, Bun test types). This minimizes supply chain risk.
