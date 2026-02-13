import { parse, ParseError, type ParseRecoveryContext } from './parser';
import type * as AST from './ast';
import { formatStatements } from './formatter';
import { DEFAULT_MAX_INPUT_SIZE } from './constants';
import type { SQLDialect } from './dialect';
import { resolveDialectProfile } from './dialects';

/** Calculate UTF-8 byte length without allocating an encoded copy. */
function utf8ByteLength(s: string): number {
  let bytes = 0;
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i);
    if (code <= 0x7f) {
      bytes += 1;
    } else if (code <= 0x7ff) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = i + 1 < s.length ? s.charCodeAt(i + 1) : 0;
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4; // surrogate pair -> 4-byte UTF-8 code point
        i++; // skip low surrogate
      } else {
        bytes += 3; // malformed surrogate sequences encode as U+FFFD in UTF-8
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      bytes += 3; // malformed surrogate sequences encode as U+FFFD in UTF-8
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

/**
 * Options for {@link formatSQL}.
 */
export interface FormatOptions {
  /**
   * Maximum allowed parser nesting depth before failing fast.
   *
   * Deeply nested sub-expressions (subqueries, CASE chains, etc.) increase
   * memory and stack usage. Set this to guard against pathological input.
   *
   * @default 200
   */
  maxDepth?: number;

  /**
   * Maximum allowed input size in bytes.
   *
   * Prevents excessive memory consumption on very large inputs.
   *
   * @default 10_485_760 (10 MB)
   */
  maxInputSize?: number;

  /**
   * Preferred maximum output line length (in display columns).
   *
   * @default 80
   */
  maxLineLength?: number;

  /**
   * Whether to recover from parse errors by passing through raw SQL.
   *
   * When `true` (default), unparseable statements are preserved as raw text instead of
   * throwing. When `false`, a {@link ParseError} is thrown on the
   * first parse failure.
   *
   * @default true
   */
  recover?: boolean;

  /**
   * Called when the parser recovers from an unparseable statement.
   * The recovered statement is passed through as raw text.
   *
   * Only called when `recover` is `true` and the parser
   * falls back to raw-passthrough for a statement.
   *
   * @param error - The ParseError that triggered recovery
   * @param raw - The raw expression node containing the original SQL text, or
   *   null if recovery could not extract any text (rare end-of-input cases)
   */
  onRecover?: (error: ParseError, raw: AST.RawExpression | null, context: ParseRecoveryContext) => void;

  /**
   * Callback invoked when recovery cannot preserve a failed statement as raw SQL.
   *
   * This is rare (typically end-of-input failures). When omitted, the parser
   * throws instead of dropping a statement silently.
   */
  onDropStatement?: (error: ParseError, context: ParseRecoveryContext) => void;

  /**
   * Callback invoked when a statement is passed through as raw text because
   * it uses unsupported syntax (e.g. SET, USE, DBCC, CALL).
   *
   * Unlike `onRecover`, this fires for statements the parser intentionally
   * does not format, not for parse errors.
   */
  onPassthrough?: (raw: AST.RawExpression, context: ParseRecoveryContext) => void;

  /**
   * SQL dialect selection or custom dialect profile.
   */
  dialect?: SQLDialect;

  /**
   * Maximum token count allowed during tokenization.
   *
   * Useful for formatting very large SQL dumps where the default tokenizer
   * ceiling is too low.
   */
  maxTokenCount?: number;
}

/**
 * Format SQL according to the Simon Holywell SQL Style Guide with river alignment.
 *
 * Keywords are right-aligned to form a visual "river" of whitespace, making
 * queries easier to scan. ALL-CAPS identifiers are lowercased, mixed-case
 * identifiers are preserved, and keywords are uppercased.
 *
 * @param input - SQL text to format. May contain multiple statements.
 * @param options - Optional formatting options.
 * @returns Formatted SQL with a trailing newline, or empty string for blank input.
 * @throws {TokenizeError} When the input contains invalid tokens (e.g., unterminated strings).
 * @throws {ParseError} When parsing fails and `recover` is `false`. When `recover`
 *   is `true` (default), unparseable statements are recovered as raw passthrough where possible.
 * @throws {Error} When input exceeds maximum size.
 *
 * @example
 * ```typescript
 * import { formatSQL } from 'holywell';
 *
 * formatSQL('select id, name from users where active = true;');
 * // =>
 * // SELECT id, name
 * //   FROM users
 * //  WHERE active = TRUE;
 * ```
 */
export function formatSQL(input: string, options: FormatOptions = {}): string {
  const maxSize = options.maxInputSize ?? DEFAULT_MAX_INPUT_SIZE;
  if (utf8ByteLength(input) > maxSize) {
    throw new Error(
      `Input exceeds maximum size of ${maxSize} bytes. Use the maxInputSize option to increase the limit.`
    );
  }

  if (!input.trim()) return '';

  const statements = parse(input, {
    recover: options.recover ?? true,
    maxDepth: options.maxDepth,
    maxTokenCount: options.maxTokenCount,
    onRecover: options.onRecover,
    onDropStatement: options.onDropStatement,
    onPassthrough: options.onPassthrough,
    dialect: options.dialect,
  });

  if (statements.length === 0) return '';

  const profile = resolveDialectProfile(options.dialect);
  const formatted = formatStatements(statements, {
    maxLineLength: options.maxLineLength,
    dialect: options.dialect,
    maxDepth: options.maxDepth,
    maxTokenCount: options.maxTokenCount,
    functionKeywords: profile.functionKeywords,
  })
    .split('\n')
    .map(line => line.trimEnd())
    .join('\n');

  return formatted.trimEnd() + '\n';
}
