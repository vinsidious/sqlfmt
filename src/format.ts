import { parse, ParseError } from './parser';
import type * as AST from './ast';
import { formatStatements } from './formatter';

const DEFAULT_MAX_INPUT_SIZE = 10_485_760; // 10MB

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
   * Whether to recover from parse errors by passing through raw SQL.
   *
   * When `true` (default), unparseable statements are preserved as raw text
   * instead of throwing. When `false`, a {@link ParseError} is thrown on the
   * first parse failure.
   *
   * @default true
   */
  recover?: boolean;

  /**
   * Called when the parser recovers from an unparseable statement.
   * The recovered statement is passed through as raw text.
   *
   * Only called when `recover` is `true` (the default) and the parser
   * falls back to raw-passthrough for a statement.
   *
   * @param error - The ParseError that triggered recovery
   * @param raw - The raw expression node containing the original SQL text, or
   *   null if recovery could not extract any text (rare end-of-input cases)
   */
  onRecover?: (error: ParseError, raw: AST.RawExpression | null) => void;

  /**
   * Callback invoked when recovery cannot preserve a failed statement as raw SQL.
   *
   * This is rare (typically end-of-input failures), but allows callers to
   * surface potential statement drops explicitly.
   */
  onDropStatement?: (error: ParseError) => void;
}

/**
 * Format SQL according to the Simon Holywell SQL Style Guide with river alignment.
 *
 * Keywords are right-aligned to form a visual "river" of whitespace, making
 * queries easier to scan. Identifiers are lowercased, keywords are uppercased.
 *
 * @param input - SQL text to format. May contain multiple statements.
 * @param options - Optional formatting options.
 * @returns Formatted SQL with a trailing newline, or empty string for blank input.
 * @throws {TokenizeError} When the input contains invalid tokens (e.g., unterminated strings).
 * @throws {ParseError} When `recover` is `false` and parsing fails. When `recover` is `true`
 *   (the default), unparseable statements are recovered as raw passthrough where possible.
 * @throws {Error} When input exceeds maximum size.
 *
 * @example
 * ```typescript
 * import { formatSQL } from '@vcoppola/sqlfmt';
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
  if (input.length > maxSize) {
    throw new Error(
      `Input exceeds maximum size of ${maxSize} bytes. Use the maxInputSize option to increase the limit.`
    );
  }

  const trimmed = input.trim();
  if (!trimmed) return '';

  const statements = parse(trimmed, {
    recover: options.recover ?? true,
    maxDepth: options.maxDepth,
    onRecover: options.onRecover,
    onDropStatement: options.onDropStatement,
  });

  if (statements.length === 0) return '';

  const formatted = formatStatements(statements)
    .split('\n')
    .map(line => line.replace(/[ \t]+$/g, ''))
    .join('\n');

  return formatted.trimEnd() + '\n';
}
