import { parse } from './parser';
import { formatStatements } from './formatter';

export interface FormatOptions {
  // Maximum allowed parser nesting depth before failing fast.
  maxDepth?: number;
}

/**
 * Format SQL according to this library's style rules.
 *
 * @param input SQL text to format.
 * @param options Optional formatter options.
 * @returns The formatted SQL with a trailing newline, or an empty string for blank input.
 * @throws {Error} When tokenization/parsing fails (including depth limit violations).
 *
 * @example
 * formatSQL('select 1;')
 * // => 'SELECT 1;\\n'
 */
export function formatSQL(input: string, options: FormatOptions = {}): string {
  const trimmed = input.trim();
  if (!trimmed) return '';

  const statements = parse(trimmed, { recover: true, maxDepth: options.maxDepth });

  if (statements.length === 0) return '';

  const formatted = formatStatements(statements)
    .split('\n')
    .map(line => line.replace(/[ \t]+$/g, ''))
    .join('\n');

  return formatted.trimEnd() + '\n';
}
