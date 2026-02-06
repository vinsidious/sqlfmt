import { tokenize } from './tokenizer';
import { Parser } from './parser';
import { formatStatements } from './formatter';

export function formatSQL(input: string): string {
  const trimmed = input.trim();
  if (!trimmed) return '';

  const tokens = tokenize(trimmed);
  const parser = new Parser(tokens);
  const statements = parser.parseStatements();

  if (statements.length === 0) return '';

  return formatStatements(statements).trimEnd() + '\n';
}
