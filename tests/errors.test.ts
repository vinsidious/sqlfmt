import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse, ParseError } from '../src/parser';
import { tokenize } from '../src/tokenizer';

describe('malformed SQL and error paths', () => {
  it('throws tokenizer diagnostics for unterminated string/comment/identifier', () => {
    expect(() => tokenize("SELECT 'broken")).toThrow();
    expect(() => tokenize('SELECT /* broken')).toThrow();
    expect(() => tokenize('SELECT "broken')).toThrow();
  });

  it('throws ParseError in strict mode for missing parens', () => {
    expect(() => parse('SELECT (1 + 2;', { recover: false })).toThrow(ParseError);
  });

  it('throws ParseError in strict mode for invalid syntax', () => {
    expect(() => parse('INSERT INTO t VALUES (1,', { recover: false })).toThrow(ParseError);
  });

  it('handles empty statements gracefully in recovery mode', () => {
    expect(parse(';;;', { recover: true })).toEqual([]);
    expect(formatSQL('   \n\t  ')).toBe('');
  });

  it('preserves unknown random text in recovery mode', () => {
    const nodes = parse('this is not sql; totally ???;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('fails fast on deeply nested expressions beyond max depth', () => {
    const deep = 'SELECT ' + '('.repeat(140) + '1' + ')'.repeat(140) + ';';
    expect(() => parse(deep, { recover: false, maxDepth: 100 })).toThrow();
  });
});
