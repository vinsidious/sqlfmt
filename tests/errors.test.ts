import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse, ParseError } from '../src/parser';
import { tokenize, TokenizeError } from '../src/tokenizer';

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

describe('error line/column tracking', () => {
  it('TokenizeError includes correct line/column for unterminated string', () => {
    try {
      tokenize("SELECT\n  'broken");
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      const te = err as TokenizeError;
      expect(te.line).toBe(2);
      expect(te.column).toBe(3);
    }
  });

  it('TokenizeError includes correct line/column for unterminated block comment', () => {
    try {
      tokenize('SELECT 1\n/* open comment');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      const te = err as TokenizeError;
      expect(te.line).toBe(2);
      expect(te.column).toBe(1);
    }
  });

  it('TokenizeError includes correct line/column for unterminated quoted identifier', () => {
    try {
      tokenize('SELECT "broken');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      const te = err as TokenizeError;
      expect(te.line).toBe(1);
      expect(te.column).toBe(8);
    }
  });

  it('ParseError includes correct line/column', () => {
    try {
      parse('SELECT\n  (1 + 2;', { recover: false });
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      const pe = err as ParseError;
      expect(pe.line).toBe(2);
      expect(pe.column).toBe(9);
    }
  });

  it('tokens carry line and column fields', () => {
    const tokens = tokenize('SELECT\n  1');
    const selectTok = tokens.find(t => t.value === 'SELECT');
    expect(selectTok!.line).toBe(1);
    expect(selectTok!.column).toBe(1);

    const numTok = tokens.find(t => t.value === '1');
    expect(numTok!.line).toBe(2);
    expect(numTok!.column).toBe(3);
  });
});
