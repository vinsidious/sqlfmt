import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { FormatterError } from '../src/formatter';
import { parse, ParseError, MaxDepthError } from '../src/parser';
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
      expect(te.column).toBe(10);
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

describe('unterminated dollar-quoted string error details', () => {
  it('handles unterminated $$ gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $$no close');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('handles unterminated $tag$ gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $custom_tag$no close');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('handles unterminated dollar string on line 2 gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT 1;\n$$no close');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });
});

describe('error line/column accuracy for multi-line SQL', () => {
  it('reports correct position for unterminated string on line 3', () => {
    try {
      tokenize("SELECT 1\nFROM t\nWHERE x = 'bad");
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      const te = err as TokenizeError;
      expect(te.line).toBe(3);
      expect(te.column).toBe(15);
    }
  });

  it('reports correct position for unterminated block comment on line 2', () => {
    try {
      tokenize('SELECT 1\n/* open');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      const te = err as TokenizeError;
      expect(te.line).toBe(2);
      expect(te.column).toBe(1);
    }
  });

  it('ParseError reports correct line for missing paren in multi-line SQL', () => {
    try {
      parse('SELECT\n  a,\n  (b + c\nFROM t;', { recover: false });
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      const pe = err as ParseError;
      // The error should be on line 4 where FROM is found instead of ')'
      expect(pe.line).toBe(4);
    }
  });

  it('ParseError has informative message text', () => {
    try {
      parse('INSERT INTO t VALUES (1, 2;', { recover: false });
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(ParseError);
      const pe = err as ParseError;
      expect(pe.message).toBeTruthy();
      expect(pe.message.length).toBeGreaterThan(5);
    }
  });
});

describe('MaxDepthError message is helpful (not misleading position)', () => {
  it('MaxDepthError message mentions nesting depth, not confusing position', () => {
    const sql = 'SELECT ' + '('.repeat(120) + '1' + ')'.repeat(120) + ';';
    try {
      parse(sql, { recover: false, maxDepth: 100 });
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(MaxDepthError);
      const mde = err as MaxDepthError;
      expect(mde.message).toContain('nesting depth');
      expect(mde.message).toContain('100');
    }
  });
});

describe('deeply nested expressions hit formatter depth limit', () => {
  it('deeply nested CASE expressions are caught by formatter depth guard', () => {
    // Build nested CASE at depth just under parser limit but enough to stress formatter
    const depth = 150;
    let expr = '1';
    for (let i = 0; i < depth; i++) {
      expr = `CASE WHEN x = ${i} THEN ${expr} ELSE 0 END`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    // Should either format successfully (with depth guard) or throw MaxDepthError
    // but should NOT cause a stack overflow
    try {
      const result = formatSQL(sql);
      expect(result).toContain('SELECT');
    } catch (err) {
      expect(err instanceof FormatterError || err instanceof MaxDepthError).toBe(true);
    }
  });

  it('deeply nested parenthesized expressions are caught by formatter depth guard', () => {
    // Use just under the parser maxDepth default so parsing succeeds,
    // but formatting depth guard kicks in
    const depth = 180;
    let expr = '1';
    for (let i = 0; i < depth; i++) {
      expr = `(${expr}) + 1`;
    }
    const sql = `SELECT ${expr} FROM t;`;
    try {
      const result = formatSQL(sql);
      expect(result).toContain('SELECT');
    } catch (err) {
      expect(err instanceof FormatterError || err instanceof MaxDepthError).toBe(true);
    }
  });
});

describe('recovery mode + strict mode interaction with new SQL constructs', () => {
  it('recovery mode handles invalid INTERVAL gracefully', () => {
    const nodes = parse("SELECT INTERVAL '1' FROMM t;", { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('strict mode rejects incomplete GROUPS window frame', () => {
    expect(() =>
      parse('SELECT SUM(x) OVER (ORDER BY y GROUPS BETWEEN) FROM t;', { recover: false })
    ).toThrow(ParseError);
  });

  it('recovery mode handles invalid CTE column list gracefully', () => {
    const nodes = parse('WITH cte (,) AS (SELECT 1) SELECT * FROM cte;', { recover: true });
    expect(nodes.length).toBeGreaterThan(0);
  });

  it('strict mode rejects malformed CTE', () => {
    expect(() =>
      parse('WITH AS (SELECT 1) SELECT * FROM cte;', { recover: false })
    ).toThrow(ParseError);
  });
});
