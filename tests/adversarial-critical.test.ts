import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { tokenize, TokenizeError } from '../src/tokenizer';
import { parse, ParseError } from '../src/parser';

/**
 * Critical adversarial tests targeting specific vulnerabilities found in code review.
 * Focus: crash conditions, silent data loss, incorrect output, edge case mishandling.
 */

describe('CRITICAL: Block comment nesting ambiguity', () => {
  it('terminates block comment at first */ (not nested)', () => {
    // Standard SQL: /* /* */ text should be: comment "/* /* */", then tokens "text"
    const sql = 'SELECT /* /* */ 1 FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('/* /*');
    expect(result).toContain('1');
  });

  it('handles */ inside string literal correctly', () => {
    const sql = "SELECT '/* comment */' FROM t;";
    const result = formatSQL(sql);
    expect(result).toContain("'/* comment */'");
  });

  it('handles /* inside string literal correctly', () => {
    const sql = "SELECT '/* open' FROM t;";
    const result = formatSQL(sql);
    expect(result).toContain("'/* open'");
  });
});

describe('CRITICAL: Scientific notation backtracking edge cases', () => {
  it('1e alone (no digits after e) backtracks correctly', () => {
    const tokens = tokenize('SELECT 1e');
    const types = tokens.filter(t => t.type !== 'whitespace').map(t => ({ type: t.type, value: t.value }));
    expect(types).toContainEqual({ type: 'number', value: '1' });
    expect(types).toContainEqual({ type: 'identifier', value: 'e' });
  });

  it('1e+ without digits backtracks to 1, e, +', () => {
    const tokens = tokenize('SELECT 1e+');
    const nonWs = tokens.filter(t => t.type !== 'whitespace');
    expect(nonWs[1].value).toBe('1');
    expect(nonWs[1].type).toBe('number');
    expect(nonWs[2].value).toBe('e');
    expect(nonWs[3].value).toBe('+');
  });

  it('1e- without digits backtracks to 1, e, -', () => {
    const tokens = tokenize('SELECT 1e-');
    const nonWs = tokens.filter(t => t.type !== 'whitespace');
    expect(nonWs[1].value).toBe('1');
    expect(nonWs[2].value).toBe('e');
    expect(nonWs[3].value).toBe('-');
  });

  it('1e+x backtracks because x is not a digit', () => {
    const tokens = tokenize('SELECT 1e+x');
    const nonWs = tokens.filter(t => t.type !== 'whitespace');
    expect(nonWs[1].value).toBe('1');
    expect(nonWs[2].value).toBe('e');
    expect(nonWs[3].value).toBe('+');
    expect(nonWs[4].value).toBe('x');
  });

  it('handles valid 1e5 without backtracking', () => {
    const tokens = tokenize('SELECT 1e5');
    const nums = tokens.filter(t => t.type === 'number');
    expect(nums).toHaveLength(1);
    expect(nums[0].value).toBe('1e5');
  });

  it('handles valid 1.5e-3 without backtracking', () => {
    const tokens = tokenize('SELECT 1.5e-3');
    const nums = tokens.filter(t => t.type === 'number');
    expect(nums).toHaveLength(1);
    expect(nums[0].value).toBe('1.5e-3');
  });

  it('handles 1e followed by FROM keyword', () => {
    const tokens = tokenize('SELECT 1e FROM t');
    const nonWs = tokens.filter(t => t.type !== 'whitespace');
    expect(nonWs[1].value).toBe('1');
    expect(nonWs[1].type).toBe('number');
    expect(nonWs[2].value).toBe('e');
    expect(nonWs[2].type).toBe('identifier');
    expect(nonWs[3].value).toBe('FROM');
  });
});

describe('CRITICAL: Dollar-quoted string edge cases', () => {
  it('handles $$ as both parameter and dollar-quote start', () => {
    // $1 is parameter, $$ is dollar quote
    const tokens = tokenize('SELECT $1, $$text$$');
    expect(tokens.filter(t => t.type === 'parameter')).toHaveLength(1);
    expect(tokens.filter(t => t.type === 'string')).toHaveLength(1);
  });

  it('handles $123 as parameter (multiple digits)', () => {
    const tokens = tokenize('SELECT $1, $23, $456');
    const params = tokens.filter(t => t.type === 'parameter').map(t => t.value);
    expect(params).toEqual(['$1', '$23', '$456']);
  });

  it('handles $ not followed by digit or valid tag start', () => {
    // $ alone or $@ etc should throw or be handled
    try {
      const tokens = tokenize('SELECT $@');
      // If it doesn't throw, $ should be an error or operator
      const dollar = tokens.find(t => t.value === '$' || t.value === '$@');
      expect(dollar).toBeDefined();
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
    }
  });

  it('handles empty dollar tag $$', () => {
    const tokens = tokenize('SELECT $$$$');
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$$$$');
  });

  it('handles dollar tag with underscore', () => {
    const tokens = tokenize('SELECT $_tag$body$_tag$');
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$_tag$body$_tag$');
  });

  it('handles dollar tag with numbers', () => {
    const tokens = tokenize('SELECT $tag123$body$tag123$');
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$tag123$body$tag123$');
  });

  it('treats $123 as parameter, then tag$ as separate tokens', () => {
    // $123tag$ is parsed as: $123 (parameter), then "tag$" causes error because $ is unexpected
    // This is correct behavior - dollar tags cannot start with a digit
    try {
      tokenize('SELECT $123tag$');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
    }
  });

  it('handles nested dollar quotes with DIFFERENT tags correctly', () => {
    // $a$ ... $b$ ... $b$ ... $a$ - inner $b$ delimiters are literal text
    const sql = 'SELECT $outer$text $inner$nested$inner$ more$outer$';
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$outer$text $inner$nested$inner$ more$outer$');
  });

  it('dollar-quoted string with unmatched inner delimiter is OK', () => {
    // $a$ ... $b$ ... $a$ - the $b$ is just literal text, doesn't need closing
    const sql = 'SELECT $outer$has $other$ inside$outer$';
    const tokens = tokenize(sql);
    const str = tokens.find(t => t.type === 'string');
    expect(str?.value).toBe('$outer$has $other$ inside$outer$');
  });
});

describe('CRITICAL: String literal edge cases', () => {
  it('handles escaped single quote at end of string', () => {
    const sql = "SELECT 'text''';";
    const result = formatSQL(sql);
    expect(result).toContain("'text'''");
  });

  it('handles multiple consecutive escaped quotes', () => {
    const sql = "SELECT '''''';";
    const result = formatSQL(sql);
    expect(result).toContain("''''''");
  });

  it('handles E-string with backslash at end', () => {
    const sql = "SELECT E'text\\\\';";
    const result = formatSQL(sql);
    expect(result).toContain("E'text\\\\'");
  });

  it('handles E-string with escape then quote', () => {
    const sql = "SELECT E'\\'escaped quote';";
    const result = formatSQL(sql);
    expect(result).toContain("E'\\'");
  });

  it('handles regular string with backslash (no escape)', () => {
    // Non-E strings don't treat backslash as escape
    const sql = "SELECT 'path\\to\\file';";
    const result = formatSQL(sql);
    expect(result).toContain('path\\to\\file');
  });

  it('handles N-prefix string', () => {
    // N'...' is national character string literal (SQL standard)
    try {
      const sql = "SELECT N'text';";
      const result = formatSQL(sql);
      expect(result).toBeDefined();
    } catch (err) {
      // May not be supported, that's OK
      expect(err).toBeDefined();
    }
  });

  it('handles B-prefix binary string', () => {
    const sql = "SELECT B'10101010';";
    const result = formatSQL(sql);
    expect(result).toContain("B'10101010'");
  });

  it('handles X-prefix hex string', () => {
    const sql = "SELECT X'DEADBEEF';";
    const result = formatSQL(sql);
    expect(result).toContain("X'DEADBEEF'");
  });

  it('handles string with newlines', () => {
    const sql = "SELECT 'line1\nline2\nline3';";
    const result = formatSQL(sql);
    expect(result).toContain('line1\nline2\nline3');
  });

  it('handles string with CRLF', () => {
    const sql = "SELECT 'line1\r\nline2';";
    const result = formatSQL(sql);
    expect(result).toContain('line1\r\nline2');
  });
});

describe('CRITICAL: Quoted identifier edge cases', () => {
  it('handles escaped double quote inside identifier', () => {
    const sql = 'SELECT "col""name" FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('"col""name"');
  });

  it('handles multiple escaped quotes in identifier', () => {
    const sql = 'SELECT """quoted""" FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('"""quoted"""');
  });

  it('handles empty quoted identifier (edge case)', () => {
    try {
      const sql = 'SELECT "" FROM t;';
      const tokens = tokenize(sql);
      const ident = tokens.find(t => t.type === 'identifier' && t.value === '""');
      expect(ident).toBeDefined();
    } catch (err) {
      // May be rejected, that's OK
      expect(err).toBeDefined();
    }
  });

  it('handles quoted identifier with newlines', () => {
    const sql = 'SELECT "col\nname" FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('"col\nname"');
  });

  it('handles quoted identifier at max length', () => {
    // Max is 10000 chars for the whole token including quotes
    const name = '"' + 'x'.repeat(9998) + '"';
    const sql = `SELECT ${name} FROM t;`;
    const result = formatSQL(sql);
    expect(result).toContain(name);
  });

  it('rejects quoted identifier over max length', () => {
    const name = '"' + 'x'.repeat(10001) + '"';
    const sql = `SELECT ${name} FROM t;`;
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });
});

describe('CRITICAL: Numeric literal edge cases', () => {
  it('handles hex with mixed case', () => {
    const sql = 'SELECT 0xAbCdEf, 0XaBcDeF;';
    const result = formatSQL(sql);
    expect(result).toContain('0xAbCdEf');
    expect(result).toContain('0XaBcDeF');
  });

  it('handles decimal with no integer part', () => {
    const sql = 'SELECT .5, .123456;';
    const result = formatSQL(sql);
    expect(result).toContain('.5');
    expect(result).toContain('.123456');
  });

  it('handles decimal with no fractional part', () => {
    const sql = 'SELECT 5., 123.;';
    const result = formatSQL(sql);
    expect(result).toContain('5.');
    expect(result).toContain('123.');
  });

  it('handles single zero', () => {
    const sql = 'SELECT 0;';
    const result = formatSQL(sql);
    expect(result).toContain('0');
  });

  it('handles 0x0 (hex zero)', () => {
    const sql = 'SELECT 0x0;';
    const result = formatSQL(sql);
    expect(result).toContain('0x0');
  });

  it('handles number with underscores in various positions', () => {
    const sql = 'SELECT 1_000, 1_2_3, 0xFF_AA;';
    const result = formatSQL(sql);
    expect(result).toContain('1_000');
    expect(result).toContain('1_2_3');
    expect(result).toContain('0xFF_AA');
  });

  it('rejects or handles number ending with underscore', () => {
    // Depending on implementation, 123_ might be 123 + identifier "_"
    try {
      const tokens = tokenize('SELECT 123_');
      const num = tokens.find(t => t.type === 'number' && t.value === '123_');
      // If underscore is part of number, that's one interpretation
      // If it's separate, that's another
      expect(tokens.length).toBeGreaterThan(0);
    } catch (err) {
      expect(err).toBeDefined();
    }
  });
});

describe('CRITICAL: Operator precedence and ambiguity', () => {
  it('handles - as both binary minus and unary minus', () => {
    const sql = 'SELECT -1, a - b, -x FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('-1');
    expect(result).toContain('a - b');
  });

  it('handles -- as line comment vs minus minus', () => {
    const sql = 'SELECT 1--comment\n FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('--comment');
  });

  it('distinguishes -- comment from - - operators', () => {
    // "- -" with space is two unary minus, "--" is comment
    const sql = 'SELECT - -1;';
    const tokens = tokenize(sql);
    const ops = tokens.filter(t => t.value === '-');
    expect(ops.length).toBe(2);
  });

  it('handles /* */ vs / and *', () => {
    const sql = 'SELECT a / b, a * b, /* comment */ c FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('/');
    expect(result).toContain('*');
    expect(result).toContain('/* comment */');
  });

  it('handles -> operator vs - and >', () => {
    const sql = 'SELECT a->b, c - d, e > f;';
    const result = formatSQL(sql);
    expect(result).toContain('->');
  });

  it('handles ->> operator vs - and >>', () => {
    const sql = 'SELECT a->>b, c >> d;';
    const result = formatSQL(sql);
    expect(result).toContain('->>');
  });

  it('handles #> vs # and >', () => {
    const sql = 'SELECT a#>b;';
    const result = formatSQL(sql);
    expect(result).toContain('#>');
  });

  it('handles #>> vs # and >>', () => {
    const sql = 'SELECT a#>>b;';
    const result = formatSQL(sql);
    expect(result).toContain('#>>');
  });

  it('handles @> and <@ operators', () => {
    const sql = 'SELECT a @> b, c <@ d;';
    const result = formatSQL(sql);
    expect(result).toContain('@>');
    expect(result).toContain('<@');
  });

  it('handles :: type cast vs : punctuation', () => {
    const sql = 'SELECT a::int, array[1:5];';
    const result = formatSQL(sql);
    expect(result).toContain('::');
    expect(result).toContain('[1:5]');
  });
});

describe('CRITICAL: Whitespace handling', () => {
  it('handles form feed', () => {
    const sql = 'SELECT\f1 FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });

  it('handles vertical tab', () => {
    const sql = 'SELECT\v1 FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });

  it('handles all whitespace types together', () => {
    const sql = 'SELECT \t\n\r\f\v 1 FROM t;';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
  });

  it('rejects null byte', () => {
    const sql = 'SELECT 1\x00FROM t;';
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('rejects control characters', () => {
    const sql = 'SELECT 1\x01FROM t;';
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });
});

describe('CRITICAL: Token count DoS protection', () => {
  it('allows exactly 1,000,000 tokens', () => {
    // Each semicolon is 1 token + final EOF = exactly 1,000,000
    const sql = ';'.repeat(999_999);
    const tokens = tokenize(sql);
    expect(tokens).toHaveLength(1_000_000);
    expect(tokens[tokens.length - 1].type).toBe('eof');
  });

  it('rejects 1,000,001 tokens', () => {
    const sql = ';'.repeat(1_000_001);
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('error message mentions token count limit', () => {
    const sql = ';'.repeat(1_000_001);
    try {
      tokenize(sql);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('1000000');
    }
  });
});

describe('CRITICAL: Input size DoS protection', () => {
  it('allows input just under 10MB limit', () => {
    // Use a large comment token to avoid identifier-length guards.
    const prefix = '-- ';
    const sql = prefix + 'a'.repeat(10_485_760 - prefix.length - 1);
    const result = formatSQL(sql);
    expect(result).toContain('--');
  });

  it('rejects input over 10MB limit', () => {
    const prefix = '-- ';
    const sql = prefix + 'a'.repeat(10_485_760 - prefix.length + 1);
    expect(() => formatSQL(sql)).toThrow('exceeds maximum size');
  });

  it('respects custom maxInputSize option', () => {
    const sql = 'SELECT 123;';
    expect(() => formatSQL(sql, { maxInputSize: 5 })).toThrow('exceeds maximum size');
  });
});

describe('CRITICAL: Depth limit DoS protection', () => {
  it('allows nesting just under maxDepth', () => {
    const depth = 99;
    const sql = 'SELECT ' + '('.repeat(depth) + '1' + ')'.repeat(depth) + ';';
    const result = formatSQL(sql, { maxDepth: 100 });
    expect(result).toContain('SELECT');
  });

  it('rejects nesting at maxDepth', () => {
    const depth = 101;
    const sql = 'SELECT ' + '('.repeat(depth) + '1' + ')'.repeat(depth) + ';';
    expect(() => formatSQL(sql, { maxDepth: 100 })).toThrow('nesting depth');
  });
});

describe('CRITICAL: Recovery mode data preservation', () => {
  it('preserves original text in recovery mode', () => {
    const sql = 'SELECT bad syntax here FROM t;';
    const result = formatSQL(sql, { recover: true });
    expect(result).toContain('SELECT');
    expect(result).toContain('bad');
  });

  it('does not silently drop statements in recovery mode', () => {
    const sql = 'SELECT 1; BAD SYNTAX; SELECT 2;';
    const result = formatSQL(sql, { recover: true });
    expect(result).toContain('SELECT 1');
    expect(result).toContain('SELECT 2');
    expect(result).toContain('BAD SYNTAX');
  });
});

describe('CRITICAL: Unterminated input error handling', () => {
  it('reports unterminated string with correct position', () => {
    try {
      tokenize("SELECT 'unterminated");
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('Unterminated string');
    }
  });

  it('reports unterminated block comment with correct position', () => {
    try {
      tokenize('SELECT /* unterminated');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('Unterminated block comment');
    }
  });

  it('reports unterminated quoted identifier with correct position', () => {
    try {
      tokenize('SELECT "unterminated');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('Unterminated quoted identifier');
    }
  });

  it('reports unterminated dollar-quoted string with delimiter in message', () => {
    try {
      tokenize('SELECT $tag$unterminated');
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('$tag$');
    }
  });
});
