import { describe, expect, it } from 'bun:test';
import { tokenize, TokenizeError, type TokenType } from '../src/tokenizer';

function nonWhitespaceTypes(sql: string): TokenType[] {
  return tokenize(sql)
    .filter(t => t.type !== 'whitespace')
    .map(t => t.type);
}

describe('tokenizer basics', () => {
  it('handles empty input', () => {
    const tokens = tokenize('');
    expect(tokens).toHaveLength(1);
    expect(tokens[0].type).toBe('eof');
  });

  it('handles comment-only input', () => {
    const tokens = tokenize('-- hi');
    expect(tokens.find(t => t.type === 'line_comment')?.value).toBe('-- hi');
  });

  it('emits all core token types', () => {
    const types = nonWhitespaceTypes("SELECT a, 1 + 2, 'x' FROM t -- c\n/* b */;");
    expect(types).toContain('keyword');
    expect(types).toContain('identifier');
    expect(types).toContain('number');
    expect(types).toContain('string');
    expect(types).toContain('operator');
    expect(types).toContain('punctuation');
    expect(types).toContain('line_comment');
    expect(types).toContain('block_comment');
    expect(types[types.length - 1]).toBe('eof');
  });

  it('tokenizes curly braces as punctuation', () => {
    const tokens = tokenize('SELECT { fn HOUR(ts) } FROM t;').filter(t => t.type !== 'whitespace');
    expect(tokens.map(t => t.value)).toContain('{');
    expect(tokens.map(t => t.value)).toContain('}');
    const braceTokens = tokens.filter(t => t.value === '{' || t.value === '}');
    expect(braceTokens.every(t => t.type === 'punctuation')).toBe(true);
  });
});

describe('tokenizer literals and parameters', () => {
  it('tokenizes dollar-quoted strings', () => {
    const stringToken = tokenize("SELECT $$body$$;").find(t => t.type === 'string');
    expect(stringToken?.value).toBe('$$body$$');
  });

  it('tokenizes tagged dollar-quoted strings', () => {
    const stringToken = tokenize("SELECT $fn$BEGIN RAISE NOTICE 'x'; END$fn$;").find(t => t.type === 'string');
    expect(stringToken?.value).toBe("$fn$BEGIN RAISE NOTICE 'x'; END$fn$");
  });

  it('tokenizes positional parameters as a single token', () => {
    const tokens = tokenize('SELECT $1, $23;').filter(t => t.type !== 'whitespace');
    expect(tokens.map(t => t.value)).toContain('$1');
    expect(tokens.map(t => t.value)).toContain('$23');
    expect(tokens.some(t => t.value === '$')).toBe(false);
  });

  it('tokenizes scientific notation numbers', () => {
    const num = tokenize('SELECT 1e5, 1.2E-4, .5e+2;').filter(t => t.type === 'number').map(t => t.value);
    expect(num).toEqual(['1e5', '1.2E-4', '.5e+2']);
  });

  it('tokenizes hex numeric literals', () => {
    const num = tokenize('SELECT 0xFF, 0X1a;').filter(t => t.type === 'number').map(t => t.value);
    expect(num).toEqual(['0xFF', '0X1a']);
  });

  it('tokenizes numeric literals with underscores', () => {
    const num = tokenize('SELECT 1_000_000, 0xFF_FF, 1.2_3e4_5;').filter(t => t.type === 'number').map(t => t.value);
    expect(num).toEqual(['1_000_000', '0xFF_FF', '1.2_3e4_5']);
  });

  it('tokenizes E-strings', () => {
    const str = tokenize("SELECT E'\\n\\t', e'\\\\x';").filter(t => t.type === 'string').map(t => t.value);
    expect(str).toEqual(["E'\\n\\t'", "e'\\\\x'"]);
  });

  it('tokenizes E-strings with doubled quotes', () => {
    const str = tokenize("SELECT E'abc'' def';").filter(t => t.type === 'string').map(t => t.value);
    expect(str).toEqual(["E'abc'' def'"]);
  });

  it('tokenizes B and X prefixed strings', () => {
    const str = tokenize("SELECT B'1010', b'0101', X'FF', x'aa';").filter(t => t.type === 'string').map(t => t.value);
    expect(str).toEqual(["B'1010'", "b'0101'", "X'FF'", "x'aa'"]);
  });

  it('tokenizes U& prefixed Unicode escape strings', () => {
    const str = tokenize("SELECT U&'\\0041\\0042';").filter(t => t.type === 'string').map(t => t.value);
    expect(str).toEqual(["U&'\\0041\\0042'"]);
  });
});

describe('tokenizer robustness', () => {
  it('supports Unicode identifiers', () => {
    const ids = tokenize('SELECT café, 用户, имя, Δvalue FROM 数据;')
      .filter(t => t.type === 'identifier')
      .map(t => t.value);

    expect(ids).toContain('café');
    expect(ids).toContain('用户');
    expect(ids).toContain('имя');
    expect(ids).toContain('Δvalue');
    expect(ids).toContain('数据');
  });

  it('throws on unterminated string literal', () => {
    expect(() => tokenize("SELECT 'unterminated")).toThrow();
  });

  it('throws on unterminated block comment', () => {
    expect(() => tokenize('SELECT /* missing')).toThrow();
  });

  it('throws on unterminated quoted identifier', () => {
    expect(() => tokenize('SELECT "unterminated')).toThrow();
  });

  it('handles unterminated dollar-quoted string gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $tag$no end');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('throws on unexpected control characters', () => {
    expect(() => tokenize('SELECT 1 \u0001 FROM t;')).toThrow(TokenizeError);
  });
});

describe('tokenizer Unicode edge cases', () => {
  it('handles CJK characters in string literals', () => {
    const tokens = tokenize("SELECT '数据库', '用户名' FROM t;");
    const strings = tokens.filter(t => t.type === 'string').map(t => t.value);
    expect(strings).toEqual(["'数据库'", "'用户名'"]);
  });

  it('handles emoji in string literals', () => {
    const tokens = tokenize("SELECT '🎉 party', '🔥🔥' FROM t;");
    const strings = tokens.filter(t => t.type === 'string').map(t => t.value);
    expect(strings).toEqual(["'🎉 party'", "'🔥🔥'"]);
  });

  it('handles mixed CJK identifiers and string literals', () => {
    const tokens = tokenize("SELECT 名前 AS 名, '値' FROM テーブル;");
    const ids = tokens.filter(t => t.type === 'identifier').map(t => t.value);
    expect(ids).toContain('名前');
    expect(ids).toContain('名');
    expect(ids).toContain('テーブル');
    const strings = tokens.filter(t => t.type === 'string').map(t => t.value);
    expect(strings).toEqual(["'値'"]);
  });

  it('tracks line/column correctly with multi-byte characters', () => {
    // '数' is 1 JS string index, so column should be 8 for SELECT after "SELECT 数,\n"
    const tokens = tokenize("SELECT '数',\n  1;");
    const numToken = tokens.find(t => t.type === 'number');
    expect(numToken?.line).toBe(2);
    expect(numToken?.column).toBe(3);
  });
});

describe('tokenizer dollar-quoted nesting', () => {
  it('handles nested dollar-quoted strings with different tags', () => {
    const sql = "SELECT $outer$ text $inner$ nested $inner$ more $outer$;";
    const tokens = tokenize(sql);
    const stringTokens = tokens.filter(t => t.type === 'string');
    expect(stringTokens).toHaveLength(1);
    expect(stringTokens[0].value).toBe('$outer$ text $inner$ nested $inner$ more $outer$');
  });

  it('handles $$ inside $tag$ dollar-quoted strings', () => {
    const sql = "SELECT $fn$body $$ not a delim $$ end$fn$;";
    const tokens = tokenize(sql);
    const stringTokens = tokens.filter(t => t.type === 'string');
    expect(stringTokens).toHaveLength(1);
    expect(stringTokens[0].value).toBe('$fn$body $$ not a delim $$ end$fn$');
  });
});

describe('tokenizer scientific notation edge cases', () => {
  it('backtracks on 1e followed by non-digit (e.g., SELECT 1e FROM t)', () => {
    const tokens = tokenize('SELECT 1e FROM t;').filter(t => t.type !== 'whitespace');
    // '1' should be a number, 'e' should be an identifier (keyword or ident)
    const values = tokens.map(t => t.value);
    expect(values[0]).toBe('SELECT');
    expect(values[1]).toBe('1');
    expect(values[2]).toBe('e');
    expect(values[3]).toBe('FROM');
    const numTokens = tokens.filter(t => t.type === 'number');
    expect(numTokens).toHaveLength(1);
    expect(numTokens[0].value).toBe('1');
  });

  it('backtracks on 1E+ followed by non-digit', () => {
    const tokens = tokenize('SELECT 1E FROM t;').filter(t => t.type !== 'whitespace');
    expect(tokens[1].value).toBe('1');
    expect(tokens[1].type).toBe('number');
    expect(tokens[2].value).toBe('E');
  });

  it('handles 1e+x (sign followed by non-digit) by backtracking', () => {
    const tokens = tokenize('SELECT 1e+x;').filter(t => t.type !== 'whitespace');
    expect(tokens[1].value).toBe('1');
    expect(tokens[1].type).toBe('number');
    expect(tokens[2].value).toBe('e');
    expect(tokens[2].type).toBe('identifier');
  });
});

describe('tokenizer edge cases', () => {
  it('tokenizes empty dollar-quoted string $$$$', () => {
    const tokens = tokenize('SELECT $$$$;');
    const stringTokens = tokens.filter(t => t.type === 'string');
    expect(stringTokens).toHaveLength(1);
    expect(stringTokens[0].value).toBe('$$$$');
  });

  it('handles unterminated dollar-quote gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $mytag$unterminated');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('handles unterminated $$ gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $$unterminated');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('throws TokenizeError when token count exceeds limit', () => {
    // Each "1," produces 3 tokens (number, punctuation, and likely whitespace or not).
    // To hit 1,000,000 tokens we need a lot of simple tokens. Use a repeated "1 " pattern.
    // Each "1 " produces 2 tokens (number + whitespace). So 500,001 repetitions = 1,000,002 tokens + eof.
    // That's too large to construct. Instead, verify the error type with a smaller mock.
    // We can test by generating enough simple single-char tokens.
    // ";" produces 1 punctuation token each. 1,000,001 semicolons should trigger it.
    const bigInput = ';'.repeat(1_000_001);
    expect(() => tokenize(bigInput)).toThrow(TokenizeError);
  });

  it('valid input just under token limit does not throw', () => {
    // 999,999 semicolons = 999,999 punctuation tokens + 1 eof = 1,000,000 total
    const input = ';'.repeat(999_999);
    const tokens = tokenize(input);
    expect(tokens[tokens.length - 1].type).toBe('eof');
    expect(tokens).toHaveLength(1_000_000);
  });
});

describe('tokenizer quoted identifier length limit', () => {
  it('throws TokenizeError for quoted identifiers exceeding 10000 characters', () => {
    const longIdent = '"' + 'a'.repeat(10_001) + '"';
    const sql = `SELECT ${longIdent};`;
    expect(() => tokenize(sql)).toThrow(TokenizeError);
  });

  it('allows quoted identifiers at exactly 10000 characters', () => {
    const longIdent = '"' + 'b'.repeat(9_998) + '"'; // 9998 chars inside quotes, total token length is 10000
    const sql = `SELECT ${longIdent};`;
    const tokens = tokenize(sql);
    const identTokens = tokens.filter(t => t.type === 'identifier');
    expect(identTokens).toHaveLength(1);
  });

  it('error message mentions maximum length for quoted identifiers', () => {
    const longIdent = '"' + 'x'.repeat(10_001) + '"';
    try {
      tokenize(`SELECT ${longIdent};`);
      throw new Error('should have thrown');
    } catch (err) {
      expect(err).toBeInstanceOf(TokenizeError);
      expect((err as TokenizeError).message).toContain('maximum length');
    }
  });
});

describe('tokenizer unterminated dollar-quoted string details', () => {
  it('handles unterminated $$ string gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $$unterminated');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('handles unterminated $tag$ string gracefully', () => {
    // Now emits bare $ as operator tokens instead of throwing
    const tokens = tokenize('SELECT $tag$unterminated');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('tokenizes unterminated $$ without throwing', () => {
    // Verifies graceful handling - $ emitted as operator
    const tokens = tokenize('SELECT $$no close');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });

  it('tokenizes unterminated $tag$ without throwing', () => {
    // Verifies graceful handling - $ emitted as operator
    const tokens = tokenize('SELECT $abc$no close');
    const operators = tokens.filter(t => t.type === 'operator' && t.value === '$');
    expect(operators.length).toBeGreaterThan(0);
  });
});

describe('scientific notation backtracking with sign characters', () => {
  it('backtracks 1e+ correctly: SELECT 1e+ FROM t', () => {
    const tokens = tokenize('SELECT 1e+ FROM t').filter(t => t.type !== 'whitespace');
    expect(tokens[1].value).toBe('1');
    expect(tokens[1].type).toBe('number');
    expect(tokens[2].value).toBe('e');
    expect(tokens[2].type).toBe('identifier');
    expect(tokens[3].value).toBe('+');
    expect(tokens[3].type).toBe('operator');
    expect(tokens[4].upper).toBe('FROM');
    expect(tokens[5].value).toBe('t');
  });

  it('backtracks 1e- correctly: SELECT 1e- FROM t', () => {
    const tokens = tokenize('SELECT 1e- FROM t').filter(t => t.type !== 'whitespace');
    expect(tokens[1].value).toBe('1');
    expect(tokens[1].type).toBe('number');
    expect(tokens[2].value).toBe('e');
    expect(tokens[2].type).toBe('identifier');
    expect(tokens[3].value).toBe('-');
    expect(tokens[3].type).toBe('operator');
    expect(tokens[4].upper).toBe('FROM');
  });

  it('backtracks 1e (no sign, no digit) correctly: SELECT 1e FROM t', () => {
    const tokens = tokenize('SELECT 1e FROM t').filter(t => t.type !== 'whitespace');
    expect(tokens[1].value).toBe('1');
    expect(tokens[1].type).toBe('number');
    expect(tokens[2].value).toBe('e');
    expect(tokens[3].upper).toBe('FROM');
    expect(tokens[4].value).toBe('t');
  });
});

describe('identifier length check runs after loop', () => {
  it('still rejects identifiers exceeding MAX_IDENTIFIER_LENGTH', () => {
    const longIdent = 'a'.repeat(10_001);
    expect(() => tokenize(`SELECT ${longIdent};`)).toThrow(TokenizeError);
  });

  it('allows identifiers at exactly MAX_IDENTIFIER_LENGTH', () => {
    const ident = 'a'.repeat(10_000);
    const tokens = tokenize(`SELECT ${ident};`);
    const ids = tokens.filter(t => t.type === 'identifier');
    expect(ids).toHaveLength(1);
    expect(ids[0].value).toBe(ident);
  });
});
