import { describe, expect, it } from 'bun:test';
import { tokenize, type TokenType } from '../src/tokenizer';

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

  it('tokenizes E-strings', () => {
    const str = tokenize("SELECT E'\\n\\t', e'\\\\x';").filter(t => t.type === 'string').map(t => t.value);
    expect(str).toEqual(["E'\\n\\t'", "e'\\\\x'"]);
  });

  it('tokenizes B and X prefixed strings', () => {
    const str = tokenize("SELECT B'1010', b'0101', X'FF', x'aa';").filter(t => t.type === 'string').map(t => t.value);
    expect(str).toEqual(["B'1010'", "b'0101'", "X'FF'", "x'aa'"]);
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

  it('throws on unterminated dollar-quoted string', () => {
    expect(() => tokenize('SELECT $tag$no end')).toThrow();
  });
});
