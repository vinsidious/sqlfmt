import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse, Parser, ParseError, MaxDepthError } from '../src/parser';
import { tokenize } from '../src/tokenizer';

describe('no console.error in library code', () => {
  it('throws when recovery cannot preserve text and onDropStatement is not provided', () => {
    const parser = new Parser(tokenize('SELECT (;'), { recover: true }) as any;

    parser.parseRawStatement = function () {
      this.pos = this.tokens.length - 1;
      return null;
    };

    expect(() => parser.parseStatements()).toThrow(ParseError);
  });
});

describe('maxDepth guards subquery nesting', () => {
  it('throws MaxDepthError for subqueries exceeding maxDepth', () => {
    const sql = 'SELECT (SELECT (SELECT 1))';
    expect(() => parse(sql, { maxDepth: 2 })).toThrow(MaxDepthError);
  });

  it('throws MaxDepthError for subqueries exceeding maxDepth in strict mode', () => {
    const sql = 'SELECT (SELECT (SELECT 1))';
    expect(() => parse(sql, { maxDepth: 2, recover: false })).toThrow(MaxDepthError);
  });

  it('allows subqueries within maxDepth limit', () => {
    const sql = 'SELECT (SELECT (SELECT 1))';
    const result = parse(sql, { maxDepth: 10 });
    expect(result.length).toBe(1);
    expect(result[0].type).toBe('select');
  });

  it('throws MaxDepthError for deeply nested subqueries', () => {
    // Build SELECT (SELECT (SELECT ... (SELECT 1) ...))
    const depth = 15;
    let sql = 'SELECT 1';
    for (let i = 0; i < depth; i++) {
      sql = `SELECT (${sql})`;
    }
    expect(() => parse(sql, { maxDepth: 5 })).toThrow(MaxDepthError);
  });

  it('MaxDepthError propagates in recovery mode for subqueries', () => {
    const sql = 'SELECT (SELECT (SELECT (SELECT 1)))';
    expect(() => parse(sql, { maxDepth: 3, recover: true })).toThrow(MaxDepthError);
  });
});

describe('STRAIGHT_JOIN as MySQL SELECT modifier', () => {
  it('treats STRAIGHT_JOIN as a join keyword in MySQL dialect', () => {
    const result = formatSQL('SELECT col1 FROM t1 STRAIGHT_JOIN t2 ON t1.id = t2.id;', { dialect: 'mysql' });
    expect(result).toContain('STRAIGHT_JOIN');
    expect(result).toContain('col1');
  });

  it('treats straight_join as a column name in PostgreSQL dialect', () => {
    const result = formatSQL('SELECT straight_join FROM t;', { dialect: 'postgres' });
    // In non-MySQL dialects, STRAIGHT_JOIN is not a keyword, so it should
    // be treated as an identifier (column name), not a join keyword.
    expect(result).toContain('straight_join');
    expect(result).toContain('FROM');
  });

  it('does not treat STRAIGHT_JOIN as a join keyword in ANSI dialect', () => {
    const result = formatSQL('SELECT straight_join FROM t;', { dialect: 'ansi' });
    expect(result).toContain('straight_join');
    expect(result).toContain('FROM');
  });
});

describe('inline block comments between clause keywords', () => {
  it('handles block comment between GROUP and BY', () => {
    const result = formatSQL('SELECT a FROM t GROUP /* comment */ BY a;');
    expect(result).toContain('GROUP BY');
    expect(result).toContain('a');
  });

  it('handles block comment between ORDER and BY', () => {
    const result = formatSQL('SELECT a FROM t ORDER /* comment */ BY a;');
    expect(result).toContain('ORDER BY');
    expect(result).toContain('a');
  });

  it('handles block comment between PARTITION and BY in window spec', () => {
    const result = formatSQL('SELECT SUM(x) OVER (PARTITION /* c */ BY y) FROM t;');
    expect(result).toContain('PARTITION BY');
  });

  it('handles line comment between ORDER and BY', () => {
    const result = formatSQL('SELECT a FROM t ORDER\n-- comment\nBY a;');
    expect(result).toContain('ORDER BY');
    expect(result).toContain('a');
  });

  it('formats mixed inline comments correctly', () => {
    const sql = 'SELECT /* c1 */ a, /* c2 */ b FROM /* c3 */ t;';
    const result = formatSQL(sql);
    expect(result).toContain('SELECT');
    expect(result).toContain('FROM');
    // Should not be raw passthrough
    expect(result).not.toBe(sql);
  });
});

describe('ILIKE dialect-aware casing', () => {
  it('uppercases ILIKE in PostgreSQL dialect', () => {
    const result = formatSQL("SELECT * FROM t WHERE col ilike '%x%';", { dialect: 'postgres' });
    expect(result).toContain('ILIKE');
  });

  it('preserves original casing of ilike in MySQL dialect', () => {
    const result = formatSQL("SELECT * FROM t WHERE col ilike '%x%';", { dialect: 'mysql' });
    expect(result).toContain('ilike');
    expect(result).not.toContain('ILIKE');
  });

  it('preserves original casing of ILIKE in ANSI dialect', () => {
    const result = formatSQL("SELECT * FROM t WHERE col ILIKE '%x%';", { dialect: 'ansi' });
    expect(result).toContain('ILIKE');
  });

  it('preserves lowercase ilike in ANSI dialect', () => {
    const result = formatSQL("SELECT * FROM t WHERE col ilike '%x%';", { dialect: 'ansi' });
    expect(result).toContain('ilike');
  });

  it('uppercases ILIKE in default (postgres) dialect', () => {
    const result = formatSQL("SELECT * FROM t WHERE col ilike '%x%';");
    expect(result).toContain('ILIKE');
  });

  it('handles NOT ILIKE with dialect-aware casing', () => {
    const result = formatSQL("SELECT * FROM t WHERE col NOT ilike '%x%';", { dialect: 'mysql' });
    expect(result).toContain('ilike');
  });
});
