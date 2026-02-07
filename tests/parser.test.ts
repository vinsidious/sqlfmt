import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { Parser } from '../src/parser';
import { tokenize } from '../src/tokenizer';
import { formatStatements } from '../src/formatter';

function parseFirst(sql: string) {
  const parser = new Parser(tokenize(sql));
  const nodes = parser.parseStatements();
  expect(nodes.length).toBeGreaterThan(0);
  return nodes[0] as any;
}

describe('parser syntax coverage', () => {
  it('parses ORDER BY NULLS LAST/FIRST', () => {
    const stmt = parseFirst('SELECT * FROM t ORDER BY a DESC NULLS LAST, b NULLS FIRST;');
    expect(stmt.type).toBe('select');
    expect(stmt.orderBy.items[0].nulls).toBe('LAST');
    expect(stmt.orderBy.items[1].nulls).toBe('FIRST');
  });

  it('parses FOR UPDATE/SHARE locking clauses', () => {
    const forUpdate = parseFirst('SELECT * FROM t FOR UPDATE;');
    expect(forUpdate.lockingClause).toBe('UPDATE');

    const forShare = parseFirst('SELECT * FROM t FOR SHARE NOWAIT;');
    expect(forShare.lockingClause).toBe('SHARE NOWAIT');
  });

  it('parses DELETE ... USING', () => {
    const stmt = parseFirst('DELETE FROM t USING u, v WHERE t.id = u.id;');
    expect(stmt.type).toBe('delete');
    expect(stmt.using).toHaveLength(2);
  });

  it('parses INSERT ... DEFAULT VALUES', () => {
    const stmt = parseFirst('INSERT INTO t DEFAULT VALUES;');
    expect(stmt.type).toBe('insert');
    expect(stmt.defaultValues).toBe(true);
  });

  it('parses UNION DISTINCT and INTERSECT ALL operators', () => {
    const stmt = parseFirst('SELECT 1 UNION DISTINCT SELECT 2 INTERSECT ALL SELECT 3;');
    expect(stmt.type).toBe('union');
    expect(stmt.operators).toEqual(['UNION DISTINCT', 'INTERSECT ALL']);
  });

  it('parses INSERT ... SELECT with UNION as query expression', () => {
    const stmt = parseFirst('INSERT INTO t SELECT id FROM a UNION SELECT id FROM b;');
    expect(stmt.type).toBe('insert');
    expect(stmt.selectQuery?.type).toBe('union');
  });

  it('parses CAST with multi-word type names', () => {
    const out = formatSQL('SELECT CAST(x AS DOUBLE PRECISION), CAST(x AS TIMESTAMP WITH TIME ZONE);');
    expect(out).toContain('CAST(x AS DOUBLE PRECISION)');
    expect(out).toContain('CAST(x AS TIMESTAMP WITH TIME ZONE)');
  });

  it('parses TRUNCATE without TABLE keyword', () => {
    const out = formatSQL('TRUNCATE foo;');
    expect(out.trim()).toBe('TRUNCATE foo;');
  });

  it('parses DROP for non-TABLE object kinds', () => {
    const out = formatSQL('DROP INDEX IF EXISTS idx_users_email;');
    expect(out.trim()).toBe('DROP INDEX IF EXISTS idx_users_email;');
  });

  it('parses ALTER for non-TABLE object kinds', () => {
    const out = formatSQL('ALTER INDEX idx_users_email RENAME TO idx_users_email_new;');
    expect(out.trim()).toBe('ALTER INDEX idx_users_email\n        RENAME TO idx_users_email_new;');
  });

  it('parses LIKE/ILIKE ESCAPE clauses', () => {
    const out = formatSQL("SELECT * FROM t WHERE a LIKE '%!_%' ESCAPE '!' OR b ILIKE '%!_%' ESCAPE '!';");
    expect(out).toContain("LIKE '%!_%' ESCAPE '!'");
    expect(out).toContain("ILIKE '%!_%' ESCAPE '!'");
  });

  it('formats FROM/JOIN aliases in lowercase', () => {
    const out = formatSQL('SELECT T.ID FROM USERS AS T JOIN ORDERS AS O ON T.ID = O.USER_ID;');
    expect(out).toContain('FROM users AS t');
    expect(out).toContain('JOIN orders AS o');
  });

  it('lowercases star qualifiers', () => {
    const out = formatSQL('SELECT T.* FROM TABLE_NAME AS T;');
    expect(out).toContain('SELECT t.*');
  });

  it('does not uppercase user-defined function names', () => {
    const out = formatSQL('SELECT myCustomFunc(a), SUM(a) FROM t;');
    expect(out).toContain('mycustomfunc(a)');
    expect(out).toContain('SUM(a)');
  });
});

describe('parser/code-quality safety checks', () => {
  it('advance throws when consumed past EOF', () => {
    const parser: any = new Parser(tokenize('SELECT 1;'));
    while (!parser.isAtEnd()) parser.advance();
    expect(() => parser.advance()).toThrow();
  });

  it('expectKeyword validates token type is keyword', () => {
    const parser: any = new Parser([
      { type: 'identifier', value: 'SELECT', upper: 'SELECT', position: 0 },
      { type: 'eof', value: '', upper: '', position: 6 },
    ]);

    expect(() => parser.expectKeyword('SELECT')).toThrow();
  });

  it('formatter throws on unknown node type', () => {
    expect(() => formatStatements([{ type: 'mystery' } as any])).toThrow();
  });

  it('enforces maximum nesting depth', () => {
    const deep = 'SELECT ' + '('.repeat(130) + '1' + ')'.repeat(130) + ';';
    expect(() => formatSQL(deep)).toThrow();
  });
});
