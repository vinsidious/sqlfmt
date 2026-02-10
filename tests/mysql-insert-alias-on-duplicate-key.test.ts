import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

function expectStrictAndRecoveryFree(sql: string): string {
  expect(() => parse(sql, { recover: false })).not.toThrow();
  const recoveries: string[] = [];
  const out = formatSQL(sql, {
    onRecover: err => recoveries.push(err.message),
  });
  expect(recoveries).toEqual([]);
  return out;
}

describe('MySQL INSERT alias forms with ON DUPLICATE KEY UPDATE', () => {
  it('supports VALUES aliases used in update expressions', () => {
    const sql = `INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6) AS t2
ON DUPLICATE KEY UPDATE c = t2.a+t2.b;`;
    const out = expectStrictAndRecoveryFree(sql);

    expect(out).toContain('AS t2');
    expect(out).toContain('ON DUPLICATE KEY UPDATE');
    expect(out).toContain('c = t2.a + t2.b');
  });

  it('supports VALUES aliases with alias column names', () => {
    const sql = `INSERT INTO t1 (a,b,c) VALUES (1,2,3),(4,5,6) AS t2(m,n,p)
ON DUPLICATE KEY UPDATE c = m+n;`;
    const out = expectStrictAndRecoveryFree(sql);

    expect(out).toMatch(/AS t2\s*\(m, n, p\)/);
    expect(out).toContain('ON DUPLICATE KEY UPDATE');
    expect(out).toContain('c = m + n');
  });

  it('supports SET source rows with aliases', () => {
    const sql = `INSERT INTO t1 SET a=1,b=2,c=3 AS t2
ON DUPLICATE KEY UPDATE c = t2.a+t2.b;`;
    const out = expectStrictAndRecoveryFree(sql);

    expect(out).toContain('SET a = 1');
    expect(out).toContain('AS t2');
    expect(out).toContain('ON DUPLICATE KEY UPDATE');
    expect(out).toContain('c = t2.a + t2.b');
  });

  it('supports TABLE sources with aliases and alias column names', () => {
    const sql = `INSERT INTO t1 (a,b,c)
TABLE t2 as t3(m,n,p)
ON DUPLICATE KEY UPDATE b = n+p;`;
    const out = expectStrictAndRecoveryFree(sql);

    expect(out).toContain('TABLE t2 AS t3');
    expect(out).toMatch(/AS t3\s*\(m, n, p\)/);
    expect(out).toContain('ON DUPLICATE KEY UPDATE');
    expect(out).toContain('b = n + p');
  });
});
