import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

function formatWithoutRecoveries(sql: string): string {
  const recoveries: string[] = [];
  const out = formatSQL(sql, {
    onRecover: err => recoveries.push(err.message),
  });
  expect(recoveries).toEqual([]);
  return out;
}

describe('MySQL index hint clauses', () => {
  it('parses and formats IGNORE INDEX table hints', () => {
    const sql = `SELECT a.id, b.category AS catid
  FROM t1 a, t2 b IGNORE INDEX (primary)
 WHERE a.count = b.id;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/IGNORE INDEX\s*\(primary\)/i);
  });

  it('parses and formats USE INDEX table hints', () => {
    const sql = 'SELECT * FROM t USE INDEX (idx_a);';

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/USE INDEX\s*\(idx_a\)/i);
  });

  it('parses and formats FORCE INDEX table hints', () => {
    const sql = 'SELECT * FROM t FORCE INDEX (idx_a);';

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/FORCE INDEX\s*\(idx_a\)/i);
  });
});
