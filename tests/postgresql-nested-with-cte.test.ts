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

describe('PostgreSQL nested WITH inside CTE bodies', () => {
  it('parses and formats nested WITH clauses inside a CTE definition', () => {
    const sql = `WITH RECURSIVE w1(c1) AS (
  WITH w2(c2) AS (
    SELECT 1
  )
  SELECT * FROM w2
)
SELECT * FROM w1;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/WITH RECURSIVE w1\s*\(c1\)\s*AS/i);
    expect(out).toMatch(/WITH w2\s*\(c2\)\s*AS/i);
    expect(out.toUpperCase()).toContain('SELECT *');
  });
});
