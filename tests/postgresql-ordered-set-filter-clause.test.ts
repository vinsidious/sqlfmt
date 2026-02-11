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

describe('PostgreSQL ordered-set aggregate filter clause', () => {
  it('parses and formats ordered-set aggregates with FILTER (WHERE ...)', () => {
    const sql = `SELECT ten,
       percentile_disc(0.5) WITHIN GROUP (ORDER BY thousand)
       FILTER (WHERE hundred=1) AS px
  FROM tenk1
 GROUP BY ten;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/WITHIN GROUP \(ORDER BY thousand\) FILTER \(WHERE hundred = 1\)/i);
  });
});
