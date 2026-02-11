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

describe('PostgreSQL UPDATE array subscript targets', () => {
  it('parses and formats single-element array assignments in SET', () => {
    const sql = "UPDATE arrtest SET e[0] = '1.1';";

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toContain("SET e[0] = '1.1'");
  });

  it('parses and formats array-slice assignments in SET', () => {
    const sql = "UPDATE arrtest SET a[1:2] = '{16,25}';";

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toContain("SET a[1:2] = '{16,25}'");
  });
});
