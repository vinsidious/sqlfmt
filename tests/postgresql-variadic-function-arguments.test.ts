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

describe('PostgreSQL VARIADIC function arguments', () => {
  it('parses and formats VARIADIC array arguments in function calls', () => {
    const sql = 'SELECT cleast_agg(VARIADIC array[4.5, f1]) FROM int4_tbl;';

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/VARIADIC ARRAY\[4\.5, f1\]/i);
  });
});
