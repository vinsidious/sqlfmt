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

describe('PostgreSQL backslash quote literals', () => {
  it('parses and formats single-quoted strings that end with a backslash', () => {
    const sql = `SELECT CASE
    WHEN Value LIKE '\\\\%' THEN Value
    ELSE '\\\\ServerName\\' || Replace(Value, ':\\', '$\\')
END;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toContain("'\\\\ServerName\\'");
    expect(out).toContain("':\\'");
    expect(out).toContain("'$\\'");
  });
});
