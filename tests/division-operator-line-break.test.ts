import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Division operator line-break parsing', () => {
  it('parses and formats division operators on their own line inside expressions', () => {
    const sql = `SELECT round(
  (SELECT count(*) FROM (SELECT DISTINCT a FROM t1) AS A)
  /
  (SELECT count(*) FROM (SELECT DISTINCT b FROM t2) AS B), 2
) AS result;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql);
    expect(out).toMatch(/COUNT\(\*\)[\s\S]*\/\s*\(SELECT COUNT\(\*\)/i);
    expect(out.trimEnd()).toMatch(/\)\s+AS result;$/i);
  });
});
