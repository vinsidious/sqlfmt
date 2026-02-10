import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL Compound Assignment Operators', () => {
  const operators = ['+=', '-=', '*=', '/=', '%=', '&=', '^=', '|='];

  for (const operator of operators) {
    it(`parses UPDATE SET with ${operator}`, () => {
      const sql = `UPDATE t SET x ${operator} 1 WHERE id = 1;`;
      expect(() => parse(sql, { recover: false })).not.toThrow();

      const out = formatSQL(sql, { recover: false });
      expect(out).toContain(`x ${operator} 1`);
    });
  }

  it('parses MERGE UPDATE SET compound assignment', () => {
    const sql = `MERGE INTO t AS target USING (SELECT 1 AS id) AS src
ON target.id = src.id
WHEN MATCHED THEN UPDATE SET target.total += 1;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('target.total += 1');
  });
});
