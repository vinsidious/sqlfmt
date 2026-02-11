import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL CREATE VIEW WITH SCHEMABINDING', () => {
  it('parses and formats CREATE VIEW statements with SCHEMABINDING attributes', () => {
    const sql = 'CREATE VIEW test WITH SCHEMABINDING AS SELECT id FROM t;';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('CREATE VIEW test WITH SCHEMABINDING AS');
    expect(out).toContain('SELECT id');
  });
});
