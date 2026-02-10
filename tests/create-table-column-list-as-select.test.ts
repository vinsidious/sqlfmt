import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('CREATE TABLE column name lists with AS SELECT', () => {
  it('formats column lists without inserting spaces before commas', () => {
    const sql = 'create table testtable_summary (name, summary_amount) as select name, amount1 + amount2 from testtable;';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('CREATE TABLE testtable_summary (');
    expect(out).toContain('name,');
    expect(out).toContain('summary_amount');
    expect(out).toContain('SELECT name, amount1 + amount2');
    expect(out).not.toContain('name , summary_amount');
  });
});
