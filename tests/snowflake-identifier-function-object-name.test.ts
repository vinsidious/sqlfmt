import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Snowflake IDENTIFIER function object names', () => {
  it('parses IDENTIFIER() table names in CREATE TABLE', () => {
    const sql = 'CREATE TABLE IDENTIFIER(:SOME_TABLE) (AMOUNT NUMBER);';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('CREATE TABLE IDENTIFIER(:SOME_TABLE) (');
    expect(out).toContain('amount NUMBER');
  });
});
