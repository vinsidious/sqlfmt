import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Snowflake VARIANT path expressions with casts', () => {
  it('parses VALUE:path access with :: casts in SELECT lists', () => {
    const sql = `create table if not exists "p08_base" as
select
    VALUE:id::TEXT id
from "_p08";`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('value:id::TEXT');
    expect(out).not.toContain('value : id::TEXT');
    expect(out).toContain('FROM "_p08";');
  });
});
