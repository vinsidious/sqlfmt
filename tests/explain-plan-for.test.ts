import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('EXPLAIN PLAN FOR statements', () => {
  it('parses EXPLAIN PLAN FOR in strict mode', () => {
    const sql = 'EXPLAIN PLAN FOR SELECT id FROM t;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('formats EXPLAIN PLAN FOR with nested query formatting', () => {
    const sql = `explain plan for select * from (
    select c_date, c_time, c_integer,
           cast(count(*) over (partition by c_date ORDER BY c_time) as varchar(20))
      from j7_v
     where c_integer > 0
) t limit 0;`;

    const out = formatSQL(sql);
    expect(out).toContain('EXPLAIN PLAN FOR');
    expect(out).toContain('SELECT c_date,');
    expect(out).toContain('FROM j7_v');
  });
});
