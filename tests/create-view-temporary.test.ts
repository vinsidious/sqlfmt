import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('CREATE TEMPORARY VIEW formatting', () => {
  it('parses CREATE TEMPORARY VIEW in strict mode and formats query clauses', () => {
    const sql = 'create temporary view nt1 as select * from some_table where id = 1;';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false }).trimEnd();
    expect(out).toBe(`CREATE TEMPORARY VIEW nt1 AS
SELECT *
  FROM some_table
 WHERE id = 1;`);
  });

  it('parses CREATE OR REPLACE TEMP VIEW in strict mode', () => {
    const sql = 'create or replace temp view nt2 as select id from some_table;';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('CREATE OR REPLACE TEMPORARY VIEW nt2 AS');
  });
});
