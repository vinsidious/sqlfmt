import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Spark temporary view formatting', () => {
  it('parses CREATE OR REPLACE TEMPORARY VIEW with column list and VALUES', () => {
    const sql = `CREATE OR REPLACE TEMPORARY VIEW int2_tbl (f1) AS
VALUES (smallint(TRIM('0   '))),
       (smallint(TRIM('  1234 '))),
       (smallint(TRIM('    -1234'))),
       (smallint('32767')),
       (smallint('-32767'));`;

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatSQL(sql);
    expect(out).toContain('CREATE OR REPLACE TEMPORARY VIEW int2_tbl (f1) AS');
    expect(out).toContain("VALUES (smallint(TRIM('0   '))),");
  });
});
