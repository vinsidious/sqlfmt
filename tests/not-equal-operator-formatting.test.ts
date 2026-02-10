import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('Not-equal operator formatting', () => {
  it('parses and formats != comparisons', () => {
    const sql = "SELECT a FROM t1 WHERE City != 'DHAKA';";

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain("City != 'DHAKA'");
  });
});
