import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('MySQL INTERVAL expression values', () => {
  it('parses function expressions after INTERVAL', () => {
    const sql = 'SELECT DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 30) DAY) FROM t1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('parses identifier values after INTERVAL', () => {
    const sql = 'SELECT DATE_ADD(NOW(), INTERVAL n DAY) FROM t1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('parses parenthesized values after INTERVAL', () => {
    const sql = 'SELECT DATE_ADD(NOW(), INTERVAL (30) DAY) FROM t1;';
    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('INTERVAL (30) DAY');
  });
});
