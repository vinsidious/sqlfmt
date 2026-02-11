import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('INSERT INTO keyword validation', () => {
  it('throws in strict mode when INTO is misspelled as INT', () => {
    const sql = "insert int PC_EMP values (5, '민지성', 'slave');";
    expect(() => parse(sql, { recover: false })).toThrow();
  });

  it('preserves statement text in recovery mode when INTO is misspelled as INT', () => {
    const sql = "insert int PC_EMP values (5, '민지성', 'slave');";
    expect(formatSQL(sql).trim()).toBe(sql);
  });

  it('continues to support INSERT statements without the optional INTO keyword', () => {
    const sql = 'insert audit_log values (1);';
    const out = formatSQL(sql);
    expect(out).toContain('INSERT INTO audit_log');
    expect(out).toContain('VALUES (1)');
  });
});
