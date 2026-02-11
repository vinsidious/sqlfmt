import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('MySQL INSERT empty VALUES tuple', () => {
  it('parses INSERT with empty column list and empty VALUES tuple in strict mode', () => {
    const sql = 'INSERT INTO status () VALUES ();';
    expect(() => parse(sql, { recover: false })).not.toThrow();
  });

  it('formats INSERT with empty tuples without parser recovery', () => {
    const sql = 'INSERT INTO status () VALUES ();';
    const recoveries: string[] = [];

    const once = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });
    const twice = formatSQL(once, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(once).toContain('INSERT INTO status');
    expect(once).toContain('VALUES ();');
    expect(twice).toBe(once);
  });
});
