import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL DELETE boundaries before DBCC statements', () => {
  it('formats DELETE and keeps following DBCC as a separate statement without requiring a semicolon', () => {
    const sql = `delete from Categories
DBCC CHECKIDENT ('[Categories]', RESEED, 0);`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('DELETE\n  FROM Categories;');
    expect(out).toContain("\nDBCC CHECKIDENT ('[Categories]', RESEED, 0);");
  });
});
