import { describe, expect, it } from 'bun:test';
import { formatSQL, parse } from '../src/index';

describe('T-SQL template placeholder identifiers', () => {
  it('keeps angle-bracket placeholders compact in multipart table names', () => {
    const sql = 'DROP TABLE <schema_name, sysname, dbo>.<table_name, sysname, sample_table>;';

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql);
    expect(out.trim()).toBe('DROP TABLE <schema_name, sysname, dbo>.<table_name, sysname, sample_table>;');
  });
});
