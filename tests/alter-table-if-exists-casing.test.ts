import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('ALTER TABLE IF EXISTS keyword casing', () => {
  it('uppercases IF EXISTS after ALTER TABLE', () => {
    const sql = 'alter table if exists t drop column x;';
    const out = formatSQL(sql, { recover: false });

    expect(out).toContain('ALTER TABLE IF EXISTS t');
    expect(out).toContain('DROP COLUMN x;');
  });
});

