import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('MySQL ALTER TABLE MODIFY keyword casing', () => {
  it('uppercases MODIFY in ALTER TABLE actions', () => {
    const sql = 'alter table people modify username varchar(255) unique;';
    const out = formatSQL(sql, { recover: false });

    expect(out).toContain('ALTER TABLE people');
    expect(out).toContain('MODIFY username VARCHAR(255) UNIQUE;');
  });
});

