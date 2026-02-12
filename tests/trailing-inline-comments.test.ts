import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Trailing inline statement comments', () => {
  it('keeps statement-ending inline comments on the same line', () => {
    const sql = `INCREMENT BY 1;  -- Step increment
CREATE TABLE my_table (id INT);`;

    const out = formatSQL(sql, { recover: true });
    expect(out).toMatch(/INCREMENT BY 1;\s+-- Step increment/);
    expect(out).not.toMatch(/INCREMENT BY 1;\s*\n\s*\n?\s*-- Step increment\s*\n\s*CREATE TABLE/);
  });
});
