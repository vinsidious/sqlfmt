import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL IF statement layout', () => {
  it('keeps IF NOT EXISTS visually attached to INSERT', () => {
    const sql = `IF NOT EXISTS (SELECT 1 FROM ComponentType WHERE [Name]=N'Wheel')
INSERT INTO ComponentType([Name], MinSelect, MaxSelect, DisplayOrder) VALUES (N'Wheel',1,1,1);`;

    const out = formatSQL(sql);
    expect(out).not.toContain(')\n\nINSERT INTO');
    expect(out).toContain("IF NOT EXISTS (SELECT 1 FROM ComponentType WHERE [Name]=N'Wheel')");
    expect(out).toContain('INSERT INTO ComponentType ([Name], MinSelect, MaxSelect, DisplayOrder)');
  });
});
