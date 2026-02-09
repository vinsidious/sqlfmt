import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('INSERT column-list comment handling', () => {
  it('keeps comment commas stable across formatting passes', () => {
    const sql = `INSERT INTO #table (
       --base,
       [project_id],
       --revenue,
       [revenue_value]
)
VALUES (1, 2);`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(once).toContain('--base,');
    expect(once).toContain('--revenue,');
    expect(once).not.toContain('--base,,');
    expect(once).not.toContain('--revenue,,');
    expect(twice).toBe(once);
  });
});
