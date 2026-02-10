import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('SET assignment subquery identifier casing', () => {
  it('keeps projection identifiers stable inside SET variable subqueries', () => {
    const sql = `SET @v_type = (SELECT type FROM t);
SET @v_value = (SELECT value FROM t);`;

    const out = formatSQL(sql);

    expect(out).toContain('SET @v_type = (SELECT type FROM t);');
    expect(out).toContain('SET @v_value = (SELECT value FROM t);');
    expect(out).not.toContain('SELECT TYPE FROM t');
    expect(out).not.toContain('SELECT VALUE FROM t');
  });
});
