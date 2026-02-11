import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('PostgreSQL CTE column list comments', () => {
  it('keeps recursive CTE column lists syntactically valid when inline comments are present', () => {
    const sql = `WITH RECURSIVE org_h (
    ancestor_oid, -- first column comment
    descendant_oid -- second column comment
) AS (
    SELECT 1, 2
)
SELECT * FROM org_h;`;

    const recoveries: string[] = [];
    const out = formatSQL(sql, {
      onRecover: err => recoveries.push(err.message),
    });

    expect(recoveries).toEqual([]);
    expect(out).toContain('WITH RECURSIVE org_h (ancestor_oid, descendant_oid) AS (');
    expect(out).not.toContain('-- first column comment, descendant_oid');
    expect(() => parse(out, { recover: false })).not.toThrow();
  });
});
