import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('PostgreSQL recursive CTE UNION comment idempotency', () => {
  it('keeps a single comment before the first UNION branch across repeated formatting', () => {
    const sql = `WITH RECURSIVE rolling_sum AS (
-- non-recursive term
           SELECT MAX(r.name) AS name, SUM(r.val) AS nr
             FROM roll_sum AS r
            UNION ALL
           SELECT rs.name - 1 AS name, s.nr - rs.val AS nr
             FROM roll_sum AS rs
             JOIN rolling_sum AS s
               ON rs.name = s.name
       )
SELECT 'val_' || s.name AS name, s.nr
  FROM rolling_sum AS s;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);
    const thrice = formatSQL(twice);

    expect(twice).toBe(once);
    expect(thrice).toBe(twice);
    expect((once.match(/-- non-recursive term/g) ?? []).length).toBe(1);
  });
});
