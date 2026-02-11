import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('PostgreSQL alias identifier stability', () => {
  it('keeps identifier alias spelling when alias text matches function keywords', () => {
    const sql = `SELECT f1
FROM SUBSELECT_TBL upper
WHERE f1 IN (SELECT f2 FROM SUBSELECT_TBL WHERE f1 = upper.f1);

SELECT f1
FROM SUBSELECT_TBL upper
WHERE f1 = upper.f1;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(once).toContain('AS upper');
    expect(once).toContain('upper.f1');
    expect(once).not.toContain('AS UPPER');
    expect(once).not.toContain('UPPER.f1');
    expect(twice).toBe(once);
  });
});
