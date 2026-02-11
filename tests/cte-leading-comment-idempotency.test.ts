import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('CTE leading comment idempotency', () => {
  it('keeps a single leading comment inside a CTE body across repeated formatting', () => {
    const sql = `WITH CTE_EmplHierarchy (BusinessEntityID, ManagerID, [Level]) AS (
    -- Anchor member: start with employees who do not have a manager
    SELECT BusinessEntityID, ManagerID, 0 AS [Level]
    FROM HumanResources.Employee
    WHERE ManagerID IS NULL
)
SELECT * FROM CTE_EmplHierarchy;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const once = formatSQL(sql);
    const twice = formatSQL(once);
    const thrice = formatSQL(twice);

    expect(twice).toBe(once);
    expect(thrice).toBe(twice);
    expect((once.match(/\/\* Anchor member: start with employees who do not have a manager \*\//g) ?? []).length).toBe(1);
  });
});
