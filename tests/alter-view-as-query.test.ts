import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('ALTER VIEW AS Query', () => {
  it('keeps GO as standalone batch separators around ALTER VIEW AS SELECT', () => {
    const sql = `ALTER VIEW EmployeeList
AS
    SELECT E.BusinessEntityID, P.LastName, P.FirstName, E.Gender, E.HireDate
    FROM Person AS P
    INNER JOIN Employees AS E
    ON P.BusinessEntityID = E.BusinessEntityID
GO

SELECT * FROM EmployeeList
GO`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('\nGO\n');
    expect(out).toContain('SELECT *');
    expect(out).not.toContain(' GO SELECT * FROM EmployeeList GO');
  });

  it('keeps WITH ENCRYPTION connected to AS SELECT in ALTER VIEW', () => {
    const sql = `ALTER VIEW EmployeeList
WITH ENCRYPTION
AS
    SELECT E.BusinessEntityID, P.LastName, P.FirstName
    FROM Person AS P
    INNER JOIN Employees AS E
    ON P.BusinessEntityID = E.BusinessEntityID`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toMatch(/WITH\s+ENCRYPTION\s+AS\s*\n\s*SELECT/i);
    expect(out).not.toContain('WITH ENCRYPTION AS;');
  });
});
