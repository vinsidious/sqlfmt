import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL consecutive sp_executesql stability', () => {
  it('keeps consecutive dynamic SQL execution blocks stable across passes', () => {
    const sql = `EXECUTE sp_executesql @statement = N'CREATE FUNCTION dbo.ufnA()
RETURNS INT
AS
BEGIN
    RETURN 1
END';

EXECUTE sp_executesql @statement = N'CREATE FUNCTION dbo.ufnB()
RETURNS INT
AS
BEGIN
    RETURN 2
END';`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
