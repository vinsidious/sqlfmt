import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL procedure body select stability', () => {
  it('keeps SELECT indentation stable inside stored procedure bodies', () => {
    const sql = `CREATE PROCEDURE p
AS
BEGIN
SELECT TOP 1 [LastRunDateTime]
FROM [Inspector].[Modules]
WHERE [Modulename] = @Modulename
END;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
