import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL BEGIN END dynamic SQL indentation idempotency', () => {
  it('keeps dynamic SQL literal indentation stable across repeated formatting', () => {
    const sql = `CREATE PROCEDURE dbo.print_context
AS
BEGIN
    DECLARE @sql_command NVARCHAR(MAX);

    SET @sql_command
               = 'SELECT SUSER_SNAME() AS security_context_in_dynamic_sql;

\tSELECT COUNT(*) AS table_count_in_dynamic_sql FROM Person.Person';

    EXEC sp_executesql @sql_command;
END;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
