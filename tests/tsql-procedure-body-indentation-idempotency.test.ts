import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL procedure body indentation idempotency', () => {
  it('keeps DECLARE list indentation stable across repeated formatting', () => {
    const sql = `CREATE PROCEDURE dbo.sp_doc
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Sql NVARCHAR(MAX)
\t\t,@ParmDefinition NVARCHAR(500)
\t\t,@QuotedDatabaseName SYSNAME
\t\t,@Msg NVARCHAR(MAX);
END;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
