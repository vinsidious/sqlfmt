import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('T-SQL IF BEGIN END idempotent formatting', () => {
  it('keeps BEGIN block indentation stable across repeated formatting', () => {
    const sql = `IF OBJECT_ID('tempdb..##GlobalTempTableCommands') IS NULL
    BEGIN
        CREATE TABLE ##GlobalTempTableCommands (
            Command NVARCHAR(4000)
        );
    END;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
    expect(once).toContain("IF OBJECT_ID('tempdb..##GlobalTempTableCommands') IS NULL\n    BEGIN");
  });
});
