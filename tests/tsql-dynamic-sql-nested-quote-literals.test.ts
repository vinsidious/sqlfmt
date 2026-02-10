import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL Dynamic SQL Nested Quote Literals', () => {
  it('keeps nested escaped quote literals inside N-prefixed dynamic SQL fragments', () => {
    const sql = `SET @s += N'IF @debug = 1 BEGIN RAISERROR(''Created table %s for significant waits logging.'', 0, 1, ''' + @log_table_significant_waits + N''') WITH NOWAIT; END;';`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain("''Created table %s for significant waits logging.''");
    expect(out).toContain('@log_table_significant_waits');
  });
});
