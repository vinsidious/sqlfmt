import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('T-SQL UPDATE table hints', () => {
  it('parses UPDATE ... WITH (<hints>) SET ...', () => {
    const sql = `UPDATE dbo.Products WITH (UPDLOCK, HOLDLOCK) SET UnitPrice = UnitPrice * 1.05 WHERE Category = N'Electronics' AND IsDiscontinued = 0;`;
    expect(() => parse(sql, { dialect: 'tsql', recover: false })).not.toThrow();
  });

  it('formats UPDATE ... WITH (<hints>) with expected clause layout', () => {
    const sql = `UPDATE dbo.Products WITH (UPDLOCK, HOLDLOCK) SET UnitPrice = UnitPrice * 1.05 WHERE Category = N'Electronics' AND IsDiscontinued = 0;`;
    const out = formatSQL(sql, { dialect: 'tsql' });
    expect(out).toBe(
      [
        'UPDATE dbo.Products WITH (UPDLOCK, HOLDLOCK)',
        '   SET UnitPrice = UnitPrice * 1.05',
        ' WHERE Category = N\'Electronics\'',
        '   AND IsDiscontinued = 0;',
        '',
      ].join('\n'),
    );
  });
});

