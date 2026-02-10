import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('INSERT SELECT source handling', () => {
  it('treats a parenthesized SELECT source as a query source, not a column list', () => {
    const sql = 'insert into OrderMgmtRC2 (select * from OrderMgmtRC);';

    expect(formatSQL(sql)).toBe(`INSERT INTO OrderMgmtRC2
SELECT *
  FROM OrderMgmtRC;
`);
  });
});
