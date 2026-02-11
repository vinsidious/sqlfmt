import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('MySQL routine block comment indentation', () => {
  it('keeps block comment indentation stable inside procedure bodies', () => {
    const sql = `CREATE PROCEDURE p()
BEGIN
/*
    Create a temporary storage area for
    Customer IDs.
*/
SELECT 1;
END;`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
  });
});
