import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('PL/SQL sequence comment line separation', () => {
  it('keeps standalone comment lines separate from trailing statement comments', () => {
    const sql = `CREATE SEQUENCE emp_seq
    START WITH 150
    INCREMENT BY 1; -- This sequence can be used to handle the id field for employees.
-- In order to actually utilize it, we will need to build something
-- that reacts to situations where an employee is inserted.
-- trigger
/`;

    const once = formatSQL(sql);
    const twice = formatSQL(once);

    expect(twice).toBe(once);
    expect(once).toContain(
      'INCREMENT BY 1; -- This sequence can be used to handle the id field for employees.\n-- In order to actually utilize it, we will need to build something'
    );
  });
});
