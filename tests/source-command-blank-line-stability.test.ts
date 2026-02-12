import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('source command blank-line stability', () => {
  it('keeps blank-line separation stable between consecutive source commands', () => {
    const sql = `source load_salaries3.dump ;

source show_elapsed.sql ;`;

    const once = formatSQL(sql, { recover: true });
    const twice = formatSQL(once, { recover: true });

    expect(once).toMatch(/source load_salaries3\.dump ;\n\nsource show_elapsed\.sql ;/);
    expect(twice).toBe(once);
  });
});
