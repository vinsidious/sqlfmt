import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Function name casing consistency', () => {
  it('uppercases LTRIM, RTRIM, and MOD function names', () => {
    const sql = "select ltrim(rtrim(concat(first_name, ' ', last_name))), mod(10,3) from contacts;";

    const out = formatSQL(sql);

    expect(out).toContain("LTRIM(RTRIM(CONCAT(first_name, ' ', last_name)))");
    expect(out).toContain('MOD(10, 3)');
  });

  it('applies consistent pg_catalog function casing for set_config and setval', () => {
    const sql = `SELECT pg_catalog.set_config('search_path', '', false);
SELECT pg_catalog.setval('public.seq', 21, true);`;

    const out = formatSQL(sql);

    expect(out).toContain("pg_catalog.SET_CONFIG('search_path', '', FALSE)");
    expect(out).toContain("pg_catalog.SETVAL('public.seq', 21, TRUE)");
  });
});
