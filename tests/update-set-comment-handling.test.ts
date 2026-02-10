import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('UPDATE SET comment handling', () => {
  it('parses comments after comma-separated assignments in strict mode', () => {
    const sql = `UPDATE gl_interface
   SET code_combination_id = '1037538', -- comment
       attribute2 = 'Y'
 WHERE status = 'EF05';`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain("SET code_combination_id = '1037538'");
    expect(out).toContain("attribute2 = 'Y'");
    expect(out).toContain("WHERE status = 'EF05';");
  });

  it('parses comments before WHERE in strict mode', () => {
    const sql = `UPDATE gl_interface
   SET code_combination_id = '1037538' -- comment
 WHERE status = 'EF05';`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain("SET code_combination_id = '1037538'");
    expect(out).toContain("WHERE status = 'EF05';");
  });
});
