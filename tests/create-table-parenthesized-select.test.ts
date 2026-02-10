import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('CREATE TABLE with parenthesized SELECT source', () => {
  it('formats parenthesized SELECT source as table-creation query source', () => {
    const sql = `CREATE TABLE back_payment
    (SELECT name, number_plate, violation, sum_fine, date_violation
     FROM fine
     WHERE date_payment IS NULL);`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('CREATE TABLE back_payment AS');
    expect(out).toContain('\nSELECT name, number_plate, violation, sum_fine, date_violation');
    expect(out).toContain('\n  FROM fine');
    expect(out).toContain('\n WHERE date_payment IS NULL;');
  });
});

