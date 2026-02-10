import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

describe('MySQL multi-target UPDATE', () => {
  it('parses comma-separated table targets before SET', () => {
    const sql = `UPDATE book, supply
   SET book.amount = book.amount + supply.amount
 WHERE book.title = supply.title;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('UPDATE book, supply');
    expect(out).toContain('SET book.amount = book.amount + supply.amount');
    expect(out).toContain('WHERE book.title = supply.title;');
  });

  it('parses comma-separated table targets with aliases', () => {
    const sql = `UPDATE fine f, payment p
   SET f.date_payment = p.date_payment
 WHERE f.name = p.name;`;

    expect(() => parse(sql, { recover: false })).not.toThrow();

    const out = formatSQL(sql, { recover: false });
    expect(out).toContain('UPDATE fine AS f, payment AS p');
    expect(out).toContain('SET f.date_payment = p.date_payment');
  });
});

