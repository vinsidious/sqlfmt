import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('SELECT explicit alias preservation', () => {
  it('keeps explicit AS aliases even when alias and source name match', () => {
    const sql = 'SELECT b.meno AS meno, b.priezvisko AS priezvisko, k.nazov AS stat;';
    const out = formatSQL(sql, { recover: false });

    expect(out).toContain('b.meno AS meno');
    expect(out).toContain('b.priezvisko AS priezvisko');
    expect(out).toContain('k.nazov AS stat');
  });
});

