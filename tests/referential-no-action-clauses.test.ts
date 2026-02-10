import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Referential NO ACTION clause formatting', () => {
  it('keeps ON DELETE NO ACTION and ON UPDATE NO ACTION as intact clauses', () => {
    const sql = 'ALTER TABLE cards ADD CONSTRAINT fk FOREIGN KEY (deck_id) REFERENCES decks(id) ON DELETE no action ON UPDATE no action;';

    const out = formatSQL(sql);

    expect(out).toContain('ON DELETE NO ACTION');
    expect(out).toContain('ON UPDATE NO ACTION');
    expect(out).not.toMatch(/ON DELETE\s+NO\s*\n\s*ACTION/i);
    expect(out).not.toMatch(/ON UPDATE\s+NO\s*\n\s*ACTION/i);
  });
});
