import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('ALTER TABLE multi-action stability', () => {
  it('keeps comma-separated ALTER TABLE actions together across repeated formatting', () => {
    const sql = 'ALTER TABLE infectious_cases DROP COLUMN Entity, DROP COLUMN Code;';
    const once = formatSQL(sql, { recover: false });
    const twice = formatSQL(once, { recover: false });

    expect(once).toContain('DROP COLUMN Entity,');
    expect(once).toContain('DROP COLUMN Code;');
    expect(twice).toBe(once);
  });
});

