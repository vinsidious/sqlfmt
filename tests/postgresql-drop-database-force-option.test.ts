import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';
import { parse } from '../src/parser';

function formatWithoutRecoveries(sql: string): string {
  const recoveries: string[] = [];
  const out = formatSQL(sql, {
    onRecover: err => recoveries.push(err.message),
  });
  expect(recoveries).toEqual([]);
  return out;
}

describe('PostgreSQL DROP DATABASE force option', () => {
  it('parses and formats DROP DATABASE with WITH (FORCE)', () => {
    const sql = 'DROP DATABASE metodologias_agiles WITH (FORCE);';

    expect(() => parse(sql, { recover: false })).not.toThrow();
    const out = formatWithoutRecoveries(sql);

    expect(out).toMatch(/DROP DATABASE metodologias_agiles WITH\s*\(FORCE\);/i);
  });
});
