import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('KEY identifier in column definitions', () => {
  it('preserves key as a column name and keeps type argument spacing intact', () => {
    const sql = `CREATE TABLE cache_stats (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) NOT NULL
);`;

    const out = formatSQL(sql);

    expect(out).toContain('key VARCHAR(255) NOT NULL');
    expect(out).not.toContain('KEY VARCHAR (255) NOT NULL');
  });
});
