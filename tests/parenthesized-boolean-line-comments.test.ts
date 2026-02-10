import { describe, expect, it } from 'bun:test';
import { formatSQL } from '../src/format';

describe('Parenthesized boolean expressions with trailing line comments', () => {
  it('keeps a single OR operator when a line comment precedes the next predicate', () => {
    const sql = `SELECT *
  FROM transfers
 WHERE block_time > now() - interval '90' day
   AND (to = 0xabc123  -- some comment
     OR "from" = 0xabc123)  -- some comment`;

    const out = formatSQL(sql);

    expect(out).toContain('AND (to = 0xabc123 -- some comment');
    expect(out).toContain('OR "from" = 0xabc123)  -- some comment');
    expect(out).not.toMatch(/\n\s*OR\s*--[^\n]*\nOR\b/);
  });
});
